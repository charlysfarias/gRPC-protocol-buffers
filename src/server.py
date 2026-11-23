from concurrent import futures
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import logging
import os
import re
import signal
import sys
import tempfile
import threading
import time
import uuid

import grpc

import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
from log_config import ProgressLogger, log_event, setup_logging

DEFAULT_HOST = os.environ.get("FILESERVER_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("FILESERVER_PORT", "50051"))
DEFAULT_WORKERS = int(os.environ.get("FILESERVER_WORKERS", "10"))
DEFAULT_MAX_FILE_MB = int(os.environ.get("FILESERVER_MAX_FILE_MB", "100"))
DEFAULT_STORAGE_DIR = Path(
    os.environ.get("FILESERVER_STORAGE", Path(__file__).resolve().parent.parent / "storage")
)
CHUNK_SIZE = 64 * 1024
GRACE_SECONDS = 5
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,199}$")
SHA256_HEX = re.compile(r"^[0-9a-fA-F]{64}$")
TEMP_PREFIX = ".upload-"
TEMP_SUFFIX = ".tmp"
FILE_MODE = 0o644

logger = logging.getLogger("server")


class TransferError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class FileEntry:
    size: int
    mtime: float
    sha256: str | None = None


def valid_filename(name):
    return bool(name and SAFE_NAME.fullmatch(name)
                and name not in {".", ".."}
                and "/" not in name and "\\" not in name)


def sha256_of(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while data := source.read(CHUNK_SIZE):
            digest.update(data)
    return digest.hexdigest()


def new_transfer_id():
    return uuid.uuid4().hex[:8]


def elapsed_ms(start):
    return (time.perf_counter() - start) * 1000


def throughput_mib_s(size, start):
    seconds = time.perf_counter() - start
    return (size / (1024 * 1024)) / seconds if seconds > 0 else 0.0


class FileService(pb2_grpc.FileServiceServicer):
    def __init__(self, storage_dir, max_file_size):
        self.storage_dir = storage_dir
        self.max_file_size = max_file_size
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.index: dict[str, FileEntry] = {}
        self.reserved: set[str] = set()
        self._remove_orphan_temp_files()
        self._rebuild_index()

    def _remove_orphan_temp_files(self):
        removed = 0
        for path in self.storage_dir.glob(f"{TEMP_PREFIX}*{TEMP_SUFFIX}"):
            path.unlink(missing_ok=True)
            removed += 1
        if removed:
            log_event(logger, "storage.orphans_removed", logging.WARNING, files=removed)

    def _rebuild_index(self):
        with self.lock:
            self.index.clear()
            for path in self.storage_dir.iterdir():
                if path.is_file() and valid_filename(path.name):
                    stat = path.stat()
                    self.index[path.name] = FileEntry(size=stat.st_size, mtime=stat.st_mtime)
        log_event(logger, "index.rebuilt", files=len(self.index))

    def _reserve(self, name):
        with self.lock:
            if name in self.index or name in self.reserved or (self.storage_dir / name).exists():
                raise TransferError(grpc.StatusCode.ALREADY_EXISTS, "Já existe um arquivo com esse nome.")
            self.reserved.add(name)

    def _release(self, name):
        with self.lock:
            self.reserved.discard(name)

    def _publish(self, temp_path, name, sha256):
        destination = self.storage_dir / name
        with self.lock:
            os.replace(temp_path, destination)
            stat = destination.stat()
            self.index[name] = FileEntry(size=stat.st_size, mtime=stat.st_mtime, sha256=sha256)

    def _lookup(self, name):
        with self.lock:
            entry = self.index.get(name)
            if entry is None:
                return None
            if entry.sha256 is not None:
                return FileEntry(entry.size, entry.mtime, entry.sha256)
        file_hash = sha256_of(self.storage_dir / name)
        with self.lock:
            entry = self.index.get(name)
            if entry is None:
                return None
            entry.sha256 = file_hash
            return FileEntry(entry.size, entry.mtime, entry.sha256)

    def ListFiles(self, request, context):
        start = time.perf_counter()
        peer = context.peer()
        log_event(logger, "list.start", peer=peer)
        with self.lock:
            snapshot = sorted(
                (name, FileEntry(e.size, e.mtime, e.sha256)) for name, e in self.index.items()
            )
        response = pb2.ListFilesResponse()
        total_bytes = 0
        for name, entry in snapshot:
            total_bytes += entry.size
            info = response.files.add(name=name, size_bytes=entry.size)
            info.modified_at.FromDatetime(datetime.fromtimestamp(entry.mtime, tz=timezone.utc))
        log_event(
            logger, "list.done", peer=peer, files=len(response.files),
            total_bytes=total_bytes, duration_ms=elapsed_ms(start),
        )
        return response

    def _validate_metadata(self, metadata):
        if not valid_filename(metadata.filename):
            raise TransferError(grpc.StatusCode.INVALID_ARGUMENT, "Nome de arquivo inválido.")
        if not SHA256_HEX.fullmatch(metadata.sha256):
            raise TransferError(
                grpc.StatusCode.INVALID_ARGUMENT,
                "SHA-256 obrigatório: informe 64 caracteres hexadecimais.",
            )
        if metadata.expected_size > self.max_file_size:
            raise TransferError(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                f"Arquivo excede o tamanho máximo de {self.max_file_size} bytes.",
            )

    def Upload(self, request_iterator, context):
        transfer_id = new_transfer_id()
        peer = context.peer()
        start = time.perf_counter()
        metadata = None
        reserved_name = None
        temp_path = None
        output = None
        progress = None
        total = 0
        chunks = 0
        digest = hashlib.sha256()
        error = None
        log_event(logger, "upload.open", transfer=transfer_id, peer=peer)

        try:
            for request in request_iterator:
                kind = request.WhichOneof("payload")
                if metadata is None:
                    if kind != "metadata":
                        raise TransferError(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "A primeira mensagem deve conter os metadados.",
                        )
                    metadata = request.metadata
                    log_event(
                        logger, "upload.start", transfer=transfer_id, peer=peer,
                        filename=metadata.filename, expected_size=metadata.expected_size,
                    )
                    self._validate_metadata(metadata)
                    self._reserve(metadata.filename)
                    reserved_name = metadata.filename
                    fd, temp_name = tempfile.mkstemp(
                        prefix=TEMP_PREFIX, suffix=TEMP_SUFFIX, dir=self.storage_dir
                    )
                    temp_path = Path(temp_name)
                    output = os.fdopen(fd, "wb")
                    progress = ProgressLogger(
                        logger, "upload.progress", metadata.expected_size,
                        transfer=transfer_id, filename=metadata.filename,
                    )
                    continue

                if kind != "chunk":
                    raise TransferError(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Após os metadados, somente chunks são aceitos.",
                    )

                chunk = request.chunk.data
                total += len(chunk)
                chunks += 1
                if total > self.max_file_size:
                    raise TransferError(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        f"Arquivo excede o tamanho máximo de {self.max_file_size} bytes.",
                    )
                if total > metadata.expected_size:
                    raise TransferError(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        f"Recebidos mais bytes que o declarado ({metadata.expected_size}).",
                    )
                output.write(chunk)
                digest.update(chunk)
                progress.update(total)

            if metadata is None:
                raise TransferError(grpc.StatusCode.INVALID_ARGUMENT, "Stream vazio: metadados ausentes.")
            if total != metadata.expected_size:
                raise TransferError(
                    grpc.StatusCode.DATA_LOSS,
                    f"Tamanho divergente: esperado {metadata.expected_size}, recebido {total}.",
                )
            actual_hash = digest.hexdigest()
            if actual_hash != metadata.sha256.lower():
                raise TransferError(grpc.StatusCode.DATA_LOSS, "Falha na verificação SHA-256.")

            output.flush()
            os.fsync(output.fileno())
            output.close()
            output = None
            os.chmod(temp_path, FILE_MODE)
            self._publish(temp_path, metadata.filename, actual_hash)
            temp_path = None

            log_event(
                logger, "upload.done", transfer=transfer_id, peer=peer,
                filename=metadata.filename, bytes=total, chunks=chunks,
                sha256=actual_hash[:16], duration_ms=elapsed_ms(start),
                mib_s=throughput_mib_s(total, start),
            )
            return pb2.TransferStatus(
                success=True, message="Upload concluído e validado.",
                bytes_transferred=total, sha256=actual_hash
            )
        except TransferError as exc:
            error = exc
        except Exception as exc:
            if not context.is_active():
                log_event(
                    logger, "upload.cancelled", logging.WARNING, transfer=transfer_id, peer=peer,
                    filename=metadata.filename if metadata else "", bytes=total,
                    duration_ms=elapsed_ms(start),
                )
                return pb2.TransferStatus()
            logger.exception(
                "upload.failed",
                extra={"fields": {"transfer": transfer_id, "peer": peer, "reason": str(exc)}},
            )
            error = TransferError(grpc.StatusCode.INTERNAL, "Erro interno ao processar o upload.")
        finally:
            if output is not None:
                output.close()
            if temp_path is not None and temp_path.exists():
                temp_path.unlink(missing_ok=True)
                log_event(logger, "upload.tempfile_removed", transfer=transfer_id, path=temp_path.name)
            if reserved_name is not None:
                self._release(reserved_name)

        log_event(
            logger, "upload.rejected", logging.WARNING, transfer=transfer_id, peer=peer,
            filename=metadata.filename if metadata else "", code=error.code.name,
            reason=error.message, bytes=total, duration_ms=elapsed_ms(start),
        )
        context.abort(error.code, error.message)

    def Download(self, request, context):
        transfer_id = new_transfer_id()
        peer = context.peer()
        start = time.perf_counter()
        log_event(logger, "download.start", transfer=transfer_id, peer=peer, filename=request.filename)

        def abort(code, message):
            log_event(
                logger, "download.rejected", logging.WARNING, transfer=transfer_id,
                peer=peer, filename=request.filename, code=code.name, reason=message,
            )
            context.abort(code, message)

        if not valid_filename(request.filename):
            abort(grpc.StatusCode.INVALID_ARGUMENT, "Nome de arquivo inválido.")

        try:
            entry = self._lookup(request.filename)
        except FileNotFoundError:
            entry = None
        if entry is None:
            abort(grpc.StatusCode.NOT_FOUND, "Arquivo inexistente.")

        path = self.storage_dir / request.filename
        sent = 0
        chunks = 0
        finished = False
        progress = ProgressLogger(
            logger, "download.progress", entry.size, transfer=transfer_id, filename=path.name,
        )
        try:
            with path.open("rb") as source:
                yield pb2.DownloadResponse(metadata=pb2.DownloadMetadata(
                    filename=path.name, size_bytes=entry.size, sha256=entry.sha256
                ))
                while data := source.read(CHUNK_SIZE):
                    sent += len(data)
                    chunks += 1
                    yield pb2.DownloadResponse(chunk=pb2.DownloadChunk(data=data))
                    progress.update(sent)

            yield pb2.DownloadResponse(status=pb2.TransferStatus(
                success=(sent == entry.size), message="Download concluído.",
                bytes_transferred=sent, sha256=entry.sha256
            ))
            finished = True
            log_event(
                logger, "download.done", transfer=transfer_id, peer=peer,
                filename=path.name, bytes=sent, chunks=chunks, sha256=entry.sha256[:16],
                duration_ms=elapsed_ms(start), mib_s=throughput_mib_s(sent, start),
            )
        except OSError as exc:
            finished = True
            logger.exception(
                "download.failed",
                extra={"fields": {"transfer": transfer_id, "peer": peer, "reason": str(exc)}},
            )
            context.abort(grpc.StatusCode.INTERNAL, "Erro interno ao ler o arquivo.")
        finally:
            if not finished:
                log_event(
                    logger, "download.cancelled", logging.WARNING, transfer=transfer_id,
                    peer=peer, filename=path.name, bytes=sent, total=entry.size,
                    duration_ms=elapsed_ms(start),
                )


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("deve ser maior que zero")
    return number


def build_parser():
    parser = argparse.ArgumentParser(
        prog="server.py",
        description="Servidor gRPC de transferência de arquivos (Equipe 08).",
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST,
        help="interface de escuta (padrão: %(default)s; env FILESERVER_HOST)",
    )
    parser.add_argument(
        "-p", "--port", type=positive_int, default=DEFAULT_PORT,
        help="porta TCP (padrão: %(default)s; env FILESERVER_PORT)",
    )
    parser.add_argument(
        "-w", "--workers", type=positive_int, default=DEFAULT_WORKERS,
        help="threads do pool que atendem chamadas simultâneas (padrão: %(default)s; env FILESERVER_WORKERS)",
    )
    parser.add_argument(
        "--max-concurrent", type=positive_int, default=None,
        help="máximo de chamadas simultâneas; excedentes recebem RESOURCE_EXHAUSTED (padrão: igual a --workers)",
    )
    parser.add_argument(
        "--max-file-mb", type=positive_int, default=DEFAULT_MAX_FILE_MB,
        help="tamanho máximo por arquivo em MiB (padrão: %(default)s; env FILESERVER_MAX_FILE_MB)",
    )
    parser.add_argument(
        "--storage", type=Path, default=DEFAULT_STORAGE_DIR,
        help="pasta de armazenamento (padrão: %(default)s; env FILESERVER_STORAGE)",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="nível de log (padrão: %(default)s)",
    )
    return parser


def serve(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging("server", getattr(logging, args.log_level))
    max_concurrent = args.max_concurrent or args.workers
    max_file_size = args.max_file_mb * 1024 * 1024
    storage_dir = args.storage.resolve()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="worker"),
        maximum_concurrent_rpcs=max_concurrent,
        options=[("grpc.so_reuseport", 0)],
    )
    pb2_grpc.add_FileServiceServicer_to_server(FileService(storage_dir, max_file_size), server)
    address = f"{args.host}:{args.port}"
    try:
        bound_port = server.add_insecure_port(address)
    except RuntimeError:
        bound_port = 0
    if bound_port == 0:
        log_event(logger, "server.bind_failed", logging.ERROR, address=address)
        print(f"Erro: não foi possível escutar em {address}. A porta já está em uso?", file=sys.stderr)
        return 1
    server.start()
    log_event(
        logger, "server.started", host=args.host, port=args.port, workers=args.workers,
        max_concurrent=max_concurrent, chunk_size=CHUNK_SIZE, max_file_size=max_file_size,
        storage=storage_dir,
    )
    stop_requested = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, frame: stop_requested.set())
    stop_requested.wait()
    log_event(logger, "server.stopping", grace_s=GRACE_SECONDS)
    server.stop(grace=GRACE_SECONDS).wait()
    log_event(logger, "server.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(serve())
