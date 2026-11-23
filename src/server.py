from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import logging
import os
import re
import signal
import tempfile
import threading
import time
import uuid

import grpc

import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
from log_config import ProgressLogger, log_event, setup_logging

HOST = "0.0.0.0"
PORT = 50051
MAX_WORKERS = 10
CHUNK_SIZE = 64 * 1024
MAX_FILE_SIZE = 100 * 1024 * 1024
STORAGE_DIR = Path(__file__).resolve().parent.parent / "storage"
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,199}$")

logger = logging.getLogger("server")


class TransferError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def valid_filename(name):
    return bool(name and SAFE_NAME.fullmatch(name)
                and name not in {".", ".."}
                and "/" not in name and "\\" not in name)


def new_transfer_id():
    return uuid.uuid4().hex[:8]


def elapsed_ms(start):
    return (time.perf_counter() - start) * 1000


def throughput_mib_s(size, start):
    seconds = time.perf_counter() - start
    return (size / (1024 * 1024)) / seconds if seconds > 0 else 0.0


class FileService(pb2_grpc.FileServiceServicer):
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.index = {}
        self._rebuild_index()

    def _rebuild_index(self):
        with self.lock:
            for path in self.storage_dir.iterdir():
                if path.is_file() and valid_filename(path.name):
                    stat = path.stat()
                    self.index[path.name] = {
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                    }
        log_event(logger, "index.rebuilt", files=len(self.index))

    def _refresh(self, name):
        path = self.storage_dir / name
        if path.is_file():
            stat = path.stat()
            self.index[name] = {"size": stat.st_size, "mtime": stat.st_mtime}

    def ListFiles(self, request, context):
        start = time.perf_counter()
        peer = context.peer()
        log_event(logger, "list.start", peer=peer)
        response = pb2.ListFilesResponse()
        total_bytes = 0
        with self.lock:
            for name in sorted(self.index):
                path = self.storage_dir / name
                if not path.is_file():
                    continue
                stat = path.stat()
                total_bytes += stat.st_size
                info = response.files.add(name=name, size_bytes=stat.st_size)
                info.modified_at.FromDatetime(
                    datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                )
        log_event(
            logger, "list.done", peer=peer, files=len(response.files),
            total_bytes=total_bytes, duration_ms=elapsed_ms(start),
        )
        return response

    def Upload(self, request_iterator, context):
        transfer_id = new_transfer_id()
        peer = context.peer()
        start = time.perf_counter()
        metadata = None
        temp_path = None
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
                    if not valid_filename(metadata.filename):
                        raise TransferError(grpc.StatusCode.INVALID_ARGUMENT, "Nome de arquivo inválido.")
                    if metadata.expected_size > MAX_FILE_SIZE:
                        raise TransferError(
                            grpc.StatusCode.RESOURCE_EXHAUSTED,
                            f"Arquivo excede o tamanho máximo de {MAX_FILE_SIZE} bytes.",
                        )
                    if (self.storage_dir / metadata.filename).exists():
                        raise TransferError(grpc.StatusCode.ALREADY_EXISTS, "Já existe um arquivo com esse nome.")
                    fd, temp_name = tempfile.mkstemp(
                        prefix=".upload-", suffix=".tmp", dir=self.storage_dir
                    )
                    os.close(fd)
                    temp_path = Path(temp_name)
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
                if total > MAX_FILE_SIZE:
                    raise TransferError(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        f"Arquivo excede o tamanho máximo de {MAX_FILE_SIZE} bytes.",
                    )
                if total > metadata.expected_size:
                    raise TransferError(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        f"Recebidos mais bytes que o declarado ({metadata.expected_size}).",
                    )
                with temp_path.open("ab") as output:
                    output.write(chunk)
                digest.update(chunk)
                progress.update(total)

            if metadata is None:
                raise TransferError(grpc.StatusCode.INVALID_ARGUMENT, "Stream vazio: metadados ausentes.")
            actual_hash = digest.hexdigest()
            if total != metadata.expected_size:
                raise TransferError(
                    grpc.StatusCode.DATA_LOSS,
                    f"Tamanho divergente: esperado {metadata.expected_size}, recebido {total}.",
                )
            if metadata.sha256 and actual_hash.lower() != metadata.sha256.lower():
                raise TransferError(grpc.StatusCode.DATA_LOSS, "Falha na verificação SHA-256.")

            destination = self.storage_dir / metadata.filename
            with self.lock:
                if destination.exists():
                    raise TransferError(grpc.StatusCode.ALREADY_EXISTS, "Já existe um arquivo com esse nome.")
                os.replace(temp_path, destination)
                temp_path = None
                self._refresh(metadata.filename)

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
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)
                log_event(logger, "upload.tempfile_removed", transfer=transfer_id, path=temp_path.name)

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

        path = self.storage_dir / request.filename
        if not path.is_file():
            abort(grpc.StatusCode.NOT_FOUND, "Arquivo inexistente.")

        size = path.stat().st_size
        if size > MAX_FILE_SIZE:
            abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                f"Arquivo excede o tamanho máximo de {MAX_FILE_SIZE} bytes.",
            )

        digest = hashlib.sha256()
        with path.open("rb") as source:
            while data := source.read(CHUNK_SIZE):
                digest.update(data)
        file_hash = digest.hexdigest()

        sent = 0
        chunks = 0
        finished = False
        progress = ProgressLogger(
            logger, "download.progress", size, transfer=transfer_id, filename=path.name,
        )
        try:
            yield pb2.DownloadResponse(metadata=pb2.DownloadMetadata(
                filename=path.name, size_bytes=size, sha256=file_hash
            ))

            with path.open("rb") as source:
                while data := source.read(CHUNK_SIZE):
                    sent += len(data)
                    chunks += 1
                    yield pb2.DownloadResponse(chunk=pb2.DownloadChunk(data=data))
                    progress.update(sent)

            yield pb2.DownloadResponse(status=pb2.TransferStatus(
                success=(sent == size), message="Download concluído.",
                bytes_transferred=sent, sha256=file_hash
            ))
            finished = True
            log_event(
                logger, "download.done", transfer=transfer_id, peer=peer,
                filename=path.name, bytes=sent, chunks=chunks, sha256=file_hash[:16],
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
                    peer=peer, filename=path.name, bytes=sent, total=size,
                    duration_ms=elapsed_ms(start),
                )


def serve():
    setup_logging("server")
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="worker")
    )
    pb2_grpc.add_FileServiceServicer_to_server(
        FileService(STORAGE_DIR), server
    )
    server.add_insecure_port(f"{HOST}:{PORT}")
    server.start()
    log_event(
        logger, "server.started", host=HOST, port=PORT, workers=MAX_WORKERS,
        chunk_size=CHUNK_SIZE, max_file_size=MAX_FILE_SIZE, storage=STORAGE_DIR,
    )
    stop_requested = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, frame: stop_requested.set())
    stop_requested.wait()
    log_event(logger, "server.stopping", grace_s=5)
    server.stop(grace=5).wait()
    log_event(logger, "server.stopped")


if __name__ == "__main__":
    serve()
