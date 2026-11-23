import hashlib
import logging
import sys
import time
from pathlib import Path

import grpc
import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
from log_config import ProgressLogger, log_event, setup_logging

ADDRESS = "localhost:50051"
CHUNK_SIZE = 64 * 1024
USAGE = "Uso: python src/client.py list | upload <arquivo> | download <nome>"

FRIENDLY_MESSAGES = {
    grpc.StatusCode.UNAVAILABLE: f"Servidor indisponível em {ADDRESS}. Verifique se ele está rodando.",
    grpc.StatusCode.DEADLINE_EXCEEDED: "Tempo limite da operação excedido.",
    grpc.StatusCode.CANCELLED: "Operação cancelada.",
}

logger = logging.getLogger("client")


def elapsed_ms(start):
    return (time.perf_counter() - start) * 1000


def list_files(stub):
    start = time.perf_counter()
    log_event(logger, "list.request", server=ADDRESS)
    response = stub.ListFiles(pb2.ListFilesRequest())
    log_event(logger, "list.response", files=len(response.files), duration_ms=elapsed_ms(start))
    if not response.files:
        print("Nenhum arquivo disponível.")
        return
    print(f"{'NOME':<40} {'TAMANHO (bytes)':>16}  MODIFICADO (UTC)")
    for item in response.files:
        modified = item.modified_at.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{item.name:<40} {item.size_bytes:>16}  {modified}")


class ClientError(Exception):
    pass


def upload(stub, file_path: Path):
    start = time.perf_counter()
    if not file_path.is_file():
        raise ClientError(f"Arquivo local não encontrado: {file_path}")
    digest = hashlib.sha256()
    size = file_path.stat().st_size
    with file_path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(block)
    file_hash = digest.hexdigest()
    log_event(
        logger, "upload.start", server=ADDRESS, filename=file_path.name,
        size=size, sha256=file_hash[:16],
    )
    progress = ProgressLogger(logger, "upload.progress", size, filename=file_path.name)

    def requests():
        yield pb2.UploadRequest(metadata=pb2.UploadMetadata(
            filename=file_path.name, expected_size=size, sha256=file_hash
        ))
        sent = 0
        with file_path.open("rb") as f:
            for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                yield pb2.UploadRequest(chunk=pb2.FileChunk(data=block))
                sent += len(block)
                progress.update(sent)

    result = stub.Upload(requests())
    if result.sha256.lower() != file_hash:
        raise ClientError("O SHA-256 confirmado pelo servidor não confere com o local.")
    log_event(
        logger, "upload.done", filename=file_path.name, bytes=result.bytes_transferred,
        sha256=result.sha256[:16], duration_ms=elapsed_ms(start),
    )


def download(stub, filename: str, output_dir=Path("downloads")):
    start = time.perf_counter()
    if not filename or Path(filename).name != filename or filename in {".", ".."}:
        raise ClientError("Nome de arquivo inválido: informe apenas o nome, sem diretórios.")
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / filename
    temp = destination.with_suffix(destination.suffix + ".part")
    metadata = None
    progress = None
    digest = hashlib.sha256()
    received = 0
    log_event(logger, "download.start", server=ADDRESS, filename=filename)

    try:
        with temp.open("wb") as output:
            for response in stub.Download(pb2.DownloadRequest(filename=filename)):
                kind = response.WhichOneof("payload")
                if kind == "metadata":
                    metadata = response.metadata
                    progress = ProgressLogger(
                        logger, "download.progress", metadata.size_bytes, filename=filename,
                    )
                    log_event(
                        logger, "download.metadata", filename=metadata.filename,
                        size=metadata.size_bytes, sha256=metadata.sha256[:16],
                    )
                elif kind == "chunk":
                    if metadata is None:
                        raise ClientError("Chunk recebido antes dos metadados.")
                    data = response.chunk.data
                    output.write(data)
                    digest.update(data)
                    received += len(data)
                    progress.update(received)
                elif kind == "status":
                    status = response.status
                    if not status.success:
                        raise ClientError(status.message)

        if metadata is None:
            raise ClientError("Metadados não recebidos.")
        if received != metadata.size_bytes:
            raise ClientError(f"Tamanho inválido: {received}/{metadata.size_bytes}.")
        if digest.hexdigest().lower() != metadata.sha256.lower():
            raise ClientError("SHA-256 não confere.")
        temp.replace(destination)
        log_event(
            logger, "download.done", filename=filename, path=destination, bytes=received,
            sha256_ok=True, duration_ms=elapsed_ms(start),
        )
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def run(command, args):
    with grpc.insecure_channel(ADDRESS) as channel:
        stub = pb2_grpc.FileServiceStub(channel)
        if command == "list" and not args:
            list_files(stub)
        elif command == "upload" and len(args) == 1:
            upload(stub, Path(args[0]))
        elif command == "download" and len(args) == 1:
            download(stub, args[0])
        else:
            return False
    return True


def main():
    setup_logging("client")
    if len(sys.argv) < 2:
        print(USAGE)
        return 2
    command, args = sys.argv[1], sys.argv[2:]
    try:
        if not run(command, args):
            print(USAGE)
            return 2
        return 0
    except grpc.RpcError as exc:
        code = exc.code()
        message = FRIENDLY_MESSAGES.get(code, exc.details())
        log_event(logger, f"{command}.failed", logging.ERROR, code=code.name, reason=message)
        print(f"Erro [{code.name}]: {message}", file=sys.stderr)
        return 1
    except ClientError as exc:
        log_event(logger, f"{command}.failed", logging.ERROR, code="CLIENT", reason=str(exc))
        print(f"Erro: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        log_event(logger, f"{command}.cancelled", logging.WARNING)
        print("Operação cancelada pelo usuário.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
