import argparse
import hashlib
import logging
import os
import sys
import time
from pathlib import Path

import grpc
import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
from log_config import ProgressLogger, log_event, setup_logging

DEFAULT_SERVER = os.environ.get("FILESERVER_ADDRESS", "localhost:50051")
DEFAULT_OUTPUT_DIR = os.environ.get("FILESERVER_DOWNLOAD_DIR", "downloads")
CHUNK_SIZE = 64 * 1024

logger = logging.getLogger("client")


class ClientError(Exception):
    pass


def elapsed_ms(start):
    return (time.perf_counter() - start) * 1000


def friendly_message(exc, server):
    code = exc.code()
    details = exc.details() or ""
    if code == grpc.StatusCode.UNAVAILABLE:
        return f"Servidor indisponível em {server}. Verifique se ele está rodando."
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return "Tempo limite da operação excedido."
    if code == grpc.StatusCode.CANCELLED:
        return "Operação cancelada."
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED and "Concurrent RPC limit" in details:
        return "Servidor ocupado: limite de transferências simultâneas atingido. Tente novamente."
    return details


def list_files(stub, args):
    start = time.perf_counter()
    log_event(logger, "list.request", server=args.server)
    response = stub.ListFiles(pb2.ListFilesRequest(), timeout=args.timeout)
    log_event(logger, "list.response", files=len(response.files), duration_ms=elapsed_ms(start))
    if not response.files:
        print("Nenhum arquivo disponível.")
        return
    print(f"{'NOME':<40} {'TAMANHO (bytes)':>16}  MODIFICADO (UTC)")
    for item in response.files:
        modified = item.modified_at.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{item.name:<40} {item.size_bytes:>16}  {modified}")


def upload(stub, args):
    start = time.perf_counter()
    file_path = Path(args.file)
    if not file_path.is_file():
        raise ClientError(f"Arquivo local não encontrado: {file_path}")
    remote_name = args.remote_name or file_path.name
    digest = hashlib.sha256()
    size = file_path.stat().st_size
    with file_path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(block)
    file_hash = digest.hexdigest()
    log_event(
        logger, "upload.start", server=args.server, filename=remote_name,
        source=file_path, size=size, sha256=file_hash[:16],
    )
    progress = ProgressLogger(logger, "upload.progress", size, filename=remote_name)

    def requests():
        yield pb2.UploadRequest(metadata=pb2.UploadMetadata(
            filename=remote_name, expected_size=size, sha256=file_hash
        ))
        sent = 0
        with file_path.open("rb") as f:
            for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                yield pb2.UploadRequest(chunk=pb2.FileChunk(data=block))
                sent += len(block)
                progress.update(sent)

    result = stub.Upload(requests(), timeout=args.timeout)
    if result.sha256.lower() != file_hash:
        raise ClientError("O SHA-256 confirmado pelo servidor não confere com o local.")
    log_event(
        logger, "upload.done", filename=remote_name, bytes=result.bytes_transferred,
        sha256=result.sha256[:16], duration_ms=elapsed_ms(start),
    )
    print(f"Enviado: {remote_name} ({result.bytes_transferred} bytes, SHA-256 {result.sha256})")


def download(stub, args):
    start = time.perf_counter()
    filename = args.name
    if not filename or Path(filename).name != filename or filename in {".", ".."}:
        raise ClientError("Nome de arquivo inválido: informe apenas o nome, sem diretórios.")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / filename
    temp = destination.with_suffix(destination.suffix + ".part")
    metadata = None
    progress = None
    digest = hashlib.sha256()
    received = 0
    log_event(logger, "download.start", server=args.server, filename=filename)

    try:
        with temp.open("wb") as output:
            for response in stub.Download(pb2.DownloadRequest(filename=filename), timeout=args.timeout):
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
        print(f"Baixado: {destination} ({received} bytes, SHA-256 verificado)")
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def positive_float(value):
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("deve ser maior que zero")
    return number


def build_parser():
    parser = argparse.ArgumentParser(
        prog="client.py",
        description="Cliente de terminal do serviço de arquivos gRPC (Equipe 08).",
    )
    parser.add_argument(
        "-s", "--server", default=DEFAULT_SERVER,
        help="endereço host:porta do servidor (padrão: %(default)s; env FILESERVER_ADDRESS)",
    )
    parser.add_argument(
        "-t", "--timeout", type=positive_float, default=None,
        help="deadline da chamada em segundos (padrão: sem limite)",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="nível de log (padrão: %(default)s)",
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMANDO")

    list_cmd = commands.add_parser("list", help="lista os arquivos disponíveis no servidor")
    list_cmd.set_defaults(handler=list_files)

    upload_cmd = commands.add_parser("upload", help="envia um arquivo (client-side streaming)")
    upload_cmd.add_argument("file", help="caminho do arquivo local")
    upload_cmd.add_argument(
        "--as", dest="remote_name", default=None,
        help="nome com que o arquivo será salvo no servidor (padrão: nome do arquivo local)",
    )
    upload_cmd.set_defaults(handler=upload)

    download_cmd = commands.add_parser("download", help="baixa um arquivo (server-side streaming)")
    download_cmd.add_argument("name", help="nome do arquivo no servidor")
    download_cmd.add_argument(
        "-o", "--output-dir", default=DEFAULT_OUTPUT_DIR,
        help="pasta de destino (padrão: %(default)s; env FILESERVER_DOWNLOAD_DIR)",
    )
    download_cmd.set_defaults(handler=download)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging("client", getattr(logging, args.log_level))
    try:
        with grpc.insecure_channel(args.server) as channel:
            args.handler(pb2_grpc.FileServiceStub(channel), args)
        return 0
    except grpc.RpcError as exc:
        message = friendly_message(exc, args.server)
        log_event(logger, f"{args.command}.failed", logging.ERROR, code=exc.code().name, reason=message)
        print(f"Erro [{exc.code().name}]: {message}", file=sys.stderr)
        return 1
    except ClientError as exc:
        log_event(logger, f"{args.command}.failed", logging.ERROR, code="CLIENT", reason=str(exc))
        print(f"Erro: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        log_event(logger, f"{args.command}.cancelled", logging.WARNING)
        print("Operação cancelada pelo usuário.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
