from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import os
import re
import tempfile
import threading

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc

HOST = "0.0.0.0"
PORT = 50051
CHUNK_SIZE = 64 * 1024            
MAX_FILE_SIZE = 100 * 1024 * 1024  
STORAGE_DIR = Path(__file__).resolve().parent.parent / "storage"
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,199}$")


def valid_filename(name):
    return bool(name and SAFE_NAME.fullmatch(name)
                and name not in {".", ".."}
                and "/" not in name and "\\" not in name)


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

    def _refresh(self, name):
        path = self.storage_dir / name
        if path.is_file():
            stat = path.stat()
            self.index[name] = {"size": stat.st_size, "mtime": stat.st_mtime}

    def ListFiles(self, request, context):
        response = pb2.ListFilesResponse()
        with self.lock:
            for name in sorted(self.index):
                path = self.storage_dir / name
                if not path.is_file():
                    continue
                stat = path.stat()
                info = response.files.add(name=name, size_bytes=stat.st_size)
                info.modified_at.FromDatetime(
                    datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                )
        response.status.success = True
        response.status.message = "Listagem concluída."
        response.status.bytes_transferred = 0
        return response

    def Upload(self, request_iterator, context):
        metadata = None
        temp_path = None
        total = 0
        digest = hashlib.sha256()

        try:
            for request in request_iterator:
                kind = request.WhichOneof("payload")
                if metadata is None:
                    if kind != "metadata":
                        return pb2.TransferStatus(
                            success=False,
                            message="A primeira mensagem deve conter os metadados."
                        )
                    metadata = request.metadata
                    if not valid_filename(metadata.filename):
                        return pb2.TransferStatus(success=False, message="Nome de arquivo inválido.")
                    if metadata.expected_size > MAX_FILE_SIZE:
                        return pb2.TransferStatus(success=False, message="Arquivo excede o tamanho máximo.")
                    destination = self.storage_dir / metadata.filename
                    if destination.exists():
                        return pb2.TransferStatus(success=False, message="Já existe um arquivo com esse nome.")
                    fd, temp_name = tempfile.mkstemp(
                        prefix=".upload-", suffix=".tmp", dir=self.storage_dir
                    )
                    os.close(fd)
                    temp_path = Path(temp_name)
                    continue

                if kind != "chunk":
                    raise ValueError("Após os metadados, somente chunks são aceitos.")

                chunk = request.chunk.data
                total += len(chunk)
                if total > MAX_FILE_SIZE or total > metadata.expected_size:
                    raise ValueError("Tamanho recebido excede o tamanho declarado/limite.")
                with temp_path.open("ab") as output:
                    output.write(chunk)
                digest.update(chunk)

            if metadata is None:
                return pb2.TransferStatus(success=False, message="Stream vazio: metadados ausentes.")
            actual_hash = digest.hexdigest()
            if total != metadata.expected_size:
                raise ValueError(f"Tamanho divergente: esperado {metadata.expected_size}, recebido {total}.")
            if metadata.sha256 and actual_hash.lower() != metadata.sha256.lower():
                raise ValueError("Falha na verificação SHA-256.")

            destination = self.storage_dir / metadata.filename
            with self.lock:
                if destination.exists():
                    raise ValueError("O arquivo já existe.")
                os.replace(temp_path, destination)
                temp_path = None
                self._refresh(metadata.filename)

            return pb2.TransferStatus(
                success=True, message="Upload concluído e validado.",
                bytes_transferred=total, sha256=actual_hash
            )
        except Exception as exc:
            return pb2.TransferStatus(success=False, message=str(exc), bytes_transferred=total)
        finally:
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)

    def Download(self, request, context):
        if not valid_filename(request.filename):
            yield pb2.DownloadResponse(status=pb2.TransferStatus(
                success=False, message="Nome de arquivo inválido."
            ))
            return

        path = self.storage_dir / request.filename
        if not path.is_file():
            yield pb2.DownloadResponse(status=pb2.TransferStatus(
                success=False, message="Arquivo inexistente."
            ))
            return

        size = path.stat().st_size
        if size > MAX_FILE_SIZE:
            yield pb2.DownloadResponse(status=pb2.TransferStatus(
                success=False, message="Arquivo excede o tamanho máximo permitido."
            ))
            return

        digest = hashlib.sha256()
        with path.open("rb") as source:
            while data := source.read(CHUNK_SIZE):
                digest.update(data)
        file_hash = digest.hexdigest()

        yield pb2.DownloadResponse(metadata=pb2.DownloadMetadata(
            filename=path.name, size_bytes=size, sha256=file_hash
        ))

        sent = 0
        with path.open("rb") as source:
            while data := source.read(CHUNK_SIZE):
                sent += len(data)
                yield pb2.DownloadResponse(chunk=pb2.DownloadChunk(data=data))

        yield pb2.DownloadResponse(status=pb2.TransferStatus(
            success=(sent == size), message="Download concluído.",
            bytes_transferred=sent, sha256=file_hash
        ))


def serve():
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FileServiceServicer_to_server(
        FileService(STORAGE_DIR), server
    )
    server.add_insecure_port(f"{HOST}:{PORT}")
    server.start()
    print(f"Servidor gRPC ouvindo em {HOST}:{PORT}")
    print(f"Armazenamento: {STORAGE_DIR}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
