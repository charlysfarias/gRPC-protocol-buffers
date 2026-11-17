import hashlib
import sys
from pathlib import Path

import grpc
import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc

ADDRESS = "localhost:50051"
CHUNK_SIZE = 64 * 1024


def list_files(stub):
    response = stub.ListFiles(pb2.ListFilesRequest())
    if not response.status.success:
        print("Erro:", response.status.message)
        return
    for item in response.files:
        print(f"{item.name} | {item.size_bytes} bytes | {item.modified_at.ToDatetime()}")


def upload(stub, file_path: Path):
    digest = hashlib.sha256()
    size = file_path.stat().st_size
    with file_path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(block)

    def requests():
        yield pb2.UploadRequest(metadata=pb2.UploadMetadata(
            filename=file_path.name, expected_size=size, sha256=digest.hexdigest()
        ))
        with file_path.open("rb") as f:
            for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                yield pb2.UploadRequest(chunk=pb2.FileChunk(data=block))

    result = stub.Upload(requests())
    print(result.message, "|", result.bytes_transferred, "bytes")
    if result.sha256:
        print("SHA-256:", result.sha256)


def download(stub, filename: str, output_dir=Path("downloads")):
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / filename
    temp = destination.with_suffix(destination.suffix + ".part")
    metadata = None
    digest = hashlib.sha256()
    received = 0

    try:
        with temp.open("wb") as output:
            for response in stub.Download(pb2.DownloadRequest(filename=filename)):
                kind = response.WhichOneof("payload")
                if kind == "metadata":
                    metadata = response.metadata
                elif kind == "chunk":
                    if metadata is None:
                        raise RuntimeError("Chunk recebido antes dos metadados.")
                    data = response.chunk.data
                    output.write(data)
                    digest.update(data)
                    received += len(data)
                elif kind == "status":
                    status = response.status
                    if not status.success:
                        raise RuntimeError(status.message)

        if metadata is None:
            raise RuntimeError("Metadados não recebidos.")
        if received != metadata.size_bytes:
            raise RuntimeError(f"Tamanho inválido: {received}/{metadata.size_bytes}.")
        if digest.hexdigest().lower() != metadata.sha256.lower():
            raise RuntimeError("SHA-256 não confere.")
        temp.replace(destination)
        print(f"Download salvo em {destination} ({received} bytes).")
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def main():
    if len(sys.argv) < 2:
        print("Use: python src/client.py list | upload <arquivo> | download <nome>")
        return
    with grpc.insecure_channel(ADDRESS) as channel:
        stub = pb2_grpc.FileServiceStub(channel)
        command = sys.argv[1]
        if command == "list":
            list_files(stub)
        elif command == "upload" and len(sys.argv) == 3:
            upload(stub, Path(sys.argv[2]))
        elif command == "download" and len(sys.argv) == 3:
            download(stub, sys.argv[2])
        else:
            print("Errou feio, errou rude. Comando inválido.")

if __name__ == "__main__":
    main()
