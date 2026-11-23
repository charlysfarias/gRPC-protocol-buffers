import argparse
import hashlib
import logging
import os
import statistics
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import grpc

import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
from log_config import log_event, setup_logging

CHUNK_SIZE = 64 * 1024

logger = logging.getLogger("concurrency-test")


@dataclass
class Result:
    client: int
    operation: str
    filename: str
    ok: bool
    duration_s: float
    size: int
    detail: str = ""


@dataclass
class Report:
    results: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, result):
        with self.lock:
            self.results.append(result)


def upload_requests(name, data, file_hash):
    yield pb2.UploadRequest(metadata=pb2.UploadMetadata(
        filename=name, expected_size=len(data), sha256=file_hash
    ))
    for offset in range(0, len(data), CHUNK_SIZE):
        yield pb2.UploadRequest(chunk=pb2.FileChunk(data=data[offset:offset + CHUNK_SIZE]))


def do_upload(stub, client_id, name, data, file_hash, timeout):
    start = time.perf_counter()
    try:
        response = stub.Upload(upload_requests(name, data, file_hash), timeout=timeout)
        ok = response.sha256 == file_hash and response.bytes_transferred == len(data)
        detail = "" if ok else "confirmação do servidor divergente"
    except grpc.RpcError as exc:
        ok, detail = False, f"{exc.code().name}: {exc.details()}"
    return Result(client_id, "upload", name, ok, time.perf_counter() - start, len(data), detail)


def do_download(stub, client_id, name, expected_hash, expected_size, timeout):
    start = time.perf_counter()
    digest = hashlib.sha256()
    received = 0
    server_hash = None
    try:
        for response in stub.Download(pb2.DownloadRequest(filename=name), timeout=timeout):
            kind = response.WhichOneof("payload")
            if kind == "metadata":
                server_hash = response.metadata.sha256
            elif kind == "chunk":
                digest.update(response.chunk.data)
                received += len(response.chunk.data)
        actual = digest.hexdigest()
        ok = actual == expected_hash == server_hash and received == expected_size
        detail = "" if ok else f"hash/tamanho divergente ({received}/{expected_size} bytes)"
    except grpc.RpcError as exc:
        ok, detail = False, f"{exc.code().name}: {exc.details()}"
    return Result(client_id, "download", name, ok, time.perf_counter() - start, received, detail)


def client_worker(client_id, args, barrier, report, shared_stub, prefix):
    channel = None
    if shared_stub is None:
        channel = grpc.insecure_channel(args.server)
        stub = pb2_grpc.FileServiceStub(channel)
    else:
        stub = shared_stub
    try:
        data = os.urandom(int(args.size_mb * 1024 * 1024))
        file_hash = hashlib.sha256(data).hexdigest()
        name = f"{prefix}-shared.bin" if args.same_name else f"{prefix}-c{client_id:03d}.bin"
        barrier.wait()
        upload_result = do_upload(stub, client_id, name, data, file_hash, args.timeout)
        report.add(upload_result)
        log_event(
            logger, "client.upload", logging.INFO if upload_result.ok else logging.WARNING,
            client=client_id, filename=name, ok=upload_result.ok,
            duration_ms=upload_result.duration_s * 1000, detail=upload_result.detail,
        )
        if upload_result.ok and not args.skip_download:
            download_result = do_download(stub, client_id, name, file_hash, len(data), args.timeout)
            report.add(download_result)
            log_event(
                logger, "client.download", logging.INFO if download_result.ok else logging.WARNING,
                client=client_id, filename=name, ok=download_result.ok,
                duration_ms=download_result.duration_s * 1000, detail=download_result.detail,
            )
    finally:
        if channel is not None:
            channel.close()


def percentile(values, pct):
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def summarize(report, wall_seconds, args):
    print()
    print("=" * 78)
    print(
        f"Resumo: {args.clients} clientes, {args.size_mb} MiB cada, "
        f"canal {'compartilhado' if args.shared_channel else 'próprio por cliente'}, "
        f"modo {'mesmo nome' if args.same_name else 'nomes distintos'}"
    )
    print("=" * 78)
    print(f"{'OPERAÇÃO':<10} {'OK':>4} {'FALHA':>6} {'MIN (s)':>9} {'MÉDIA (s)':>10} {'P95 (s)':>9} {'MÁX (s)':>9}")
    for operation in ("upload", "download"):
        items = [r for r in report.results if r.operation == operation]
        if not items:
            continue
        durations = [r.duration_s for r in items if r.ok] or [0.0]
        ok = sum(r.ok for r in items)
        print(
            f"{operation:<10} {ok:>4} {len(items) - ok:>6} {min(durations):>9.3f} "
            f"{statistics.mean(durations):>10.3f} {percentile(durations, 95):>9.3f} {max(durations):>9.3f}"
        )
    total_bytes = sum(r.size for r in report.results if r.ok)
    print(f"Tempo total: {wall_seconds:.3f} s | Vazão agregada: {total_bytes / (1024 * 1024) / wall_seconds:.2f} MiB/s")
    failures = [r for r in report.results if not r.ok]
    if failures:
        print("\nFalhas:")
        for r in sorted(failures, key=lambda item: item.client):
            print(f"  cliente {r.client:03d} {r.operation:<8} {r.filename}: {r.detail}")


def evaluate(report, args):
    uploads = [r for r in report.results if r.operation == "upload"]
    downloads = [r for r in report.results if r.operation == "download"]
    if args.same_name:
        winners = [r for r in uploads if r.ok]
        losers = [r for r in uploads if not r.ok]
        expected_losers = all(r.detail.startswith("ALREADY_EXISTS") for r in losers)
        downloads_ok = all(r.ok for r in downloads)
        passed = len(winners) == 1 and expected_losers and downloads_ok
        print(
            f"\nVerificação de corrida: {len(winners)} vencedor(es), {len(losers)} rejeitado(s) "
            f"com ALREADY_EXISTS -> {'APROVADO' if passed else 'REPROVADO'}"
        )
        return passed
    passed = len(uploads) == args.clients and all(r.ok for r in report.results)
    print(f"\nVerificação de integridade: {'APROVADO' if passed else 'REPROVADO'}")
    return passed


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("deve ser maior que zero")
    return number


def positive_float(value):
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("deve ser maior que zero")
    return number


def build_parser():
    parser = argparse.ArgumentParser(
        prog="concurrency_test.py",
        description="Dispara N clientes simultâneos contra o servidor e verifica a integridade de cada transferência.",
    )
    parser.add_argument("-s", "--server", default=os.environ.get("FILESERVER_ADDRESS", "localhost:50051"),
                        help="endereço host:porta do servidor (padrão: %(default)s)")
    parser.add_argument("-n", "--clients", type=positive_int, default=10,
                        help="número de clientes simultâneos (padrão: %(default)s)")
    parser.add_argument("--size-mb", type=positive_float, default=5,
                        help="tamanho do arquivo de cada cliente em MiB (padrão: %(default)s)")
    parser.add_argument("-t", "--timeout", type=positive_float, default=120,
                        help="deadline de cada chamada em segundos (padrão: %(default)s)")
    parser.add_argument("--shared-channel", action="store_true",
                        help="todos os clientes usam um único canal (uma conexão HTTP/2 multiplexada)")
    parser.add_argument("--same-name", action="store_true",
                        help="todos enviam o mesmo nome: exatamente um deve vencer, os demais recebem ALREADY_EXISTS")
    parser.add_argument("--skip-download", action="store_true", help="executa apenas os uploads")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="nível de log (padrão: %(default)s)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging("concurrency-test", getattr(logging, args.log_level))
    prefix = f"loadtest-{uuid.uuid4().hex[:6]}"
    report = Report()
    barrier = threading.Barrier(args.clients + 1)
    shared_channel = grpc.insecure_channel(args.server) if args.shared_channel else None
    shared_stub = pb2_grpc.FileServiceStub(shared_channel) if shared_channel else None

    with grpc.insecure_channel(args.server) as probe:
        try:
            grpc.channel_ready_future(probe).result(timeout=5)
        except grpc.FutureTimeoutError:
            print(f"Erro: servidor indisponível em {args.server}.", file=sys.stderr)
            return 1

    log_event(
        logger, "test.start", server=args.server, clients=args.clients, size_mb=args.size_mb,
        shared_channel=args.shared_channel, same_name=args.same_name, prefix=prefix,
    )
    threads = [
        threading.Thread(
            target=client_worker, name=f"client-{i:03d}",
            args=(i, args, barrier, report, shared_stub, prefix),
        )
        for i in range(args.clients)
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    start = time.perf_counter()
    for thread in threads:
        thread.join()
    wall = time.perf_counter() - start
    if shared_channel is not None:
        shared_channel.close()

    summarize(report, wall, args)
    passed = evaluate(report, args)
    log_event(logger, "test.done", passed=passed, duration_s=wall)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
