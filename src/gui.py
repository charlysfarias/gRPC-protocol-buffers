import threading
import grpc
import file_service_pb2 as pb2
import file_service_pb2_grpc as pb2_grpc
import tkinter as tk
import hashlib
import re
import unicodedata
from tkinter import ttk
from client import friendly_message
from pathlib import Path
from tkinter import filedialog
from client import CHUNK_SIZE

class FileClientApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Cliente gRPC - Transferência de Arquivos")
        self.root.geometry("650x420")

        # 1. Faixa Superior: Endereço e botão Atualizar
        top_frame = tk.Frame(root, padx=10, pady=8)
        top_frame.pack(fill=tk.X)

        tk.Label(top_frame, text="Servidor:").pack(side=tk.LEFT)
        self.server_entry = tk.Entry(top_frame, width=25)
        self.server_entry.insert(0, "localhost:50051")
        self.server_entry.pack(side=tk.LEFT, padx=5)

        self.refresh_btn = tk.Button(top_frame, text="Atualizar Lista", command=self.on_refresh)
        self.refresh_btn.pack(side=tk.LEFT)

        # 2. Faixa Central: Tabela de arquivos (Treeview)
        table_frame = tk.Frame(root, padx=10, pady=5)
        table_frame.pack(fill=tk.BOTH, expand=True)

        columns = ("nome", "tamanho", "data")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings")
        self.tree.heading("nome", text="Nome do Arquivo")
        self.tree.heading("tamanho", text="Tamanho")
        self.tree.heading("data", text="Modificado em (UTC)")
        self.tree.column("nome", width=300)
        self.tree.column("tamanho", width=120, anchor=tk.E)
        self.tree.column("data", width=180, anchor=tk.CENTER)
        self.tree.pack(fill=tk.BOTH, expand=True)

        # 3. Faixa de Ações: Botões Upload e Download
        action_frame = tk.Frame(root, padx=10, pady=8)
        action_frame.pack(fill=tk.X)

        self.upload_btn = tk.Button(action_frame, text="Enviar Arquivo (Upload)", command=self.on_upload)
        self.upload_btn.pack(side=tk.LEFT, padx=(0, 10))

        self.download_btn = tk.Button(action_frame, text="Baixar Selecionado (Download)", command=self.on_download)
        self.download_btn.pack(side=tk.LEFT)

        # 4. Faixa Inferior: Barra de progresso e status
        bottom_frame = tk.Frame(root, padx=10, pady=8)
        bottom_frame.pack(fill=tk.X)

        self.progress = ttk.Progressbar(bottom_frame, orient=tk.HORIZONTAL, mode="determinate", maximum=100)
        self.progress.pack(fill=tk.X, pady=(0, 5))

        self.status_label = tk.Label(bottom_frame, text="Pronto.", anchor=tk.W)
        self.status_label.pack(fill=tk.X)

        self.on_refresh()

    def sanitize_filename(self, name):
        sem_acento = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
        limpo = re.sub(r"[^A-Za-z0-9._ -]", "_", sem_acento).lstrip("._ -")
        return (limpo or "arquivo")[:200]

    def set_status(self, message, error=False):
        self.status_label.config(text=message, fg="red" if error else "black")

    def format_size(self, size_bytes):
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        return f"{size_bytes / (1024 * 1024):.2f} MB"

    def on_refresh(self):
        server_addr = self.server_entry.get().strip()
        if not server_addr:
            self.set_status("Informe o endereço do servidor.", error=True)
            return
        self.refresh_btn.config(state=tk.DISABLED)
        self.set_status("Buscando arquivos no servidor...")
        threading.Thread(
            target=self._fetch_files_worker, args=(server_addr,), daemon=True
        ).start()

    def _fetch_files_worker(self, server_addr):
        try:
            with grpc.insecure_channel(server_addr) as channel:
                stub = pb2_grpc.FileServiceStub(channel)
                response = stub.ListFiles(pb2.ListFilesRequest(), timeout=5)
            rows = [
                (
                    item.name,
                    self.format_size(item.size_bytes),
                    item.modified_at.ToDatetime().strftime("%Y-%m-%d %H:%M:%S"),
                )
                for item in response.files
            ]
            self.root.after(0, self._update_table, rows)
        except grpc.RpcError as exc:
            msg = friendly_message(exc, server_addr)
            self.root.after(0, self._on_refresh_error, msg)

    def _update_table(self, rows):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in rows:
            self.tree.insert("", tk.END, values=row)
        self.refresh_btn.config(state=tk.NORMAL)
        self.set_status(f"{len(rows)} arquivo(s) encontrado(s) no servidor.")

    def _on_refresh_error(self, message):
        self.refresh_btn.config(state=tk.NORMAL)
        self.set_status(f"Erro ao listar: {message}", error=True)

    def set_buttons_state(self, state):
        self.refresh_btn.config(state=state)
        self.upload_btn.config(state=state)
        self.download_btn.config(state=state)
    
    def on_upload(self):
        server_addr = self.server_entry.get().strip()
        if not server_addr:
            self.set_status("Informe o endereço do servidor.", error=True)
            return

        selected_file = filedialog.askopenfilename(title="Selecione o arquivo para enviar")
        if not selected_file:
            return

        file_path = Path(selected_file)
        self.set_buttons_state(tk.DISABLED)
        self.progress["value"] = 0
        self.set_status(f"Preparando envio de '{file_path.name}'...")

        threading.Thread(
            target=self._upload_worker, args=(server_addr, file_path), daemon=True
        ).start()

    def _upload_worker(self, server_addr, file_path):
        try:
            remote_name = self.sanitize_filename(file_path.name)
            total_size = file_path.stat().st_size
            digest = hashlib.sha256()
            with file_path.open("rb") as f:
                for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                    digest.update(block)
            file_hash = digest.hexdigest()

            def request_generator():
                yield pb2.UploadRequest(
                    metadata=pb2.UploadMetadata(
                        filename=remote_name,
                        expected_size=total_size,
                        sha256=file_hash,
                    )
                )
                sent = 0
                if total_size == 0:
                    self.root.after(0, self._update_progress, 100)
                    return
                with file_path.open("rb") as f:
                    for block in iter(lambda: f.read(CHUNK_SIZE), b""):
                        yield pb2.UploadRequest(chunk=pb2.FileChunk(data=block))
                        sent += len(block)
                        percent = (sent * 100) / total_size
                        self.root.after(0, self._update_progress, percent)

            with grpc.insecure_channel(server_addr) as channel:
                stub = pb2_grpc.FileServiceStub(channel)
                result = stub.Upload(request_generator())

            if result.sha256.lower() != file_hash:
                self.root.after(
                    0, self._on_upload_error, "Falha de integridade: SHA-256 divergente."
                )
                return

            self.root.after(0, self._on_upload_done, remote_name, result.bytes_transferred)
        except grpc.RpcError as exc:
            msg = friendly_message(exc, server_addr)
            self.root.after(0, self._on_upload_error, msg)
        except OSError as exc:
            self.root.after(0, self._on_upload_error, f"Erro ao ler arquivo local: {exc}")

    def _update_progress(self, percent):
        self.progress["value"] = percent

    def _on_upload_done(self, filename, transferred):
        self.set_buttons_state(tk.NORMAL)
        self.progress["value"] = 100
        self.set_status(f"Upload concluído: '{filename}' ({self.format_size(transferred)}).")
        self.on_refresh()

    def _on_upload_error(self, message):
        self.set_buttons_state(tk.NORMAL)
        self.progress["value"] = 0
        self.set_status(f"Erro no upload: {message}", error=True)

    def on_download(self):
        server_addr = self.server_entry.get().strip()
        if not server_addr:
            self.set_status("Informe o endereço do servidor.", error=True)
            return

        selection = self.tree.selection()
        if not selection:
            self.set_status("Selecione um arquivo na tabela para baixar.", error=True)
            return

        filename = str(self.tree.item(selection[0], "values")[0])
        save_path = filedialog.asksaveasfilename(
            title="Salvar arquivo como", initialfile=filename
        )
        if not save_path:
            return

        destination = Path(save_path)
        self.set_buttons_state(tk.DISABLED)
        self.progress["value"] = 0
        self.set_status(f"Baixando '{filename}'...")

        threading.Thread(
            target=self._download_worker,
            args=(server_addr, filename, destination),
            daemon=True,
        ).start()

    def _download_worker(self, server_addr, filename, destination):
        temp_path = destination.with_suffix(destination.suffix + ".part")
        metadata = None
        digest = hashlib.sha256()
        received = 0

        try:
            with temp_path.open("wb") as output:
                with grpc.insecure_channel(server_addr) as channel:
                    stub = pb2_grpc.FileServiceStub(channel)
                    for response in stub.Download(pb2.DownloadRequest(filename=filename)):
                        kind = response.WhichOneof("payload")
                        if kind == "metadata":
                            metadata = response.metadata
                            if metadata.size_bytes == 0:
                                self.root.after(0, self._update_progress, 100)
                        elif kind == "chunk":
                            if metadata is None:
                                raise RuntimeError("Chunk recebido antes dos metadados.")
                            data = response.chunk.data
                            output.write(data)
                            digest.update(data)
                            received += len(data)
                            if metadata.size_bytes > 0:
                                percent = (received * 100) / metadata.size_bytes
                                self.root.after(0, self._update_progress, percent)
                        elif kind == "status":
                            if not response.status.success:
                                raise RuntimeError(response.status.message)

            if metadata is None or received != metadata.size_bytes:
                raise RuntimeError("Transferência incompleta.")
            if digest.hexdigest().lower() != metadata.sha256.lower():
                raise RuntimeError("Falha de integridade: SHA-256 não confere.")

            temp_path.replace(destination)
            self.root.after(0, self._on_download_done, destination.name, received)
        except grpc.RpcError as exc:
            temp_path.unlink(missing_ok=True)
            msg = friendly_message(exc, server_addr)
            self.root.after(0, self._on_download_error, msg)
        except Exception as exc:
            temp_path.unlink(missing_ok=True)
            self.root.after(0, self._on_download_error, str(exc))

    def _on_download_done(self, filename, received):
        self.set_buttons_state(tk.NORMAL)
        self.progress["value"] = 100
        self.set_status(f"Download concluído: '{filename}' ({self.format_size(received)}).")

    def _on_download_error(self, message):
        self.set_buttons_state(tk.NORMAL)
        self.progress["value"] = 0
        self.set_status(f"Erro no download: {message}", error=True)


if __name__ == "__main__":
    root = tk.Tk()
    app = FileClientApp(root)
    root.mainloop()