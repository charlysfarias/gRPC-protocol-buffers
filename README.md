# Equipe 08 — Transferência de arquivos com gRPC

## 1. Objetivo e escopo

O sistema implementa um serviço distribuído de armazenamento e transferência de arquivos usando gRPC e Protocol Buffers.

Operações:
- `ListFiles`: consulta os arquivos disponíveis, com nome, tamanho em bytes e data de modificação.
- `Upload`: recebe um arquivo por **client-side streaming**.
- `Download`: transmite um arquivo por **server-side streaming**.

O escopo desta primeira entrega contempla o contrato `.proto`, estratégia de fragmentação, armazenamento, índice em memória, validações, geração dos stubs e servidor funcional na porta TCP `50051`.

## 2. Arquitetura

```text
+----------------+       gRPC / HTTP/2       +----------------------+
| Cliente Python | ------------------------> | Servidor gRPC :50051 |
|                |                            |                      |
| ListFiles      | <------------------------ | FileService          |
| Upload stream  | ------------------------> | - valida metadados   |
| Download      | <------------------------ | - recebe/envia chunks|
+----------------+                            | - calcula SHA-256    |
                                              +----------+-----------+
                                                         |
                                              +----------v-----------+
                                              | storage/             |
                                              | arquivos persistidos |
                                              +----------------------+
```

O servidor mantém um índice em memória (`dict`) com nome, tamanho e instante de modificação. No início, o índice é reconstruído examinando os arquivos já persistidos no diretório `storage/`. O sistema de arquivos é a fonte persistente; o índice é uma estrutura auxiliar em memória.

## 3. Contrato Protocol Buffers

O contrato está em `proto/file_service.proto`.

| RPC | Tipo | Responsabilidade |
|---|---|---|
| `ListFiles` | Unary | Retorna os metadados dos arquivos |
| `Upload` | Client streaming | Recebe metadados e uma sequência de chunks |
| `Download` | Server streaming | Envia metadados, chunks e status final |

### Formato do upload

A primeira mensagem do stream deve ser `UploadMetadata`, contendo:
- `filename`: nome simples do arquivo;
- `expected_size`: tamanho esperado em bytes;
- `sha256`: hash SHA-256 calculado pelo cliente.

As mensagens seguintes são `FileChunk`, cada uma com até 64 KiB de dados. O servidor rejeita streams sem metadados, chunks antes dos metadados, tamanho excedente e divergência em relação ao tamanho declarado.

### Formato do download

O servidor envia, nesta ordem:
1. `DownloadMetadata` com nome, tamanho e SHA-256;
2. zero ou mais `DownloadChunk`;
3. `TransferStatus` final.

O cliente só considera o download concluído depois de verificar o tamanho recebido e comparar o SHA-256.

## 4. Fragmentação e integridade

- **Tamanho do chunk:** 64 KiB (65.536 bytes).
- **Metadados:** primeira mensagem do upload; primeira resposta do download.
- **Tamanho máximo:** 100 MiB por arquivo.
- **Integridade:** SHA-256 calculado sobre o conteúdo integral.
- **Verificação:** o servidor compara o tamanho recebido com `expected_size` e o hash calculado com o hash informado. No download, o cliente repete as verificações.
- **Persistência segura:** o upload é gravado em arquivo temporário no próprio diretório de armazenamento. Somente após as verificações o arquivo é movido atomicamente para o nome definitivo. Em caso de falha, o temporário é removido.

O tamanho de 64 KiB reduz a necessidade de manter o arquivo inteiro em memória e limita o tamanho de cada mensagem. O valor pode ser ajustado conforme testes de desempenho e limites de mensagem do ambiente.

## 5. Armazenamento e validações

Estrutura física:

```text
equipe08_grpc/
├── proto/
│   └── file_service.proto
├── src/
│   ├── server.py
│   └── client.py
├── storage/       # criado automaticamente
├── downloads/     # criado pelo cliente ao baixar
├── requirements.txt
└── README.md
```

Validações implementadas:
- nome permitido por lista de caracteres; rejeita `..`, `/` e `\\`;
- limite de 100 MiB;
- rejeição de arquivo inexistente no download;
- rejeição de nome duplicado no upload;
- tamanho declarado versus tamanho recebido;
- hash SHA-256;
- gravação temporária e publicação somente após validação.

## 6. Instalação e geração dos stubs

Recomendado: Python 3.10 ou superior e ambiente virtual.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

A partir da raiz do projeto, gere os stubs:

```bash
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=./src \
  --grpc_python_out=./src \
  ./proto/file_service.proto
```

Isso gera:
- `src/file_service_pb2.py`
- `src/file_service_pb2_grpc.py`

## 7. Execução

Terminal 1 — servidor:

```bash
python src/server.py
```

O servidor ficará disponível em `localhost:50051`.

Terminal 2 — cliente:

```bash
# Listar arquivos
python src/client.py list

# Enviar arquivo
python src/client.py upload ./exemplo.pdf

# Baixar arquivo
python src/client.py download exemplo.pdf
```

O diretório `storage/` é criado automaticamente. Os downloads são gravados em `downloads/`.

## 8. Roteiro de testes para demonstração

1. Iniciar o servidor e executar `list` com o armazenamento vazio.
2. Enviar um arquivo pequeno e executar `list` novamente.
3. Baixar o arquivo e comparar o conteúdo com o original (por exemplo, `sha256sum`).
4. Tentar enviar novamente o mesmo nome e verificar a rejeição.
5. Tentar baixar um nome inexistente e verificar o status de erro.
6. Enviar arquivo acima do limite (100 MiB) e verificar a rejeição.
7. Conferir que arquivos temporários não permanecem no armazenamento após uma transferência inválida.

## 9. Limitações e evolução

Esta versão é uma base acadêmica. O servidor usa canal gRPC sem TLS (`insecure_channel`) e não implementa autenticação, autorização, replicação, persistência de índice em banco de dados ou controle de concorrência distribuído entre múltiplas instâncias. Esses itens podem ser discutidos como evoluções futuras, conforme o escopo das próximas entregas.
