# Equipe 08 — Transferência e gerenciamento de arquivos com gRPC

Projeto da disciplina **CIN0143 — Introdução aos Sistemas Distribuídos e Redes de Computadores** (CIn/UFPE, 2026.2).

Sistema distribuído cliente-servidor para armazenar, listar, enviar e baixar arquivos usando **gRPC** com **Protocol Buffers**. O upload usa *client-side streaming*, o download usa *server-side streaming*, e cada transferência é fragmentada em chunks e verificada por SHA-256 nas duas pontas.

> **Estado atual: Entrega 3 (Interface Gráfica).** Servidor concorrente, cliente de terminal, interface gráfica em Tkinter com barra de progresso em tempo real, logs estruturados e testes de concorrência.

## Sumário

1. [Decisão tecnológica](#1-decisão-tecnológica)
2. [Arquitetura](#2-arquitetura)
3. [Contrato Protocol Buffers](#3-contrato-protocol-buffers)
4. [Fragmentação e integridade](#4-fragmentação-e-integridade)
5. [Estado central e armazenamento](#5-estado-central-e-armazenamento)
6. [Validações e códigos de erro](#6-validações-e-códigos-de-erro)
7. [Concorrência](#7-concorrência)
8. [Logs](#8-logs)
9. [Estrutura de pastas](#9-estrutura-de-pastas)
10. [Instalação](#10-instalação)
11. [Execução](#11-execução)
12. [Testes](#12-testes)
13. [Roadmap](#13-roadmap)

---

## 1. Decisão tecnológica

| Escolha | Justificativa |
|---|---|
| **gRPC** (tecnologia sugerida) | Os três requisitos do tema são modos nativos do gRPC: chamada unária (listagem), *client streaming* (upload) e *server streaming* (download). O fluxo de chunks fica declarado no próprio contrato, sem protocolo de framing escrito à mão. |
| **Protocol Buffers** | Serialização binária compacta. O campo `bytes` carrega o conteúdo cru do arquivo; em JSON seria preciso Base64, cerca de 33% a mais por chunk. O `.proto` é um contrato tipado do qual se geram os stubs de cliente e servidor. |
| **HTTP/2** (transporte do gRPC) | Multiplexação: várias transferências simultâneas trafegam como *streams* independentes numa única conexão TCP. Também traz controle de fluxo (*backpressure*), *deadlines* e cancelamento propagados entre cliente e servidor. |
| **Python 3.10+** com `grpcio` e `tkinter` | Implementação oficial do gRPC, geração de stubs com `grpcio-tools`, e bibliotecas padrão suficientes para o domínio (`hashlib`, `pathlib`, `tempfile`, `threading`, `logging`) e interface gráfica (`tkinter`/`ttk`), permitindo importar os stubs diretamente sem proxy intermediário. |

**Alternativas consideradas:** REST com `multipart/form-data` não tem contrato de streaming tipado e exigiria Range/Chunked feitos à mão. WebSockets não têm contrato nem códigos de status padronizados. Frontend web SPA exigiria configurar um proxy Envoy ou servidor intermediário para traduzir HTTP/REST para gRPC.

## 2. Arquitetura

```text
                      gRPC sobre HTTP/2 (TCP :50051)
+--------------------+                                +------------------------------+
| Cliente (CLI / GUI)|  ListFiles   (unária)  ----->  | Servidor gRPC                |
|  src/client.py     |  <-----  ListFilesResponse     |  src/server.py               |
|  src/gui.py        |                                |                              |
|  - calcula SHA-256 |  Upload  (client streaming)    |  FileService                 |
|  - fragmenta 64KiB |  metadata, chunk, chunk ... -> |   - valida nome/tamanho/hash |
|  - progresso/chunk |  <-----  TransferStatus        |   - reserva nome             |
|  - verifica hash   |                                |   - grava .tmp + fsync       |
|    no download     |  Download (server streaming)   |   - publica com os.replace   |
|                    |  <- metadata, chunk ..., status|   - logs estruturados        |
+--------------------+                                +--------------+---------------+
                                                                     |
  N clientes simultâneos (processos, máquinas      ThreadPoolExecutor (--workers)
  ou streams num mesmo canal HTTP/2)                  + RLock no índice
                                                                     |
                                                      +--------------v---------------+
                                                      | Índice em memória (dict)     |
                                                      |  nome -> tamanho, data, hash |
                                                      +--------------+---------------+
                                                                     |
                                                      +--------------v---------------+
                                                      | storage/  (disco, persistente)|
                                                      +------------------------------+
```

**Responsabilidades**

- **Servidor (`src/server.py`):** expõe o serviço `FileService` e é o único dono da pasta `storage/`. Valida as requisições, mantém o índice em memória, recebe e envia chunks e registra logs de cada chamada.
- **Interface Gráfica (`src/gui.py`):** aplicação desktop em Tkinter que exibe o explorador de arquivos do servidor, executa chamadas gRPC em *threads* separadas para não bloquear a interface, atualiza barras de progresso por chunk e apresenta mensagens visuais de erro.
- **Cliente de Terminal (`src/client.py`):** CLI com os comandos `list`, `upload` e `download`. Calcula o SHA-256 antes do envio e confere o hash e o tamanho no download antes de gravar o arquivo final.
- **Logs (`src/log_config.py`):** formato de log `chave=valor` comum a servidor, cliente e testes.
- **Contrato (`proto/file_service.proto`):** fonte única de verdade das mensagens e RPCs. Os stubs `file_service_pb2*.py` são gerados a partir dele.

## 3. Contrato Protocol Buffers

Arquivo completo: [`proto/file_service.proto`](proto/file_service.proto).

```protobuf
service FileService {
  rpc ListFiles(ListFilesRequest) returns (ListFilesResponse);
  rpc Upload(stream UploadRequest) returns (TransferStatus);
  rpc Download(DownloadRequest) returns (stream DownloadResponse);
}
```

| RPC | Tipo | Requisição | Resposta |
|---|---|---|---|
| `ListFiles` | Unária | `ListFilesRequest {}` | `ListFilesResponse { repeated FileInfo files }`, com `FileInfo = {name, size_bytes, modified_at (Timestamp)}` |
| `Upload` | Client streaming | stream de `UploadRequest` (`oneof`: `metadata` ou `chunk`) | `TransferStatus {success, message, bytes_transferred, sha256}` |
| `Download` | Server streaming | `DownloadRequest {filename}` | stream de `DownloadResponse` (`oneof`: `metadata`, `chunk` ou `status`) |

**Sequência do upload**

1. `UploadMetadata {filename, expected_size, sha256}`: obrigatoriamente a primeira mensagem.
2. Zero ou mais `FileChunk {data}`, cada um com até 64 KiB.
3. O cliente fecha o stream; o servidor valida e responde um `TransferStatus` com o SHA-256 calculado por ele.

**Sequência do download**

1. `DownloadMetadata {filename, size_bytes, sha256}`.
2. Zero ou mais `DownloadChunk {data}`.
3. `TransferStatus` final, com os bytes enviados e o hash.

Erros não vêm dentro dessas mensagens: o servidor encerra a chamada com um **código de status gRPC** (seção 6). O campo `status` removido de `ListFilesResponse` está marcado como `reserved` para que o número 2 nunca seja reutilizado com outro significado.

## 4. Fragmentação e integridade

| Item | Decisão |
|---|---|
| Tamanho do chunk | **64 KiB** (65.536 bytes), bem abaixo do limite padrão de 4 MiB por mensagem gRPC e com pouca memória por transferência |
| Metadados | Primeira mensagem do upload e primeira resposta do download (`oneof`) |
| Tamanho máximo | 100 MiB por arquivo (configurável com `--max-file-mb`) |
| Integridade | SHA-256 do conteúdo integral, **obrigatório** no upload (64 caracteres hexadecimais) |
| Verificação no upload | O servidor compara os bytes recebidos com `expected_size` e o hash calculado com o declarado. O cliente ainda confere o hash devolvido no `TransferStatus`. |
| Verificação no download | O cliente compara os bytes recebidos com `size_bytes` e o hash calculado com o dos metadados. Só então renomeia o `.part` para o nome final. |
| Publicação atômica | O upload é gravado em `storage/.upload-XXXX.tmp`, recebe `fsync` e só depois de validado é movido com `os.replace` (operação atômica) para o nome definitivo. Em qualquer falha ou cancelamento o temporário é apagado. Temporários órfãos de uma queda do servidor são removidos na inicialização. |

A ordem dos chunks é garantida pelo próprio gRPC: dentro de um stream HTTP/2 as mensagens chegam na ordem em que foram enviadas, por isso não há número de sequência no `FileChunk`.

## 5. Estado central e armazenamento

- **Persistência:** a pasta `storage/` (configurável com `--storage`) guarda os arquivos com o nome definitivo, em uma estrutura plana.
- **Índice em memória:** `dict[str, FileEntry]` com `size`, `mtime` e `sha256` de cada arquivo. É reconstruído a partir do disco quando o servidor sobe e atualizado a cada upload concluído.
  - `ListFiles` responde só a partir do índice.
  - `Download` usa o hash guardado, sem reler o arquivo para calculá-lo. Para arquivos que já existiam no disco, o hash é calculado no primeiro download e guardado.
- **Reservas:** um `set` com os nomes que estão sendo enviados naquele momento. Um segundo upload com o mesmo nome é rejeitado na primeira mensagem, sem transferir nenhum byte.
- **Sincronização:** um `threading.RLock` protege o índice e as reservas. Operações lentas (calcular hash, gravar em disco) são feitas **fora** do lock para não bloquear os outros clientes.
- O servidor é o único dono de `storage/`. Arquivos copiados manualmente para a pasta com o servidor rodando só aparecem depois de reiniciar.

## 6. Validações e códigos de erro

| Situação | Código gRPC |
|---|---|
| Nome inválido: fora de `[A-Za-z0-9._ -]`, começando com `.`, contendo `/`, `\` ou `..` (path traversal), ou com mais de 200 caracteres | `INVALID_ARGUMENT` |
| SHA-256 ausente ou malformado | `INVALID_ARGUMENT` |
| Primeira mensagem não é metadado, stream vazio, ou mais bytes que o declarado | `INVALID_ARGUMENT` |
| Arquivo inexistente no download | `NOT_FOUND` |
| Nome já existe ou já está sendo enviado por outro cliente | `ALREADY_EXISTS` |
| Arquivo acima do limite, ou limite de chamadas simultâneas atingido | `RESOURCE_EXHAUSTED` |
| Faltam bytes ou o SHA-256 não confere | `DATA_LOSS` |
| Prazo (`--timeout`) excedido | `DEADLINE_EXCEEDED` |
| Servidor fora do ar | `UNAVAILABLE` |
| Falha inesperada de disco | `INTERNAL` (o log do servidor guarda o traceback) |

No cliente de terminal, erros exibem mensagem curta (`Erro [NOT_FOUND]: Arquivo inexistente.`) com código de saída `0` (sucesso), `1` (erro), `2` (uso incorreto) ou `130` (Ctrl+C). Na interface gráfica, os erros são exibidos na barra de status inferior e nomes enviados passam por sanitização prévia para compatibilidade com a expressão regular do servidor.

## 7. Concorrência

- **Modelo de execução no servidor:** usa um `ThreadPoolExecutor` com `--workers` threads (padrão 10). Cada RPC de streaming ocupa uma thread do início ao fim da transferência, então até `--workers` transferências rodam em paralelo.
- **Limite de chamadas:** `--max-concurrent` (padrão: igual a `--workers`) limita quantas chamadas o servidor aceita no total.
  - Chamadas além do limite recebem `RESOURCE_EXHAUSTED` imediatamente, em vez de ficarem esperando sem aviso.
  - Com `--max-concurrent` maior que `--workers`, as excedentes esperam numa fila por uma thread livre.
- **Disputa pelo mesmo nome:** é resolvida pela reserva do nome (seção 5). Exatamente um cliente vence; downloads paralelos de um mesmo arquivo permanecem liberados.
- **Concorrência na interface gráfica:** as operações de rede em `src/gui.py` rodam em *threads* secundárias (`threading.Thread`) e atualizam componentes visuais via `root.after(0, ...)`, mantendo a interface responsiva durante transferências.
- **Cancelamento e deadline:** se o cliente desiste ou o prazo estoura, o servidor detecta, registra `upload.cancelled` ou `download.cancelled`, apaga o temporário e libera o nome reservado.
- **Porta exclusiva:** o servidor desativa `SO_REUSEPORT`. Sem isso, o gRPC permitiria que um segundo processo escutasse na mesma porta, e cada um teria o próprio índice, deixando os dados inconsistentes. Agora o segundo processo falha com "A porta já está em uso?".
- **Encerramento gracioso:** Ctrl+C ou `SIGTERM` param de aceitar novas chamadas e dão 5 s para as transferências em andamento terminarem.

## 8. Logs

Servidor, cliente e testes escrevem no console em um formato `chave=valor` fácil de ler e de filtrar com `grep`:

```text
2026-11-22 20:57:48.774 INFO    [worker_0] server event=upload.start transfer=834c6d2b peer=ipv4:127.0.0.1:35374 filename=big4.bin expected_size=60000000
2026-11-22 20:57:48.801 INFO    [worker_1] server event=upload.start transfer=5465525e peer=ipv4:127.0.0.1:35378 filename=big1.bin expected_size=60000000
2026-11-22 20:57:49.590 INFO    [worker_0] server event=upload.done transfer=834c6d2b peer=ipv4:127.0.0.1:35374 filename=big4.bin bytes=60000000 chunks=916 sha256=bec43e828b38220c duration_ms=816.13 mib_s=70.11
2026-11-22 21:03:12.076 WARNING [worker_0] server event=download.rejected transfer=2e8b91c4 peer=ipv4:127.0.0.1:46618 filename=nada.bin code=NOT_FOUND reason="Arquivo inexistente."
2026-11-22 20:57:49.858 WARNING [worker_0] server event=download.cancelled transfer=bfdf675f peer=ipv4:127.0.0.1:35408 filename=big1.bin bytes=262144 total=60000000 duration_ms=43.62
```

- `[worker_N]`: thread do pool que atende a chamada; threads diferentes mostram a concorrência.
- `transfer`: identificador curto que liga todas as linhas da mesma transferência.
- `peer`: endereço do cliente.

**Eventos do servidor**

| Grupo | Eventos |
|---|---|
| Ciclo de vida | `server.started`, `server.stopping`, `server.stopped`, `server.bind_failed`, `index.rebuilt`, `storage.orphans_removed` |
| Listagem | `list.start`, `list.done` |
| Upload | `upload.open`, `upload.start`, `upload.progress` (25/50/75%), `upload.done`, `upload.rejected`, `upload.cancelled`, `upload.failed`, `upload.tempfile_removed` |
| Download | `download.start`, `download.progress`, `download.done`, `download.rejected`, `download.cancelled`, `download.failed` |

O nível de detalhe é ajustado com `--log-level` (`DEBUG`, `INFO`, `WARNING`, `ERROR`).

## 9. Estrutura de pastas

```text
gRPC-protocol-buffers/
├── proto/
│   └── file_service.proto        # contrato gRPC
├── src/
│   ├── server.py                 # servidor gRPC (FileService)
│   ├── client.py                 # cliente de terminal (list/upload/download)
│   ├── gui.py                    # interface gráfica desktop (Tkinter)
│   ├── log_config.py             # formato de log compartilhado
│   ├── file_service_pb2.py       # gerado pelo protoc (não versionado)
│   └── file_service_pb2_grpc.py  # gerado pelo protoc (não versionado)
├── tests/
│   └── concurrency_test.py       # N clientes simultâneos + verificação de integridade
├── storage/                      # criado pelo servidor (não versionado)
├── downloads/                    # criado pelo cliente CLI (não versionado)
├── requirements.txt
└── README.md
```

## 10. Instalação

Requisitos: **Python 3.10 ou superior** (confira com `python3 --version`) e **Tkinter** para a interface gráfica.

Em sistemas Debian/Ubuntu/Pop!_OS, instale o pacote gráfico do Python caso ainda não esteja presente:

```bash
sudo apt update && sudo apt install -y python3-tk
```

Clone o repositório e instale as dependências Python:

```bash
git clone https://github.com/charlysfarias/gRPC-protocol-buffers.git
cd gRPC-protocol-buffers

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No Windows, ative o ambiente com `.venv\Scripts\activate`.

Gere os stubs a partir do contrato. É preciso gerar de novo **sempre que o `.proto` mudar**:

```bash
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=./src \
  --grpc_python_out=./src \
  ./proto/file_service.proto
```

Isso cria `src/file_service_pb2.py` e `src/file_service_pb2_grpc.py`.

## 11. Execução

Todos os comandos são executados a partir da raiz do projeto, com o ambiente virtual ativado.

### Servidor

```bash
python src/server.py
```

| Opção | Padrão | Variável de ambiente | Descrição |
|---|---|---|---|
| `--host` | `0.0.0.0` | `FILESERVER_HOST` | Interface de escuta |
| `-p`, `--port` | `50051` | `FILESERVER_PORT` | Porta TCP |
| `-w`, `--workers` | `10` | `FILESERVER_WORKERS` | Threads que atendem chamadas simultâneas |
| `--max-concurrent` | = `--workers` | — | Máximo de chamadas aceitas; excedentes recebem `RESOURCE_EXHAUSTED` |
| `--max-file-mb` | `100` | `FILESERVER_MAX_FILE_MB` | Tamanho máximo por arquivo (MiB) |
| `--storage` | `./storage` | `FILESERVER_STORAGE` | Pasta de armazenamento |
| `--log-level` | `INFO` | — | `DEBUG`, `INFO`, `WARNING` ou `ERROR` |

Exemplo: `python src/server.py -p 6000 -w 20 --max-concurrent 50 --storage /tmp/arquivos`.

Para encerrar: **Ctrl+C**.

### Interface Gráfica (GUI)

Com o servidor em execução, abra a interface gráfica em outro terminal:

```bash
python src/gui.py
```

Na janela:
- **Servidor / Atualizar Lista:** conecta ao endereço informado (padrão `localhost:50051`) e preenche a tabela com nome, tamanho e data de modificação dos arquivos.
- **Enviar Arquivo (Upload):** abre o seletor de arquivos local, calcula o SHA-256, envia em chunks de 64 KiB via *client-side streaming* atualizando a barra de progresso e atualiza a listagem ao concluir.
- **Baixar Selecionado (Download):** baixa o arquivo selecionado na tabela via *server-side streaming* atualizando a barra de progresso, verifica o SHA-256 e salva no destino escolhido pelo usuário.

### Cliente de Terminal (CLI)

As opções globais vêm **antes** do comando:

```bash
python src/client.py [-s HOST:PORTA] [-t SEGUNDOS] [--log-level NIVEL] COMANDO ...
```

| Comando | Exemplo | Descrição |
|---|---|---|
| `list` | `python src/client.py list` | Lista nome, tamanho e data dos arquivos |
| `upload ARQUIVO [--as NOME]` | `python src/client.py upload ./relatorio.pdf` | Envia por client streaming; `--as` define outro nome no servidor |
| `download NOME [-o PASTA]` | `python src/client.py download relatorio.pdf -o ~/Desktop` | Baixa por server streaming (padrão: `./downloads`) |

| Opção global | Padrão | Descrição |
|---|---|---|
| `-s`, `--server` | `localhost:50051` (env `FILESERVER_ADDRESS`) | Endereço do servidor |
| `-t`, `--timeout` | sem limite | Deadline da chamada em segundos |
| `--log-level` | `INFO` | Use `WARNING` para ver só o resultado |

### Em duas máquinas

Na máquina do servidor, descubra o IP (`ipconfig getifaddr en0` no macOS, `hostname -I` no Linux) e rode `python src/server.py`. Na outra máquina, na mesma rede, informe `192.168.0.10:50051` no campo de servidor da interface gráfica ou rode:

```bash
python src/client.py -s 192.168.0.10:50051 list
```

O firewall precisa permitir a porta 50051.

## 12. Testes

### 12.1 Demonstração pela Interface Gráfica (Clientes Simultâneos)

1. Inicie o servidor em um terminal (`python src/server.py`).
2. Abra duas instâncias da interface gráfica em terminais separados (`python src/gui.py`).
3. **Upload e atualização:** envie um arquivo pela primeira janela e clique em **Atualizar Lista** na segunda janela para visualizá-lo.
4. **Downloads paralelos:** selecione o mesmo arquivo nas duas janelas e clique em **Baixar Selecionado (Download)** simultaneamente; ambas as transferências ocorrem em paralelo sem bloqueio.
5. **Colisão de escrita:** tente enviar dois arquivos com o mesmo nome simultaneamente nas duas janelas (ou reenvie um arquivo já existente); o servidor rejeita a colisão com `ALREADY_EXISTS` e a interface exibe a mensagem de erro.

### 12.2 Roteiro manual (CLI)

Com o servidor rodando em um terminal, execute no outro:

```bash
head -c 5000000 /dev/urandom > exemplo.bin

python src/client.py list
python src/client.py upload exemplo.bin
python src/client.py list
python src/client.py download exemplo.bin

shasum -a 256 exemplo.bin downloads/exemplo.bin

python src/client.py upload exemplo.bin
python src/client.py download nao-existe.bin
python src/client.py download ../etc/passwd
python src/client.py -s localhost:59999 list
```

| Passo | Resultado esperado |
|---|---|
| 1ª listagem | `Nenhum arquivo disponível.` |
| Upload | `Enviado: exemplo.bin (5000000 bytes, SHA-256 ...)` |
| 2ª listagem | Tabela com o arquivo |
| Download | `Baixado: downloads/exemplo.bin (5000000 bytes, SHA-256 verificado)` |
| `shasum` (no Linux, `sha256sum`) | Os dois hashes são idênticos |
| Upload repetido | `Erro [ALREADY_EXISTS]: Já existe um arquivo com esse nome.` |
| Arquivo inexistente | `Erro [NOT_FOUND]: Arquivo inexistente.` |
| Path traversal | `Erro: Nome de arquivo inválido: informe apenas o nome, sem diretórios.` |
| Porta errada | `Erro [UNAVAILABLE]: Servidor indisponível em localhost:59999. ...` |

**Limite de tamanho:** suba o servidor com `--max-file-mb 1` e envie um arquivo maior. O resultado esperado é `Erro [RESOURCE_EXHAUSTED]: Arquivo excede o tamanho máximo de 1048576 bytes.`

**Cancelamento:** pressione Ctrl+C no meio do upload de um arquivo grande. O servidor registra `upload.cancelled` e não deixa `.tmp` em `storage/`.

### 12.3 Teste automatizado de concorrência

`tests/concurrency_test.py` dispara N clientes **ao mesmo tempo** (sincronizados por uma `threading.Barrier`). Cada um envia um arquivo aleatório, baixa de volta e confere o SHA-256. Ao final, o script mostra tempos (mínimo, média, p95, máximo), vazão agregada e **APROVADO** ou **REPROVADO** (código de saída `0` ou `1`).

```bash
python tests/concurrency_test.py -n 10 --size-mb 10
python tests/concurrency_test.py -n 8 --shared-channel
python tests/concurrency_test.py -n 8 --same-name
```

- **Primeiro comando:** 10 clientes, cada um com a própria conexão.
- **`--shared-channel`:** todos usam uma única conexão HTTP/2, mostrando a multiplexação.
- **`--same-name`:** todos disputam o mesmo nome; exatamente 1 deve vencer e os demais recebem `ALREADY_EXISTS`.

| Opção | Padrão | Descrição |
|---|---|---|
| `-s`, `--server` | `localhost:50051` | Endereço do servidor |
| `-n`, `--clients` | `10` | Clientes simultâneos |
| `--size-mb` | `5` | Tamanho do arquivo de cada cliente |
| `-t`, `--timeout` | `120` | Deadline por chamada (s) |
| `--shared-channel` | desligado | Um único canal para todos |
| `--same-name` | desligado | Testa a disputa pelo mesmo nome |
| `--skip-download` | desligado | Só uploads |

Exemplo de saída (10 clientes × 10 MiB):

```text
==============================================================================
Resumo: 10 clientes, 10.0 MiB cada, canal próprio por cliente, modo nomes distintos
==============================================================================
OPERAÇÃO     OK  FALHA   MIN (s)  MÉDIA (s)   P95 (s)   MÁX (s)
upload       10      0     0.453      0.530     0.542     0.542
download     10      0     0.036      0.275     0.310     0.310
Tempo total: 0.852 s | Vazão agregada: 234.73 MiB/s

Verificação de integridade: APROVADO
```

Com mais clientes que `--max-concurrent`, os excedentes recebem `RESOURCE_EXHAUSTED` e o teste reprova, mostrando o limite em ação. Para que todos sejam atendidos via fila, suba o servidor com, por exemplo, `--max-concurrent 50`.

Os arquivos de teste ficam em `storage/` com o prefixo `loadtest-`. Para limpar, pare o servidor e apague a pasta `storage/`.

## 13. Roadmap

| Entrega | Escopo | Status |
|---|---|---|
| 1 — Arquitetura e escopo | Tecnologia justificada, `.proto`, chunking e integridade, estado central, validações, servidor base em porta fixa | ✅ Concluída |
| 2 — Comunicação e core | Listagem, upload e download funcionais, cliente de terminal, concorrência, logs estruturados, códigos de status gRPC, configuração por CLI, teste de concorrência | ✅ Concluída |
| 3 — Interface gráfica | Explorador de arquivos, upload e download com progresso em tempo real, travas contra colisão de escrita e tratamento visual de erros | ✅ Concluída |
| 4 — Apresentação | Demonstração final | ⏳ 01 e 03/12/2026 |

**Limitações conhecidas:**

- Canal sem TLS (`insecure`).
- Sem autenticação nem autorização.
- Sem remoção de arquivos nem retomada de transferências interrompidas.
- Uma única instância do servidor, sem replicação.