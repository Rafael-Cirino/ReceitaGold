# ReceitaGold

Pipeline for transforming Receita Federal data into the Silver and Gold layers.

## English / Inglês

### Overview

This project processes raw data in `bronze/`, normalizes the `silver/` layer in SQLite, and builds the `gold/` layer for downstream usage.

The default configuration uses the `data/` folder inside the `ReceitaGold` project itself:

```text
ReceitaGold/
  data/
    bronze/
    silver/
    gold/
```

If a `data/` folder exists in the parent repository directory, as in the main workspace structure:

```text
/home/cirino/Projects/lead_project/
  data/
  ReceitaGold/
```

you can point the pipeline to that folder by setting the `DATA_DIR` environment variable or by passing `--data-dir` explicitly. This ensures the project reads from the shared root data folder instead of the subproject-local copy.

### Data folder precedence

The application resolves the data directory in this order:

1. Use `DATA_DIR` if it is defined in the environment.
2. Otherwise, use `ReceitaGold/data` as the default.
3. If you want to use the parent repository data folder, set `DATA_DIR=/home/cirino/Projects/lead_project/data` before running commands.

In short: when there is a parent-level `data/` folder and you want to use it, the manual `DATA_DIR` override must take precedence over the project default.

### Requirements

- Python 3.11+
- `pip` or `uv`
- Access to the input files under `bronze/receita`

### Setup

From the project folder:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running with the parent repository data folder

When the shared data folder is at `/home/cirino/Projects/lead_project/data`, run:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
export DATA_DIR=/home/cirino/Projects/lead_project/data
python main.py ingest-silver --data-dir "$DATA_DIR/bronze/receita"
python main.py build-gold
```

This makes the pipeline operate on the same shared workspace dataset without depending on the subproject-local `data` folder.

### Running with the project default data folder

If you want to use the local `ReceitaGold` folder instead:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python main.py ingest-silver
python main.py build-gold
```

### Useful commands

Show CLI help:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python main.py --help
```

Run only the Silver ingestion layer:

```bash
python main.py ingest-silver --help
```

Run the Gold layer build:

```bash
python main.py build-gold --help
```

### Notes

- The default Silver database is in `data/silver/silver.db`.
- The default Gold output is in `data/gold`.
- If the expected `data/` directory is missing, the pipeline will fail with `FileNotFoundError` when looking for the input files.
- To keep the workflow consistent in a workspace with a top-level `data` folder, prefer exporting `DATA_DIR` before executing commands.

---

## Português do Brasil / Brazilian Portuguese

### Visão geral

Este projeto processa os dados brutos em `bronze/`, normaliza a camada `silver/` em SQLite e monta a camada `gold/` para uso posterior.

A configuração padrão usa a pasta `data/` dentro do próprio projeto `ReceitaGold`:

```text
ReceitaGold/
  data/
    bronze/
    silver/
    gold/
```

Se existir uma pasta `data/` no diretório pai do repositório, como no workspace do projeto principal:

```text
/home/cirino/Projects/lead_project/
  data/
  ReceitaGold/
```

você pode apontar o pipeline para essa pasta usando a variável de ambiente `DATA_DIR` ou passando `--data-dir` explicitamente. Isso garante que os dados sejam lidos da pasta compartilhada do projeto principal em vez da cópia local do subprojeto.

### Ordem de precedência da pasta de dados

A lógica da aplicação resolve o diretório de dados nessa ordem:

1. Usa `DATA_DIR` se estiver definido no ambiente.
2. Caso contrário, usa `ReceitaGold/data` como padrão.
3. Se você quiser usar a pasta de dados do repositório pai, defina `DATA_DIR=/home/cirino/Projects/lead_project/data` antes de rodar os comandos.

Em outras palavras: quando houver uma pasta `data/` fora do repo e você quiser usar essa fonte, o override manual do `DATA_DIR` deve prevalecer sobre o padrão do projeto.

### Requisitos

- Python 3.11+
- `pip` ou `uv`
- Acesso aos arquivos de entrada em `bronze/receita`

### Setup

A partir da pasta do projeto:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Rodando com a pasta de dados do repositório pai

Quando a pasta de dados está em `/home/cirino/Projects/lead_project/data`, use:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
export DATA_DIR=/home/cirino/Projects/lead_project/data
python main.py ingest-silver --data-dir "$DATA_DIR/bronze/receita"
python main.py build-gold
```

Esse ajuste faz o pipeline operar sobre a mesma base de dados compartilhada pelo workspace, sem depender da pasta interna do subprojeto.

### Rodando com a pasta padrão do projeto

Se você quiser usar a pasta local do `ReceitaGold`:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python main.py ingest-silver
python main.py build-gold
```

### Comandos úteis

Listar ajuda do CLI:

```bash
cd /home/cirino/Projects/lead_project/ReceitaGold/python
python main.py --help
```

Executar somente a ingestão da camada Silver:

```bash
python main.py ingest-silver --help
```

Executar a build da camada Gold:

```bash
python main.py build-gold --help
```

### Observações

- O banco Silver padrão fica em `data/silver/silver.db`.
- O banco Gold padrão fica em `data/gold`.
- Se o diretório `data/` não existir no local esperado, o pipeline irá falhar com `FileNotFoundError` ao procurar os arquivos de entrada.
- Para manter o fluxo consistente em um workspace com `data` no nível superior, prefira exportar `DATA_DIR` antes de rodar os comandos.
