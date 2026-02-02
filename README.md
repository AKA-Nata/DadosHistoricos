### Script de extração e consolidação mensal.

O que ele faz:
- Consulta o banco por **janelas mensais** entre `START_DATE` e a data atual menos `EXCLUDE_LAST_DAYS`.
- Faz **streaming** do resultado em **chunks** (`CHUNK_SIZE`) para evitar alto consumo de memória.
- Gera um **CSV temporário** (download) com os dados extraídos.
- Mescla o CSV temporário com o **CSV histórico existente** e aplica **deduplicação**.
- Dedup é feita por `id_tramitacao` + `id_workflow_instancia`, mantendo prioridade para o conteúdo recém-baixado (CSV novo prevalece).
- Atualiza o CSV final de forma **atômica** (escreve em staging e depois `os.replace`).
- Possui **watchdog** opcional que monitora `SHOW FULL PROCESSLIST` e tenta interromper queries que excedam o limite configurado.


## Requisitos

- Python 3.10+ (recomendado)
- Dependências:
  - `pandas`
  - `SQLAlchemy`
  - `PyMySQL`

Exemplo (pip):
```bash
pip install pandas sqlalchemy pymysql
```

## Configuração

Todas as configurações são feitas por variáveis de ambiente.

### Variáveis obrigatórias (conexão)
- `CPJWCS_HOST`
- `CPJWCS_USER`
- `CPJWCS_PASSWORD`

### Variáveis opcionais (com defaults)
- `CPJWCS_PORT` (default: `3306`)
- `CPJWCS_DB` (default: `cpjwcs`)
- `CPJWCS_DATA_DIR` (default: `./data`)
- `CPJWCS_GRUPOS_TRABALHO` (sem default; deve ser definido para habilitar o filtro na query)
- `START_DATE` (default: `2025-01-01`)
- `EXCLUDE_LAST_DAYS` (default: `30`)
- `CHUNK_SIZE` (default: `100000`)
- `TENTATIVAS` (default: `3`)
- `ESPERA_SEGUNDOS` (default: `5`)
- `NOME_ARQUIVO` (default: `DadosHistoricosV2.csv`)
- `WATCHDOG_INTERVAL_SEC` (default: `30`)
- `KILL_IF_OVER_SEC` (default: `180`)

### Exemplo de `.env.example`
Crie um arquivo `.env.example` com chaves vazias (não coloque valores reais):
```env
CPJWCS_HOST=
CPJWCS_PORT=3306
CPJWCS_DB=cpjwcs
CPJWCS_USER=
CPJWCS_PASSWORD=

CPJWCS_DATA_DIR=./data
CPJWCS_GRUPOS_TRABALHO=255,186,248,251

START_DATE=2025-01-01
EXCLUDE_LAST_DAYS=30
CHUNK_SIZE=100000
TENTATIVAS=3
ESPERA_SEGUNDOS=5
NOME_ARQUIVO=DadosHistoricosV2.csv

WATCHDOG_INTERVAL_SEC=30
KILL_IF_OVER_SEC=180
```

## Como executar

1) Defina as variáveis de ambiente (ex.: via `.env` local carregado pela sua IDE/terminal)  
2) Execute:

```bash
python arquivo.py
```

## Saídas geradas

Por padrão, o script grava tudo em `./data` (ou em `CPJWCS_DATA_DIR`):

- `DadosHistoricosV2.csv` (ou `NOME_ARQUIVO`): CSV consolidado final
- `Logs/log_DDMMYYYY_HHMM.log`: logs de execução
- Arquivos temporários (criados e removidos automaticamente):
  - `._CSV_NOVO_*.csv`
  - `._staging_*.csv`

## Observações importantes

- O filtro `CPJWCS_GRUPOS_TRABALHO` é exigido para evitar extrações sem restrição no `cad_processo`.
- Caso a consulta demore mais que `KILL_IF_OVER_SEC` em estado `Command=Query`, o watchdog tenta:
  - `KILL QUERY <conn_id>` e depois `KILL <conn_id>`.
- O script é orientado a **reduzir risco operacional** (streaming, retry, escrita atômica), mas não substitui monitoramento do banco e boas práticas de capacidade (índices, janelas, etc.).

## Troubleshooting

- Erro: “Configuração incompleta…”  
  Verifique `CPJWCS_HOST`, `CPJWCS_USER` e `CPJWCS_PASSWORD`.

- Erro: “CPJWCS_GRUPOS_TRABALHO não definido…”  
  Defina a variável com os IDs separados por vírgula.

- CSV final não atualiza  
  Verifique permissões de escrita em `CPJWCS_DATA_DIR` e se algum processo mantém o CSV aberto.

