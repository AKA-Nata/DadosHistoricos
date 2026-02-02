"""
Script de extração mensal (MariaDB/MySQL) + consolidação CSV (merge/dedup)

Variáveis de ambiente esperadas:
- CPJWCS_HOST=...
- CPJWCS_PORT=3306
- CPJWCS_DB=cpjwcs
- CPJWCS_USER=...
- CPJWCS_PASSWORD=...   (obrigatória; sem default)
- CPJWCS_DATA_DIR=...   (pasta onde ficará o CSV e Logs; default: ./data)
Opcional:
- CPJWCS_GRUPOS_TRABALHO=255,186,248,...
- START_DATE=2025-01-01
- EXCLUDE_LAST_DAYS=30
- CHUNK_SIZE=100000
- TENTATIVAS=3
- ESPERA_SEGUNDOS=5
- NOME_ARQUIVO=DadosHistoricosV2.csv
"""

import os
import re
import time
import uuid
import shutil
import logging
import traceback
import threading
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from tempfile import NamedTemporaryFile
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import URL
from pymysql.constants import CLIENT


# ============================================================
# CONFIG / SETTINGS (somente via env)
# ============================================================

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError as e:
        raise ValueError(f"Variável {name} inválida (esperado int): {v!r}") from e

def _env_date(name: str, default: date) -> date:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        # aceita YYYY-MM-DD
        y, m, d = v.split("-")
        return date(int(y), int(m), int(d))
    except Exception as e:
        raise ValueError(f"Variável {name} inválida (esperado YYYY-MM-DD): {v!r}") from e

def _parse_int_tuple(csv: str) -> tuple[int, ...]:
    parts = [p.strip() for p in (csv or "").split(",") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError as e:
            raise ValueError(f"Valor inválido em CPJWCS_GRUPOS_TRABALHO: {p!r} (esperado int)") from e
    return tuple(out)

@dataclass(frozen=True)
class Settings:
    # Extração
    start_date: date
    exclude_last_days: int
    chunk_size: int
    tentativas: int
    espera_segundos: int

    # Paths
    data_dir: Path
    log_dir: Path
    nome_arquivo: str
    caminho_arquivo: Path

    # Dedup
    chaves_unicas: tuple[str, ...]

    # Query
    grupos_trabalho: tuple[int, ...]

    # Watchdog
    watchdog_interval_sec: int
    kill_if_over_sec: int

@dataclass(frozen=True)
class DBSettings:
    host: str
    port: int
    database: str
    user: str
    password: str

def load_settings() -> tuple[Settings, DBSettings]:
    start_date = _env_date("START_DATE", date(2025, 1, 1))
    exclude_last_days = _env_int("EXCLUDE_LAST_DAYS", 30)

    chunk_size = _env_int("CHUNK_SIZE", 100_000)
    tentativas = _env_int("TENTATIVAS", 3)
    espera_segundos = _env_int("ESPERA_SEGUNDOS", 5)

    # Diretório de dados (não hardcode UNC/share)
    data_dir_str = os.getenv("CPJWCS_DATA_DIR", "").strip()
    if data_dir_str:
        data_dir = Path(data_dir_str).expanduser().resolve()
    else:
        data_dir = (Path(__file__).resolve().parent / "data").resolve()

    log_dir = data_dir / "Logs"
    nome_arquivo = os.getenv("NOME_ARQUIVO", "DadosHistoricosV2.csv").strip() or "DadosHistoricosV2.csv"
    caminho_arquivo = data_dir / nome_arquivo

    chaves_unicas = ("id_tramitacao", "id_workflow_instancia")

    grupos_env = os.getenv("CPJWCS_GRUPOS_TRABALHO", "").strip()
    if grupos_env:
        grupos_trabalho = _parse_int_tuple(grupos_env)
    else:
        # Mantém um default vazio para evitar “vazamento” de parâmetros internos;
        # force a configuração via env caso a query dependa disso.
        grupos_trabalho = ()

    watchdog_interval = _env_int("WATCHDOG_INTERVAL_SEC", 30)
    kill_over = _env_int("KILL_IF_OVER_SEC", 180)

    # DB (sem defaults sensíveis)
    host = os.getenv("CPJWCS_HOST", "").strip()
    user = os.getenv("CPJWCS_USER", "").strip()
    password = os.getenv("CPJWCS_PASSWORD", "")  # não .strip() para não alterar senha
    port = _env_int("CPJWCS_PORT", 3306)
    database = os.getenv("CPJWCS_DB", "cpjwcs").strip() or "cpjwcs"

    missing = []
    if not host:
        missing.append("CPJWCS_HOST")
    if not user:
        missing.append("CPJWCS_USER")
    if not password:
        missing.append("CPJWCS_PASSWORD")

    if missing:
        raise RuntimeError(
            "Configuração incompleta. Defina as variáveis de ambiente: "
            + ", ".join(missing)
            + ". Sugestão: use um arquivo .env local (NÃO commitar) e carregue no seu terminal/IDE."
        )

    s = Settings(
        start_date=start_date,
        exclude_last_days=exclude_last_days,
        chunk_size=chunk_size,
        tentativas=tentativas,
        espera_segundos=espera_segundos,
        data_dir=data_dir,
        log_dir=log_dir,
        nome_arquivo=nome_arquivo,
        caminho_arquivo=caminho_arquivo,
        chaves_unicas=chaves_unicas,
        grupos_trabalho=grupos_trabalho,
        watchdog_interval_sec=watchdog_interval,
        kill_if_over_sec=kill_over,
    )

    db = DBSettings(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    return s, db


# ============================================================
# UTIL timestamp console
# ============================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_print(msg: str):
    print(f"[{now_str()}] {msg}")
    logging.info(msg)


# ============================================================
# Sanitização / utilidades CSV
# ============================================================
_ILLEGAL_CTRL_RE = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')
EXCEL_MIN_DATE = pd.Timestamp('1900-01-01')
CSV_DT_FMT = '%Y-%m-%d %H:%M:%S'

def _clean_str(x: str) -> str:
    x = _ILLEGAL_CTRL_RE.sub('', x)
    x = x.replace('\r\n', '\n').replace('\r', '\n')
    return x

def sanitize_df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    obj_cols = df.select_dtypes(include=['object', 'string']).columns
    for c in obj_cols:
        df[c] = df[c].map(lambda v: _clean_str(v) if isinstance(v, str) else v)

    dtz_cols = df.select_dtypes(include=['datetimetz']).columns
    for c in dtz_cols:
        try:
            df[c] = df[c].dt.tz_convert(None) if getattr(df[c].dt, 'tz', None) is not None else df[c].dt.tz_localize(None)
        except Exception:
            try:
                df[c] = df[c].dt.tz_localize(None)
            except Exception:
                pass

    dt_cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for c in dt_cols:
        mask_bad = (df[c].notna()) & (df[c] < EXCEL_MIN_DATE)
        if mask_bad.any():
            df.loc[mask_bad, c] = pd.NaT
        df[c] = df[c].dt.strftime(CSV_DT_FMT)

    for c in df.columns:
        if df[c].dtype == 'object':
            if df[c].map(lambda x: isinstance(x, (list, dict, set, tuple, bytes))).any():
                df[c] = df[c].map(lambda x: _clean_str(str(x)) if pd.notna(x) else x)

    return df


# ============================================================
# Logging
# ============================================================
def setup_logging(log_dir: Path):
    log_dir.mkdir(parents=True, exist_ok=True)
    log_now = datetime.now().strftime('%d%m%Y_%H%M')
    log_filename = f'log_{log_now}.log'
    logging.basicConfig(
        filename=str(log_dir / log_filename),
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )


# ============================================================
# DB Engine
# ============================================================
def get_engine(db: DBSettings):
    # Não logar URL (pode conter user etc.).
    url = URL.create(
        drivername="mysql+pymysql",
        username=db.user,
        password=db.password,
        host=db.host,
        port=db.port,
        database=db.database,
        query={"charset": "utf8mb4"},
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
        connect_args={
            "connect_timeout": 60,
            "read_timeout": 1800,
            "write_timeout": 1800,
            "client_flag": CLIENT.MULTI_STATEMENTS,
        },
    )
    return engine


# ============================================================
# SQL (Query mensal - parametrizada por janela e grupos)
# ============================================================
def build_query_mes(dt_start: date, dt_end: date, grupos_trabalho: tuple[int, ...]) -> str:
    ds = dt_start.strftime('%Y-%m-%d')
    de = dt_end.strftime('%Y-%m-%d')

    if not grupos_trabalho:
        raise RuntimeError(
            "CPJWCS_GRUPOS_TRABALHO não definido. "
            "Defina a variável (ex.: CPJWCS_GRUPOS_TRABALHO=255,186,248,...) "
            "para habilitar o filtro no cad_processo."
        )

    grupos = ",".join(str(x) for x in grupos_trabalho)

    return f"""
SELECT
  c1.grupo_trabalho AS GrupoTrabalho,

  p1.data_hora_lan     AS MenorLancamentoPrincipal,
  p1.ag_data_hora      AS FatalPrincipal,
  p1.prazo_data_inicio AS ContagemPrincipal,
  p1.evento,
  COALESCE(p1.id_workflow_instancia, p1.id_tramitacao) AS ID,
  p1.id_workflow_instancia,
  p1.id_tramitacao,
  p1.id_processo,
  p1.id_tramitacao_situacao,
  p1.id_tramitacao_motivo,

  COALESCE(w.QtdAgendas,0)        AS QtdAgendas,
  w.MenorDataLancamentoSUB,
  w.MenorContagemSUB,
  w.MenorFatalSUB,
  w.MaiorDataLancamentoSUB,
  w.MaiorContagemSUB,
  w.MaiorFatalSUB,

  COALESCE(ls.id_pessoa_atribuida,   p1.id_pessoa_atribuida)   AS IdPessoaAtribuida,
  COALESCE(ls.id_pessoa_solicitante, p1.id_pessoa_solicitante) AS IdPessoaSolicitante,
  COALESCE(ls.id_pessoa_solicitada,  p1.id_pessoa_solicitada)  AS IdPessoaSolicitada,
  COALESCE(ls.cumprido_por,          p1.cumprido_por)          AS CumpridoPor,
  p1.cumprido_em,
  p1.texto,

  COALESCE(tsSUB.descricao, tsPRIN.descricao) AS situacao,
  mt_any.descricao AS justificativa

FROM
(
  SELECT
    t.id_processo,
    t.id_workflow_instancia,

    MIN(CASE
          WHEN t.evento NOT IN ('$CT','$EP','AEDP')
          THEN t.id_tramitacao
        END) AS min_principal_id,

    SUM(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN 1 ELSE 0 END) AS QtdAgendas,

    MIN(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.data_hora_lan END)      AS MenorDataLancamentoSUB,
    MIN(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.prazo_data_inicio END)  AS MenorContagemSUB,
    MIN(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.ag_data_hora END)       AS MenorFatalSUB,

    MAX(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.data_hora_lan END)      AS MaiorDataLancamentoSUB,
    MAX(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.prazo_data_inicio END)  AS MaiorContagemSUB,
    MAX(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.ag_data_hora END)       AS MaiorFatalSUB,

    MAX(CASE WHEN t.evento IN ('$CT','$EP','AEDP') THEN t.id_tramitacao END) AS last_sub_id,

    MAX(CASE
          WHEN t.evento IN ('$CT','$EP','AEDP')
           AND t.id_tramitacao_motivo IS NOT NULL
          THEN t.id_tramitacao
        END) AS last_motivo_id

  FROM tramitacao t FORCE INDEX (tramitacao_k19)
  JOIN cad_processo c
    ON c.pj = t.id_processo
   AND c.grupo_trabalho IN ({grupos})
  WHERE t.data_hora_lan >= DATE '{ds}'
    AND t.data_hora_lan <  DATE '{de}'
    AND t.id_workflow_instancia IS NOT NULL
  GROUP BY t.id_processo, t.id_workflow_instancia
  HAVING min_principal_id IS NOT NULL
  ORDER BY NULL
) w

JOIN tramitacao p1
  ON p1.id_tramitacao = w.min_principal_id

JOIN cad_processo c1
  ON c1.pj = p1.id_processo
 AND c1.grupo_trabalho IN ({grupos})

LEFT JOIN tramitacao ls ON ls.id_tramitacao = w.last_sub_id
LEFT JOIN tramitacao lm ON lm.id_tramitacao = w.last_motivo_id

LEFT JOIN tramitacao_situacao tsPRIN ON tsPRIN.id_tramitacao_situacao = p1.id_tramitacao_situacao
LEFT JOIN tramitacao_situacao tsSUB  ON tsSUB.id_tramitacao_situacao  = ls.id_tramitacao_situacao
LEFT JOIN tramitacao_motivo   mt_any ON mt_any.id_tramitacao_motivo   = lm.id_tramitacao_motivo

UNION ALL

SELECT
  c1.grupo_trabalho AS GrupoTrabalho,

  t1.data_hora_lan     AS MenorLancamentoPrincipal,
  t1.ag_data_hora      AS FatalPrincipal,
  t1.prazo_data_inicio AS ContagemPrincipal,
  t1.evento,
  t1.id_tramitacao     AS ID,
  t1.id_workflow_instancia,
  t1.id_tramitacao,
  t1.id_processo,
  t1.id_tramitacao_situacao,
  t1.id_tramitacao_motivo,

  0    AS QtdAgendas,
  NULL AS MenorDataLancamentoSUB,
  NULL AS MenorContagemSUB,
  NULL AS MenorFatalSUB,
  NULL AS MaiorDataLancamentoSUB,
  NULL AS MaiorContagemSUB,
  NULL AS MaiorFatalSUB,

  t1.id_pessoa_atribuida   AS IdPessoaAtribuida,
  t1.id_pessoa_solicitante AS IdPessoaSolicitante,
  t1.id_pessoa_solicitada  AS IdPessoaSolicitada,
  t1.cumprido_por          AS CumpridoPor,
  t1.cumprido_em,
  t1.texto,

  tsPRIN.descricao AS situacao,
  NULL AS justificativa

FROM tramitacao t1 FORCE INDEX (tramitacao_k19)
JOIN cad_processo c1
  ON c1.pj = t1.id_processo
 AND c1.grupo_trabalho IN ({grupos})
LEFT JOIN tramitacao_situacao tsPRIN
  ON tsPRIN.id_tramitacao_situacao = t1.id_tramitacao_situacao
WHERE t1.data_hora_lan >= DATE '{ds}'
  AND t1.data_hora_lan <  DATE '{de}'
  AND t1.id_workflow_instancia IS NULL
  AND t1.evento NOT IN ('$CT','$EP','AEDP');
""".strip()


# ============================================================
# Janelas mensais
# ============================================================
def first_day_of_month(d: date) -> date:
    return d.replace(day=1)

def add_one_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)

def iter_month_windows(start: date, end_exclusive: date):
    cur = first_day_of_month(start)
    while cur < end_exclusive:
        nxt = add_one_month(cur)
        if nxt <= end_exclusive:
            yield cur, nxt
            cur = nxt
        else:
            yield cur, end_exclusive
            break


# ============================================================
# Watchdog: processlist + kill se > limite (COM STATE + INTERPRETAÇÃO)
# ============================================================
STATE_INTERPRETATION = {
    "Sending data": "Lendo linhas e enviando resultado (scan pesado / join grande / rede).",
    "Creating tmp table": "Criando tabela temporária (GROUP BY / ORDER BY / UNION).",
    "Copying to tmp table": "Copiando dados para tabela temporária (volume alto).",
    "Sorting result": "Ordenando resultado (filesort).",
    "Sorting for group": "Ordenando para GROUP BY.",
    "Executing": "Executando plano de execução (CPU/IO ativo).",
    "statistics": "Otimizador coletando estatísticas.",
    "Waiting for table metadata lock": "Aguardando lock de metadata (DDL concorrente).",
    "Waiting for table level lock": "Aguardando lock de tabela.",
    "Waiting for handler commit": "Esperando commit de outro processo.",
    "Opening tables": "Abrindo tabelas (cache frio ou pressão de FD).",
    "System lock": "Esperando lock interno do engine.",
    "init": "Inicializando execução da query.",
    "end": "Finalizando execução.",
}

def watchdog_processlist(stop_evt: threading.Event, conn_id: int, watchdog_interval: int, kill_over: int, db: DBSettings):
    """
    A cada watchdog_interval:
      - SHOW FULL PROCESSLIST
      - acha conn_id
      - printa state + interpretação
      - se Query e Time > kill_over: KILL QUERY + KILL
    """
    if not conn_id:
        return

    # Engine em information_schema usando a mesma conexão base
    info_db = DBSettings(host=db.host, port=db.port, database="information_schema", user=db.user, password=db.password)
    try:
        info_engine = get_engine(info_db)
    except Exception as e:
        log_print(f"Watchdog: não conseguiu criar engine info_schema: {e}")
        return

    while not stop_evt.is_set():
        time.sleep(watchdog_interval)
        if stop_evt.is_set():
            break

        try:
            with info_engine.connect() as c:
                rows = c.execute(text("SHOW FULL PROCESSLIST")).mappings().all()

                target = None
                for r in rows:
                    rid = r.get("Id", r.get("ID"))
                    if rid is None:
                        continue
                    if int(rid) == int(conn_id):
                        target = r
                        break

                if not target:
                    continue

                cmd = str(target.get("Command", "")).strip()
                tm = int(target.get("Time", 0) or 0)
                state = str(target.get("State", "")).strip()

                interpretacao = STATE_INTERPRETATION.get(
                    state,
                    "Estado não mapeado (pode ser normal ou indicar gargalo específico)."
                )

                log_print(
                    "Watchdog check | "
                    f"conn_id={conn_id} | "
                    f"cmd={cmd} | "
                    f"state='{state or 'N/A'}' | "
                    f"interpretação='{interpretacao}' | "
                    f"time={tm}s limite={kill_over}s"
                )

                if cmd.lower() == "query" and tm > kill_over:
                    log_print(f"Watchdog: conn_id={conn_id} está em Query há {tm}s. Tentando matar...")
                    try:
                        c.execute(text(f"KILL QUERY {int(conn_id)}"))
                        log_print(f"KILL QUERY {conn_id} enviado.")
                    except Exception as e:
                        log_print(f"Falhou KILL QUERY {conn_id}: {e}")

                    try:
                        c.execute(text(f"KILL {int(conn_id)}"))
                        log_print(f"KILL {conn_id} enviado.")
                    except Exception as e:
                        log_print(f"Falhou KILL {conn_id}: {e}")

                    stop_evt.set()

        except Exception as e:
            log_print(f"Watchdog erro: {e}")

    try:
        info_engine.dispose()
    except Exception:
        pass


# ============================================================
# Streaming por mês com retry + watchdog
# ============================================================
def stream_mes(engine, dt_start: date, dt_end: date, s: Settings, db: DBSettings):
    query_sql = build_query_mes(dt_start, dt_end, s.grupos_trabalho)
    ultimo_erro = None

    for tentativa in range(1, s.tentativas + 1):
        stop_evt = threading.Event()

        try:
            with engine.connect() as conn:
                conn_id = int(conn.execute(text("SELECT CONNECTION_ID()")).scalar() or 0)

                log_print(f"Início query mês {dt_start} -> {dt_end} (tentativa {tentativa}/{s.tentativas}) conn_id={conn_id}")

                wd_thread = threading.Thread(
                    target=watchdog_processlist,
                    args=(stop_evt, conn_id, s.watchdog_interval_sec, s.kill_if_over_sec, db),
                    daemon=True
                )
                wd_thread.start()

                t0 = time.perf_counter()

                df_iter = pd.read_sql_query(
                    text(query_sql),
                    conn.execution_options(stream_results=True),
                    chunksize=s.chunk_size
                )
                for chunk in df_iter:
                    if stop_evt.is_set():
                        break
                    yield chunk

                t1 = time.perf_counter()
                stop_evt.set()
                log_print(f"Fim query mês {dt_start} -> {dt_end} | elapsed={(t1 - t0):.3f}s")
                return

        except OperationalError as e:
            stop_evt.set()
            ultimo_erro = e
            log_print(f"[{dt_start}→{dt_end}] Tentativa {tentativa}/{s.tentativas} falhou: {e}")
            if tentativa == s.tentativas:
                raise
            time.sleep(s.espera_segundos)

        finally:
            try:
                stop_evt.set()
            except Exception:
                pass

    if ultimo_erro:
        raise ultimo_erro

def stream_todos_os_meses(engine, s: Settings, db: DBSettings, end_exclusive: date):
    for dt_start, dt_end in iter_month_windows(s.start_date, end_exclusive):
        log_print(f"Janela: {dt_start} -> {dt_end}")
        yield from stream_mes(engine, dt_start, dt_end, s, db)
        log_print(f"Finalizado janela: {dt_start} -> {dt_end}")


# ============================================================
# Escrita: baixar tudo em um CSV NOVO (temporário) consolidado
# ============================================================
def write_csv_atomic_simple_from_chunks(
    chunk_iter,
    dest_path: Path,
    encoding: str = 'utf-8',
    sep: str = ',',
    na_rep: str = '',
) -> int:
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    total_written = 0

    with NamedTemporaryFile(prefix='tmp_dl_', suffix='.csv', delete=False) as tmpf:
        local_tmp_path = Path(tmpf.name)

    try:
        header_written = False
        last_print = time.perf_counter()
        rows_since = 0

        for df in chunk_iter:
            if df is None or df.empty:
                continue

            df = sanitize_df_for_csv(df)

            df.to_csv(
                str(local_tmp_path),
                mode='a',
                index=False,
                header=not header_written,
                sep=sep,
                na_rep=na_rep,
                encoding=encoding,
                lineterminator='\n'
            )
            header_written = True
            total_written += len(df)
            rows_since += len(df)

            if time.perf_counter() - last_print >= 30:
                log_print(f"CSV NOVO (download): +{rows_since:,} (total {total_written:,})")
                last_print = time.perf_counter()
                rows_since = 0

        staging_path = dest_path.parent / f'._staging_dl_{uuid.uuid4().hex}.csv'
        shutil.copyfile(str(local_tmp_path), str(staging_path))
        os.replace(str(staging_path), str(dest_path))

    finally:
        try:
            if local_tmp_path.exists():
                local_tmp_path.unlink()
        except Exception:
            pass

    return total_written


# ============================================================
# Merge + drop_duplicates + replace atômico no ORIGINAL
# (CSV NOVO prevalece: concat([novo, original]) + keep='first')
# ============================================================
def merge_and_dedup_csv_atomic(
    original_csv: Path,
    downloaded_csv: Path,
    key_cols: tuple[str, ...],
    encoding: str = 'utf-8',
    sep: str = ','
) -> tuple[int, int, int]:
    frames = []
    before_total = 0

    log_print("Lendo CSV NOVO para mesclagem (ele deve prevalecer nas duplicatas)...")
    df_novo = pd.read_csv(str(downloaded_csv), encoding=encoding, sep=sep, dtype=str, low_memory=False)
    frames.append(df_novo)
    before_total += len(df_novo)

    if original_csv.exists():
        log_print("Lendo CSV ORIGINAL para mesclagem...")
        df_orig = pd.read_csv(str(original_csv), encoding=encoding, sep=sep, dtype=str, low_memory=False)
        frames.append(df_orig)
        before_total += len(df_orig)
    else:
        log_print("CSV ORIGINAL não existe ainda (primeira carga).")

    log_print("Concatenando e aplicando drop_duplicates (mantendo CSV NOVO)...")
    df_all = pd.concat(frames, ignore_index=True)

    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=list(key_cols), keep='first')
    after = len(df_all)
    removed = before - after

    original_csv.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(prefix='tmp_merge_', suffix='.csv', delete=False) as tmpf:
        tmp_path = Path(tmpf.name)

    try:
        df_all.to_csv(str(tmp_path), index=False, encoding=encoding, sep=sep, lineterminator='\n')

        staging_path = original_csv.parent / f'._staging_merge_{uuid.uuid4().hex}.csv'
        shutil.copyfile(str(tmp_path), str(staging_path))
        os.replace(str(staging_path), str(original_csv))
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

    return before_total, after, removed


# ============================================================
# MAIN
# ============================================================
def gerar_csv_mensal():
    s, db = load_settings()
    setup_logging(s.log_dir)

    t_all0 = time.perf_counter()
    try:
        log_print("Início da execução")

        hoje = date.today()
        end_limit = hoje - timedelta(days=s.exclude_last_days) if s.exclude_last_days > 0 else hoje

        if end_limit <= s.start_date:
            raise RuntimeError(f"END_LIMIT ({end_limit}) <= START_DATE ({s.start_date}). Ajuste EXCLUDE_LAST_DAYS/START_DATE.")

        log_print(f"Baixando do banco de {s.start_date} até {end_limit} (exclusivo), por meses...")
        log_print(f"Diretório de dados: {s.data_dir}")
        log_print(f"CSV destino: {s.caminho_arquivo.name}")

        engine = get_engine(db)

        # 1) baixa tudo (por janelas mensais) -> CSV NOVO consolidado
        chunk_iter = stream_todos_os_meses(engine, s, db, end_limit)

        download_path = s.data_dir / f"._CSV_NOVO_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        log_print(f"Gerando CSV NOVO temporário: {download_path.name}")

        total_download = write_csv_atomic_simple_from_chunks(
            chunk_iter=chunk_iter,
            dest_path=download_path,
            encoding='utf-8',
            sep=',',
            na_rep=''
        )
        log_print(f"CSV NOVO gerado OK | linhas={total_download:,}")

        # 2) mescla CSV NOVO + CSV ORIGINAL, 3) dedup mantendo CSV NOVO, 4) replace original
        before_total, after_total, removed = merge_and_dedup_csv_atomic(
            original_csv=s.caminho_arquivo,
            downloaded_csv=download_path,
            key_cols=s.chaves_unicas,
            encoding='utf-8',
            sep=','
        )

        log_print(f"Mesclagem+Dedup OK | linhas_lidas_total={before_total:,} final={after_total:,} removidas={removed:,}")
        log_print(f"CSV final atualizado: {s.caminho_arquivo}")

        # 5) remove CSV NOVO temporário
        try:
            download_path.unlink()
            log_print("CSV NOVO temporário removido.")
        except Exception as e:
            log_print(f"Não conseguiu remover CSV NOVO temporário: {e}")

        t_all1 = time.perf_counter()
        log_print(f"Fim | tempo_total={(t_all1 - t_all0):.3f}s")

        try:
            engine.dispose()
        except Exception:
            pass

    except Exception:
        log_print("Erro durante a execução:")
        print(traceback.format_exc())
        logging.error("Erro durante a execução:")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    gerar_csv_mensal()
