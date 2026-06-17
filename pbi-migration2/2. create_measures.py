# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Crear measures en las Metric Views
# MAGIC
# MAGIC Lee las Metric Views base del notebook 1 (que ya incluyen vistas LOD pre-calculadas) y para cada una:
# MAGIC 1. Pide a Claude que traduzca cada DAX a SQL (JSON)
# MAGIC 2. Inserta measures una a una al YAML y valida
# MAGIC 3. Segunda pasada: measures compuestas que referencian otras measures

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros

# COMMAND ----------

import pandas as pd
import json, re, requests, sys
from collections import defaultdict

# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "migracion_pbix", "Catalogo destino")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("schema", "couch", "Schema destino")
dbutils.widgets.text("data_locations", "", "Ubicaciones de tablas físicas (lista catalog.schema separada por coma; vacío=usar destino)")
dbutils.widgets.text("module_path", "", "Path modulos (vacío = derivar del usuario actual)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4", "Endpoint LLM")
# Traductor (pbi_name_translator): por default usa el mismo catalog/schema del destino,
# pero se puede apuntar a OTRO (ej. sat_reportes/default si ahí está el traductor poblado).
dbutils.widgets.text("translator_catalog", "", "Catalog del traductor (vacío = usar catalog destino)")
dbutils.widgets.text("translator_schema", "", "Schema del traductor (vacío = usar schema destino)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATA_LOCATIONS_RAW = dbutils.widgets.get("data_locations").strip()
MODULE_PATH = dbutils.widgets.get("module_path").strip() or f"/Workspace/Users/{_CURRENT_USER}/powerbi-model-analyzer"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")

# Parsear data_locations (catalog.schema, ...) — donde viven las tablas físicas
DATA_LOCATIONS = []
if DATA_LOCATIONS_RAW:
    for _loc in DATA_LOCATIONS_RAW.split(','):
        _loc = _loc.strip()
        if not _loc or '.' not in _loc:
            continue
        _c, _s = _loc.split('.', 1)
        DATA_LOCATIONS.append((_c.strip(), _s.strip()))
if not DATA_LOCATIONS:
    DATA_LOCATIONS = [(CATALOG, SCHEMA)]
print(f"Ubicaciones de tablas físicas: {DATA_LOCATIONS}")

TRANSLATOR_CATALOG = dbutils.widgets.get("translator_catalog").strip() or CATALOG
TRANSLATOR_SCHEMA  = dbutils.widgets.get("translator_schema").strip() or SCHEMA


RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
print(f"Destino:   {CATALOG}.{SCHEMA}")
print(f"Traductor: {TRANSLATOR_CATALOG}.{TRANSLATOR_SCHEMA}")
print(f"LLM:       {LLM_ENDPOINT}")

# --- Helper: test de ejecucion real de una measure via SQL Warehouse ---
import time as _time

def _test_measure_execution(view_name, measure_name):
    """Ejecuta SELECT MEASURE(name) FROM mv GROUP BY first_dim LIMIT 1 via SQL Warehouse.
    Retorna (ok, error_msg). Detecta errores de runtime como CAST_OVERFLOW."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")

    # Buscar warehouse
    wh_resp = requests.get(f"https://{host}/api/2.0/sql/warehouses",
                           headers={"Authorization": f"Bearer {token}"}, timeout=30)
    warehouses = wh_resp.json().get('warehouses', [])
    wh_id = None
    for w in warehouses:
        if 'serverless' in w.get('name', '').lower() or w.get('warehouse_type') == 'PRO':
            wh_id = w['id']
            break
    if not wh_id and warehouses:
        wh_id = warehouses[0]['id']
    if not wh_id:
        return True, None  # Sin warehouse, skip el test

    # Full scan: sin LIMIT, sin GROUP BY -> obliga a evaluar la expresion sobre TODAS las filas.
    # Atrapa errores de cast/data como "value Lambda cannot be cast to DOUBLE".
    test_sql = f"SELECT MEASURE(`{measure_name}`) FROM {view_name}"
    resp = requests.post(
        f"https://{host}/api/2.0/sql/statements",
        json={"warehouse_id": wh_id, "statement": test_sql, "wait_timeout": "0s"},
        headers={"Authorization": f"Bearer {token}"}, timeout=30
    )
    stmt = resp.json()
    stmt_id = stmt.get('statement_id', '')
    state = stmt.get('status', {}).get('state', '')

    start = _time.time()
    while state in ('PENDING', 'RUNNING', '') and (_time.time() - start) < 60:
        _time.sleep(2)
        poll = requests.get(
            f"https://{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"}, timeout=30
        ).json()
        state = poll.get('status', {}).get('state', '')
        stmt = poll

    if state == 'SUCCEEDED':
        return True, None
    else:
        err = stmt.get('status', {}).get('error', {}).get('message', f'state={state}')
        return False, err

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer Metric Views base y measures pendientes

# COMMAND ----------

# Measures con DAX completo
measures_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_measures')}").toPandas()
print(f"{len(measures_df)} measures totales")

# Descubrir Metric Views base (creadas por notebook 1)
# Usamos la API de Unity Catalog porque SHOW CREATE TABLE no funciona con Metric Views
mv_views = {}
tables_in_catalog = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

for r in tables_in_catalog:
    tname = r.tableName
    # Solo MVs de ESTA corrida: con prefijo mv_ y sufijo de run_id (si aplica).
    # Esto evita procesar mv_* viejas de otras migraciones que conviven en el mismo schema.
    if not tname.startswith("mv_"):
        continue
    if SUFFIX and not tname.endswith(SUFFIX):
        continue
    view_name = f"{CATALOG}.{SCHEMA}.{tname}"
    try:
        resp = requests.get(
            f"https://{host}/api/2.1/unity-catalog/tables/{view_name}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        resp.raise_for_status()
        table_info = resp.json()
        yaml_text = table_info.get("view_definition", "")
        if yaml_text:
            # base_table sin "mv_" Y sin sufijo de run_id (para matchear con DAX original)
            base_table = tname[3:]
            if SUFFIX and base_table.endswith(SUFFIX):
                base_table = base_table[:-len(SUFFIX)]
            mv_views[base_table] = {
                'yaml': yaml_text,
                'view_name': view_name,
            }
            print(f"  {view_name} ({len(yaml_text)} chars)")
        else:
            print(f"  {view_name}: sin view_definition")
    except Exception as e:
        print(f"  Error leyendo {view_name}: {str(e)[:500]}")

print(f"\n{len(mv_views)} Metric Views base encontradas")

# Tablas de datos en UC — iterar DATA_LOCATIONS (donde viven las tablas físicas)
# en lugar de solo CATALOG.SCHEMA (donde viven los artefactos generados).
existing_tables = {}        # {table_name: [col_names]}     — sin FQN, nombre simple
existing_table_types = {}   # {table_name: {col: dtype}}
table_to_fqn = {}           # {table_name: catalog.schema.table} — para construir SQL FQN
for c_loc, s_loc in DATA_LOCATIONS:
    try:
        _tabs = spark.sql(f"SHOW TABLES IN {c_loc}.{s_loc}").collect()
    except Exception as _e:
        print(f"  ✗ no pude listar {c_loc}.{s_loc}: {str(_e)[:120]}")
        continue
    for _r in _tabs:
        _tname = _r.tableName
        if _tname.startswith(("pbi_", "mv_", "v_dashboard_", "lod_", "datetabletemplate_", "localdatetable_")):
            continue
        _full = f"{c_loc}.{s_loc}.{_tname}"
        try:
            _cols = spark.sql(f"DESCRIBE {_full}").collect()
            existing_tables[_tname] = [c.col_name for c in _cols if not c.col_name.startswith('#')]
            existing_table_types[_tname] = {c.col_name: c.data_type for c in _cols if not c.col_name.startswith('#')}
            table_to_fqn[_tname] = _full
        except Exception as _e:
            print(f"  Error leyendo {_full}: {str(_e)[:120]}")
print(f"  {len(existing_tables)} tablas físicas descubiertas en data_locations")

# Relaciones
try:
    rels_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_relationships')}").toPandas()
except:
    rels_df = pd.DataFrame()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agrupar measures por view

# COMMAND ----------

def detect_base_table(dax_expression, measure_table):
    if not dax_expression:
        return None
    agg_patterns = [
        r"COUNTROWS\(\s*'?([^')]+)'?\s*\)",
        r"(?:COUNT|SUM|AVERAGE|MIN|MAX|DISTINCTCOUNT)\(\s*'?([^')\[]+)'?\[",
    ]
    general_patterns = [
        r"SELECTEDVALUE\(\s*'?([^')\[]+)'?\[",
        r"FILTER\(\s*'?([^'),]+)'?",
        r"VALUES\(\s*'?([^')\[]+)'?\[",
        r"'([^']+)'\[",
    ]
    excluded = {measure_table.lower(), 'true', 'false', 'blank'}
    agg_tables = set()
    all_tables = set()
    for pattern in agg_patterns:
        for m in re.finditer(pattern, dax_expression, re.IGNORECASE):
            t = (m.group(1) or m.group(2) if m.lastindex > 1 else m.group(1)).strip()
            if t.lower() not in excluded:
                agg_tables.add(t)
                all_tables.add(t)
    for pattern in general_patterns:
        for m in re.finditer(pattern, dax_expression, re.IGNORECASE):
            t = m.group(1).strip()
            if t.lower() not in excluded:
                all_tables.add(t)
    return list(agg_tables)[0] if agg_tables else (list(all_tables)[0] if all_tables else None)


def _find_all_table_refs(dax, measure_table):
    if not dax:
        return []
    patterns = [
        r"COUNTROWS\(\s*'?([^')]+)'?\s*\)",
        r"(?:COUNT|SUM|AVERAGE|MIN|MAX|DISTINCTCOUNT)\(\s*'?([^')\[]+)'?\[",
        r"SELECTEDVALUE\(\s*'?([^')\[]+)'?\[",
        r"FILTER\(\s*'?([^'),]+)'?",
        r"VALUES\(\s*'?([^')\[]+)'?\[",
        r"ALL\(\s*'?([^')]+)'?\s*\)",
        r"ALLEXCEPT\(\s*'?([^'),]+)'?",
        r"'([^']+)'\[",
    ]
    excluded = {measure_table.lower(), 'true', 'false', 'blank'}
    tables = []
    for pattern in patterns:
        for m in re.finditer(pattern, dax, re.IGNORECASE):
            for g in range(1, m.lastindex + 1 if m.lastindex else 2):
                try:
                    t = m.group(g)
                    if t and t.strip().lower() not in excluded and not any(c in t for c in '()=<>'):
                        tables.append(t.strip())
                except:
                    pass
    return tables


def _match_to_uc_table(table_name, existing_tables):
    normalized = table_name.lower().replace(' ', '_').replace("'", "")
    if normalized in existing_tables:
        return normalized
    for tname in existing_tables:
        if normalized.replace('_', '') == tname.replace('_', ''):
            return tname
        if normalized.replace('_', '') in tname.replace('_', '') or tname.replace('_', '') in normalized.replace('_', ''):
            return tname
    return None


# Agrupar measures por su view destino
measures_by_view = defaultdict(list)
unassigned = []

# ── Intento 1: leer mapping pre-calculado por notebook 1 ─────────────────────
# Notebook 1 ya hizo este trabajo (regex + LLM batch) y persistió measure_to_view_mapping.
# Si existe, evitamos duplicar el esfuerzo (y no perdemos consistencia).
_USED_PREMAP = False
try:
    _premap_fqn = f"{CATALOG}.{SCHEMA}.{_t('measure_to_view_mapping')}"
    _premap_df = spark.sql(f"SELECT * FROM {_premap_fqn}").toPandas()
    if len(_premap_df) > 0:
        # Construir índice por nombre de measure para lookup rápido
        _premap_idx = {}
        for _, r in _premap_df.iterrows():
            _premap_idx[r['pbi_measure_name']] = {
                'base_table': r.get('base_table', '') or '',
                'target_mv': r.get('target_mv', '') or '',
            }
        # Aplicar el mapping a cada measure
        for _, row in measures_df.iterrows():
            name = row.get("Measure", "")
            table = row.get("Tabla", "")
            dax = str(row.get("DAX", ""))
            pre = _premap_idx.get(name)
            if pre and pre['base_table'] and pre['base_table'] in mv_views:
                measures_by_view[pre['base_table']].append({
                    'measure_name': name, 'table': table, 'dax': dax,
                })
            else:
                unassigned.append({
                    'measure_name': name, 'table': table, 'target': pre['base_table'] if pre else None, 'dax': dax,
                })
        _USED_PREMAP = True
        print(f"Mapping pre-calculado leído de {_premap_fqn}: {len(_premap_df)} filas")
        print(f"  Asignadas a MV existente: {sum(len(v) for v in measures_by_view.values())}")
        print(f"  Sin MV (van a fallback LLM): {len(unassigned)}")
except Exception as _e:
    print(f"(info) No hay mapping pre-calculado ({str(_e)[:120]}); usando detección original")

# ── Intento 2: detección original (regex + LLM) si no había mapping pre-calculado ─────
if not _USED_PREMAP:
    for _, row in measures_df.iterrows():
        name = row.get("Measure", "")
        table = row.get("Tabla", "")
        dax = str(row.get("DAX", ""))

        # Detectar tabla base
        base = detect_base_table(dax, table)
        if base:
            base_normalized = base.lower().replace(' ', '_').replace("'", "")
            matched = _match_to_uc_table(base, existing_tables) or base_normalized
        else:
            # Fallback + reasignacion
            base_normalized = table.lower().replace(' ', '_').replace("'", "") if table else None
            matched = None
            if base_normalized:
                matched = _match_to_uc_table(table, existing_tables)
                if not matched:
                    for dt in _find_all_table_refs(dax, table):
                        matched = _match_to_uc_table(dt, existing_tables)
                        if matched:
                            break
            if not matched:
                matched = base_normalized

        # Buscar en que view cae
        if matched and matched in mv_views:
            measures_by_view[matched].append({'measure_name': name, 'table': table, 'dax': dax})
        else:
            unassigned.append({'measure_name': name, 'table': table, 'target': matched, 'dax': dax})

# ── Fallback LLM: si quedaron measures sin asignar, pedir a Claude que las clasifique ──
# El regex falla cuando el DAX referencia múltiples tablas, usa calc cols complejas,
# o cuando la measure vive en una "table" de PBI que no corresponde a una tabla física
# (ej. tablas como MedidasFat, MedidasAll que son contenedoras de measures).
def _resolve_unassigned_with_claude(unassigned_list, available_bases, batch_size=20):
    """Manda batches de measures sin asignar a Claude y devuelve {measure_name: base_table}."""
    if not unassigned_list or not available_bases:
        return {}
    final_map = {}
    n = len(unassigned_list)
    n_batches = (n + batch_size - 1) // batch_size
    print(f"  Pasando {n} measures sin asignar a Claude en {n_batches} batches…")
    for i in range(0, n, batch_size):
        batch = unassigned_list[i:i + batch_size]
        payload = [
            {"name": u['measure_name'], "pbi_table": u.get('table',''), "dax": (u.get('dax') or '')[:600]}
            for u in batch
        ]
        prompt = f"""You map Power BI DAX measures to a base Metric View in Databricks.

Available Metric View base names (use ONLY these; return null if no good match):
{json.dumps(sorted(available_bases), indent=2)}

For each measure, decide which base table the aggregation primarily operates on.
Consider the DAX expression (which tables it scans) and the measure's PBI 'home' table.
If the measure references a measure from another table, follow the chain to the underlying aggregation table.

Measures to classify:
{json.dumps(payload, indent=2)}

Return ONLY a JSON object {{"<measure_name>": "<base_table_or_null>", ...}}. No markdown, no explanations."""
        try:
            result = call_claude(prompt, max_tokens=4000)
            if "```" in result:
                m = re.search(r'\{.*\}', result, re.DOTALL)
                result = m.group() if m else result
            batch_map = json.loads(result)
            for k, v in batch_map.items():
                if v and v in available_bases:
                    final_map[k] = v
        except Exception as e:
            print(f"    [WARN] batch {i//batch_size + 1} falló: {str(e)[:200]}")
    return final_map

if unassigned and mv_views:
    llm_assignments = _resolve_unassigned_with_claude(unassigned, list(mv_views.keys()))
    still_unassigned = []
    for u in unassigned:
        target = llm_assignments.get(u['measure_name'])
        if target and target in mv_views:
            measures_by_view[target].append({
                'measure_name': u['measure_name'],
                'table': u['table'],
                'dax': u.get('dax', ''),
            })
            print(f"  [LLM] '{u['measure_name']}' → {target}")
        else:
            still_unassigned.append(u)
    unassigned = still_unassigned

print(f"\nMeasures asignadas a views:")
for vname, mlist in measures_by_view.items():
    print(f"  {vname}: {len(mlist)} measures")
if unassigned:
    print(f"\nSin view ({len(unassigned)}):")
    for u in unassigned:
        print(f"  {u['measure_name']} (tabla: {u['table']}, target: {u['target']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Importar docs y construir system prompt

# COMMAND ----------

sys.path.insert(0, MODULE_PATH)
from metrics_view_docs import METRICS_VIEW_DOCS
from dax_function_reference import get_relevant_dax_docs

DAX_DOCS = get_relevant_dax_docs(measures_df)
print(f"DAX docs: {len(DAX_DOCS)} chars")

SYSTEM_PROMPT = f"""You are an expert in converting Power BI DAX measures to Databricks SQL expressions for Metrics Views.

{METRICS_VIEW_DOCS}

{DAX_DOCS}

GENERAL RULES:
- Use MEASURE() for composed measures that reference other measures
- Use scalar subqueries for ALL(table) patterns
- Use window specs for ALL(column) patterns
- Output ONLY valid JSON — no markdown fences, no explanations
"""

# Modelos que NO aceptan el parámetro 'temperature' (extended thinking).
_MODELS_NO_TEMPERATURE = {'databricks-claude-opus-4-7'}


import time as _time_retry
_RETRY_STATUS = {502, 503, 504, 429}
_MAX_RETRIES = 5

def _post_with_retry(url, headers, payload, timeout):
    """POST con retry exponencial para 502/503/504/429 y errores de red."""
    delay = 2
    last_resp = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code not in _RETRY_STATUS:
                return resp
            last_resp = resp
            print(f"  [retry {attempt+1}/{_MAX_RETRIES}] {resp.status_code} de {url.split('/')[-2]}, esperando {delay}s…")
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"  [retry {attempt+1}/{_MAX_RETRIES}] error de red ({type(e).__name__}), esperando {delay}s…")
        _time_retry.sleep(delay)
        delay = min(delay * 2, 60)
    return last_resp  # devuelve la última respuesta (con su status code) si nunca pasó

def call_claude(prompt, system_prompt=None, max_tokens=4000):
    if system_prompt is None:
        system_prompt = SYSTEM_PROMPT
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    url = f"https://{host}/serving-endpoints/{LLM_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
    }
    if LLM_ENDPOINT not in _MODELS_NO_TEMPERATURE:
        payload["temperature"] = 0.1

    resp = _post_with_retry(url, headers, payload, 120)
    # Fallback: si el modelo rechaza temperature dinámicamente
    if resp.status_code == 400 and 'temperature' in resp.text.lower():
        payload.pop('temperature', None)
        resp = _post_with_retry(url, headers, payload, 120)

    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        lines = content.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines)
    return content.strip()


def _parse_json_object(txt):
    """Parser robusto de UN objeto JSON. Tolera fences, texto antes/después y
    objetos falsos en prose. Usa json.JSONDecoder.raw_decode."""
    if not txt:
        return None
    s = txt.strip()
    if s.startswith("```"):
        lines = s.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    decoder = json.JSONDecoder()
    i = 0
    n = len(s)
    while i < n:
        idx = s.find('{', i)
        if idx < 0:
            break
        try:
            obj, _ = decoder.raw_decode(s[idx:])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
        i = idx + 1
    return None


def _parse_json_array(txt):
    """Parser robusto de UN array JSON. Tolera fences, texto antes/después y
    arrays falsos en prose."""
    if not txt:
        return []
    s = txt.strip()
    if s.startswith("```"):
        lines = s.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    decoder = json.JSONDecoder()
    i = 0
    n = len(s)
    while i < n:
        idx = s.find('[', i)
        if idx < 0:
            break
        try:
            obj, _ = decoder.raw_decode(s[idx:])
            if isinstance(obj, list):
                return obj
        except json.JSONDecodeError:
            pass
        i = idx + 1
    return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Helpers

# COMMAND ----------

def _yaml_quote(value):
    """Quote a YAML string value safely."""
    if value is None:
        return '""'
    value = str(value)
    value = value.replace('\\', '\\\\').replace('"', '\\"')
    value = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    while '  ' in value:
        value = value.replace('  ', ' ')
    return f'"{value.strip()}"'


def _build_measure_yaml(m):
    """Build YAML lines for a single measure from Claude's JSON."""
    lines = []
    lines.append(f"  - name: {m['name']}")
    lines.append(f"    expr: {_yaml_quote(m['sql_expr'])}")
    if m.get("display_name"):
        lines.append(f"    display_name: {_yaml_quote(m['display_name'])}")
    if m.get("comment"):
        lines.append(f"    comment: {_yaml_quote(m['comment'])}")
    # format: pass through Claude's object as-is
    fmt = m.get("format")
    if fmt and isinstance(fmt, dict) and fmt.get("type"):
        lines.append("    format:")
        for fk, fv in fmt.items():
            if fv is None:
                continue
            if isinstance(fv, dict):
                lines.append(f"      {fk}:")
                for sk, sv in fv.items():
                    lines.append(f"        {sk}: {sv}")
            else:
                lines.append(f"      {fk}: {fv}")
    # window: pass through Claude's list as-is
    window = m.get("window")
    if window and isinstance(window, list) and len(window) > 0:
        lines.append("    window:")
        for w in window:
            if isinstance(w, dict) and w.get("order"):
                parts = [f"{k}: {v}" for k, v in w.items() if v is not None]
                lines.append(f"      - {{{', '.join(parts)}}}")
    return "\n".join(lines)


def _build_bare_measure_yaml(name, expr, display_name=None):
    """Build minimal YAML for a measure (no format/window)."""
    lines = [f"  - name: {name}", f"    expr: {_yaml_quote(expr)}"]
    if display_name:
        lines.append(f"    display_name: {_yaml_quote(display_name)}")
    return "\n".join(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Traducir DAX a SQL y crear measures una a una

# COMMAND ----------

MAX_FIX_RETRIES = 3
all_results = []

# Cargar traductor PBI ↔ Databricks (creado por paso 1e). Si está disponible,
# lo pasamos al LLM para que use nombres exactos en lugar de adivinar.
translator_by_table = {}  # {pbi_table: [{'pbi_name', 'pbi_type', 'databricks_name'}]}
# El traductor puede vivir en otro catalog/schema (configurable via widgets).
# Si TRANSLATOR_CATALOG/SCHEMA fueron sobreescritos por widget, no se aplica el sufijo run_id
# (porque la tabla externa puede no tener ese sufijo). Si son iguales al destino, sí se aplica.
_trans_table = "pbi_name_translator"
if TRANSLATOR_CATALOG == CATALOG and TRANSLATOR_SCHEMA == SCHEMA:
    _trans_table = _t(_trans_table)
_trans_fqn = f"{TRANSLATOR_CATALOG}.{TRANSLATOR_SCHEMA}.{_trans_table}"
try:
    _t_df = spark.sql(
        f"SELECT pbi_table, pbi_name, pbi_type, databricks_name "
        f"FROM {_trans_fqn} "
        f"WHERE databricks_name IS NOT NULL AND databricks_name != ''"
    ).toPandas()
    for _, r in _t_df.iterrows():
        translator_by_table.setdefault(str(r['pbi_table']), []).append({
            'pbi_name': str(r['pbi_name']),
            'pbi_type': str(r['pbi_type']),
            'databricks_name': str(r['databricks_name']),
        })
    print(f"Traductor cargado desde {_trans_fqn}: {len(_t_df)} mappings")
except Exception as _e:
    print(f"  (info) sin traductor disponible en {_trans_fqn} ({str(_e)[:100]})")

def _translator_block_for_table(pbi_table):
    """Construye el sub-diccionario de traducciones para la tabla actual + sus relacionadas.
    Incluye 'all tables' como fallback cuando un measure cruza tablas."""
    if not translator_by_table:
        return "  (no translator available — use snake_case ASCII normalization)"
    lines = []
    # Primero la tabla actual
    if pbi_table in translator_by_table:
        lines.append(f"  Table '{pbi_table}':")
        for it in translator_by_table[pbi_table]:
            lines.append(f"    [{it['pbi_type']}] '{it['pbi_name']}' → {it['databricks_name']}")
    # Otras tablas (las measures pueden referenciar otras)
    for other_table, items in translator_by_table.items():
        if other_table == pbi_table:
            continue
        lines.append(f"  Table '{other_table}':")
        for it in items[:30]:  # cap por brevedad
            lines.append(f"    [{it['pbi_type']}] '{it['pbi_name']}' → {it['databricks_name']}")
    return "\n".join(lines) if lines else "  (no mappings for this table)"

for base_table, mlist in measures_by_view.items():
    info = mv_views[base_table]
    view_name = info['view_name']
    current_yaml = info['yaml']
    catalog_table = base_table

    # Columnas de la tabla source
    cols = existing_tables.get(catalog_table, [])
    col_types = existing_table_types.get(catalog_table, {})
    cols_detail = "\n".join(f"  - {c}: {col_types.get(c, 'unknown')}" for c in cols)

    # Tablas relacionadas
    related_tables_cols = ""
    base_orig = mlist[0].get('table', '') if mlist else ''
    related_table_names = set()
    if hasattr(rels_df, 'iterrows'):
        for _, rel in rels_df.iterrows():
            from_t = str(rel.get('FromTableName', ''))
            to_t = str(rel.get('ToTableName', ''))
            if from_t == base_orig:
                related_table_names.add(to_t)
            elif to_t == base_orig:
                related_table_names.add(from_t)
    for rt in related_table_names:
        rt_match = _match_to_uc_table(rt, existing_tables)
        if rt_match:
            rt_cols = existing_tables.get(rt_match, [])
            rt_types = existing_table_types.get(rt_match, {})
            # FQN correcto basado en data_locations
            _rt_fqn = table_to_fqn.get(rt_match, f"{CATALOG}.{SCHEMA}.{rt_match}")
            related_tables_cols += f"\n\nRelated: {_rt_fqn} (PBI: '{rt}')"
            for c in rt_cols:
                related_tables_cols += f"\n  - {c}: {rt_types.get(c, 'unknown')}"

    # Ordenar measures topológicamente: las que NO referencian otras measures de la lista van primero.
    # Heurística: contar cuántas measures de mlist aparecen como [Name] en el DAX.
    _all_measure_names = {m['measure_name'] for m in mlist}
    def _ref_count(m):
        _refs = re.findall(r'\[([^\]]+)\]', m.get('dax', '') or '')
        return sum(1 for r in _refs if r in _all_measure_names)
    mlist = sorted(mlist, key=_ref_count)

    # Quitar TODA la seccion measures del YAML base (incluyendo __row_count y cualquier otra)
    # Asi empezamos limpio — solo source + joins + dimensions
    clean_lines = []
    in_measures = False
    for line in current_yaml.split('\n'):
        stripped = line.strip()
        if stripped == 'measures:' or stripped.startswith('measures:'):
            in_measures = True
            continue
        if in_measures:
            # Seguir saltando mientras estemos en contenido indentado de measures
            # Una linea no-vacia sin indentacion marca el inicio de otra seccion top-level
            if stripped == '' or line.startswith('  ') or line.startswith('\t'):
                continue
            else:
                in_measures = False
        clean_lines.append(line)
    current_yaml = '\n'.join(clean_lines).rstrip() + '\n'

    # Extraer TODOS los nombres existentes (dimensions + measures residuales) para colisiones
    existing_names = set()
    for line in current_yaml.split('\n'):
        stripped = line.strip()
        if stripped.startswith('- name:'):
            existing_names.add(stripped.replace('- name:', '').strip().strip("'\""))

    # Extraer dimensions del YAML para pasarlas al prompt
    yaml_dimensions = []
    in_dims = False
    current_dim = {}
    for line in current_yaml.split('\n'):
        stripped = line.strip()
        if stripped == 'dimensions:' or stripped.startswith('dimensions:'):
            in_dims = True
            continue
        if in_dims:
            if stripped.startswith('- name:'):
                if current_dim:
                    yaml_dimensions.append(current_dim)
                current_dim = {'name': stripped.replace('- name:', '').strip().strip("'\""), 'expr': ''}
            elif stripped.startswith('expr:') and current_dim:
                current_dim['expr'] = stripped.replace('expr:', '').strip().strip("'\"")
            elif stripped and not line.startswith(' ') and not line.startswith('\t') and ':' in stripped:
                if current_dim:
                    yaml_dimensions.append(current_dim)
                    current_dim = {}
                in_dims = False
    if current_dim:
        yaml_dimensions.append(current_dim)

    dimensions_list = "\n".join(f"  - {d['name']}: {d['expr']}" for d in yaml_dimensions if d.get('name'))

    # Detect current source
    current_source_lines = []
    in_src = False
    for sl in current_yaml.split('\n'):
        if sl.strip().startswith('source:'):
            src_value = sl.split('source:', 1)[1].strip()
            if src_value in ('|', '>'):
                in_src = True
            else:
                current_source_lines.append(src_value)
            continue
        if in_src:
            if sl.startswith('  '):
                current_source_lines.append(sl.strip())
            else:
                in_src = False
    # FQN correcto: si la tabla está en data_locations (otro catalog), usar esa ubicación,
    # no el destino (CATALOG.SCHEMA) que es donde viven los artefactos generados.
    _table_fqn = table_to_fqn.get(catalog_table, f"{CATALOG}.{SCHEMA}.{catalog_table}")
    current_source = ' '.join(current_source_lines).strip() or _table_fqn

    print(f"\n{'='*60}")
    print(f"{view_name}: {len(mlist)} measures, una a una")
    print(f"  Dimensions en YAML: {len(yaml_dimensions)}")
    print(f"  Nombres existentes: {len(existing_names)}")
    print(f"{'='*60}")

    accepted = []
    skipped = []
    skipped_runtime = []

    for m_idx, m in enumerate(mlist):
        measure_name = m['measure_name']
        dax = m['dax']

        print(f"\n  [{m_idx+1}/{len(mlist)}] {measure_name}")

        translate_prompt = f"""Convert this single DAX measure to a SQL expression for a Databricks Metrics View.

RESPONSE FORMAT — return ONE of these JSON structures:

Option A — Regular measure:
{{"name": "snake_case_name", "sql_expr": "SQL expression", "display_name": "Human Name", "comment": "Original DAX: ...", "format": {{"type": "number"}} or null, "window": [{{"order": "dim", "range": "all", "semiadditive": "last"}}] or null}}

Skip (ONLY if genuinely impossible):
{{"skip": true, "reason": "explanation"}}

Format rules:
- number: {{"type": "number"}} or {{"type": "number", "decimal_places": {{"type": "exact", "places": 2}}}}
- percentage: {{"type": "percentage"}}
- currency: {{"type": "currency", "currency_code": "MXN"}}
- null if no formatting

Current source: {current_source}
Source table (FQN): {_table_fqn}
CRITICAL: If you need to write a subquery referencing the source table (e.g., correlated MAX), use the FQN above ({_table_fqn}), NOT just the table name. The current schema is `{CATALOG}.{SCHEMA}` but the source table is NOT there.
Source columns:
{cols_detail}
{related_tables_cols}

PBI ↔ DATABRICKS NAME DICTIONARY (use these exact databricks names when translating DAX references):
{_translator_block_for_table(base_orig)}

Dimensions ALREADY AVAILABLE in this Metric View (use directly in expressions):
{dimensions_list}

Measures already in this view (use MEASURE(name) to reference):
{json.dumps(accepted)}

Measure to convert:
  Name: {measure_name}
  DAX: {dax}

AVAILABLE BUILDING BLOCKS — use these to construct the SQL expression:
1. Source columns — must be inside an aggregate: SUM, COUNT, MAX, MIN, AVG, ANY_VALUE
2. Dimensions listed above — pre-computed and available directly. Some are Fixed LOD columns (window functions computed in the source). Use ANY_VALUE(dim_name) to reference them in measures.
3. Existing measures — reference with MEASURE(name)
4. Window measures — for Coarser LOD patterns (grand totals, percent-of-total, running totals, period-over-period, YTD, semiadditive). Use the window field as documented in sections 8, 9, and 13b of the Metric View docs in the system prompt.

CRITICAL: SQL window functions (ANY function with OVER clause) are NOT allowed in measure expr.
For ANY pattern that would need a window function, you have TWO alternatives:
- Fixed LOD: the dimensions listed above already include pre-computed window results. Find the matching dimension and use ANY_VALUE(dimension_name).
- Coarser LOD / Window measures: use the "window" field on the measure (order + range + semiadditive, all three required).
Apply the full LOD documentation from the system prompt (sections 8, 9, 13b) — it covers all window patterns including trailing, cumulative, period-over-period, semiadditive, YTD, and multi-dimension exclusion.

NEVER skip a measure because "it needs a window function." There is ALWAYS a Fixed LOD dimension or a Coarser LOD window measure pattern that handles it.

COLUMN NAME CASE & QUOTING (CRITICAL):
- Use the EXACT column names AS LISTED in the "Source columns" block (preserve case, do not lowercase, do not snake_case).
  Example: if source has `MessageTime`, write `SUM(MessageTime)` (NOT `SUM(message_time)`).
- ASCII column names (only letters/digits/underscore): use UNQUOTED in SQL — `SUM(MessageTime)`, `SUM(fecha_corte)`.
- Column names with SPACES, ACCENTS, Ñ, %, /, or any non-ASCII char (e.g., `Mes_Año`, `Cumplimiento %`):
    * In the SQL expr you MUST backtick-quote: SUM(`Mes_Año`)
    * The downstream YAML serializer wraps the value in double quotes — that handles YAML escaping.
    * NEVER produce raw YAML like `expr: ``Mes_Año`` ` — the metric view parser fails on backticks that start a YAML token.
- The DICTIONARY block above shows PBI ↔ Databricks name mappings, but the actual SQL must reference the EXACT source column name (the right-hand side of the dictionary).

DAX conversion rules:
- DIVIDE(a, b): MEASURE(a) / NULLIF(MEASURE(b), 0)
- CALCULATE(agg, filter): agg FILTER (WHERE condition)
- ALL('Table'): scalar subquery (SELECT agg FROM {_table_fqn}) or Coarser LOD with window range: all
- Every column MUST be in an aggregation or MEASURE()
- CAST: **prefer TRY_CAST over CAST** for any conversion that touches user data (string→numeric, string→date, numeric→smaller numeric).
    TRY_CAST returns NULL on malformed values instead of failing the entire query. Real-world source data often has dirty values like 'VARIAS', 'N/A', empty strings, or numbers exceeding INTEGER range.
    * String to number:  TRY_CAST(col AS BIGINT)            NOT  CAST(col AS BIGINT)
    * String to date:    TRY_CAST(col AS DATE)              NOT  CAST(col AS DATE)
    * Long to int:       TRY_CAST(col AS INT)               NOT  CAST(col AS INT)
    * Use plain CAST only when the source type is GUARANTEED clean (e.g., DATE → STRING formatting, INT → BIGINT widening).
- CAST dates from string: use TRY_CAST(x AS DATE) for ADD_MONTHS, DATE_ADD, etc. (returns NULL if malformed, doesn't crash)
- INTERVAL only accepts LITERAL values: INTERVAL '7' DAY is valid, INTERVAL (expression) DAY is NOT. For computed intervals use DATE_SUB(date, n) or DATE_ADD(date, n) where n is an integer expression. Example: instead of "date - INTERVAL (DAYOFWEEK(date) - 2) DAY" write "DATE_SUB(date, DAYOFWEEK(date) - 2)".
- WEEKDAY(date, mode) is not supported in Databricks. Use DAYOFWEEK(date) which returns 1=Sunday..7=Saturday, or EXTRACT(DAYOFWEEK FROM date).
- EOMONTH is not supported. Use LAST_DAY(date) instead.
- HASONEVALUE + SELECTEDVALUE: CASE WHEN COUNT(DISTINCT col) = 1 THEN ANY_VALUE(col) ELSE 'default' END
- ALWAYS attempt to convert. Only skip if there is literally no SQL equivalent AND no matching dimension AND no window measure pattern.

Return ONLY the JSON object. No markdown."""

        result = call_claude(translate_prompt, max_tokens=2000)
        m_json = _parse_json_object(result)
        if not m_json:
            print(f"    x Error parseando JSON. Respuesta cruda de Claude:")
            print(f"    ---\n{result[:2000]}\n    ---")
            skipped.append(measure_name)
            all_results.append({"View": view_name, "Original": measure_name, "Measure": measure_name, "Status": "SKIP", "DAX": dax})
            continue

        if m_json.get("skip"):
            reason = m_json.get('reason', 'no convertible')
            print(f"    SKIP: {reason}")
            skipped.append(measure_name)
            all_results.append({"View": view_name, "Original": measure_name, "Measure": measure_name, "Status": "SKIP", "DAX": dax})
            continue

        # --- Regular measure ---
        mname = m_json.get('name', measure_name.lower().replace(' ', '_'))
        current_expr = m_json.get('sql_expr', '')

        # Colision con dimension o measure existente
        if mname in existing_names or mname in accepted:
            mname = f"m_{mname}"
            # Si sigue colisionando, agregar sufijo numerico
            if mname in existing_names or mname in accepted:
                mname = f"{mname}_{m_idx}"

        # --- Insertar en YAML y validar ---
        success = False
        for attempt in range(MAX_FIX_RETRIES + 1):
            if attempt == 0:
                block = _build_measure_yaml({**m_json, 'name': mname})
            else:
                block = _build_bare_measure_yaml(mname, current_expr, m_json.get('display_name'))

            if "measures:" not in current_yaml:
                test_yaml = current_yaml.rstrip() + "\nmeasures:\n" + block + "\n"
            else:
                test_yaml = current_yaml.rstrip() + "\n" + block + "\n"

            try:
                spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{test_yaml}\n$$""")
                # Test de ejecucion real via SQL Warehouse
                exec_ok, exec_err = _test_measure_execution(view_name, mname)
                if not exec_ok:
                    print(f"    x RUNTIME {mname}: {exec_err[:150]}")
                    # Revertir: volver al YAML sin esta measure
                    spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{current_yaml}\n$$""")
                    skipped_runtime.append({'measure': mname, 'view': view_name, 'error': exec_err[:200]})
                    success = False
                    break
                success = True
                current_yaml = test_yaml
                accepted.append(mname)
                print(f"    OK {mname}" + (f" (fix {attempt})" if attempt > 0 else ""))
                break
            except Exception as e:
                last_error = str(e)[:500]
                print(f"    x intento {attempt+1}: {last_error[:200]}")

                if attempt >= MAX_FIX_RETRIES:
                    break

                # Error de nombre duplicado → renombrar sin gastar llamada a Claude
                if 'names must be unique' in last_error:
                    mname = f"measure_{mname}" if not mname.startswith("measure_") else f"{mname}_{attempt}"
                    print(f"    -> renombrando a: {mname}")
                    continue

                # Pedir fix regular a Claude
                is_last_attempt = (attempt >= MAX_FIX_RETRIES - 1)
                fix_prompt = f"""Fix this SQL expression for a Databricks Metrics View measure. This is attempt {attempt+1} of {MAX_FIX_RETRIES+1}.

Measure: {mname}
Expr: {current_expr}
Error: {last_error}

Dimensions AVAILABLE in this Metric View (use these names, NOT raw table.column):
{dimensions_list}

Existing measures (reference with MEASURE(name)):
{json.dumps(accepted)}

Source table columns:
{cols}

RULES:
- For UNRESOLVED_COLUMN: the column might exist as a DIMENSION above (possibly with a join prefix like joinname_column). Search the dimensions list. Or it might be a MEASURE — check the measures list.
- For METRIC_VIEW_WINDOW_FUNCTION_NOT_SUPPORTED: window functions are NOT allowed in measures. Use ANY_VALUE(dimension_name) if there is a pre-computed LOD dimension above, or use the window field (order + range + semiadditive).
- For INVALID_AGGREGATE_FILTER: the FILTER WHERE clause cannot contain aggregates or references to other tables. Use only source columns or dimension names.
- For DATATYPE_MISMATCH: use TRY_CAST(x AS DATE) for date functions (safer than CAST — returns NULL on bad data). If a column is BIGINT and needs to be a date, use DATE_FROM_UNIX_DATE(col) or TO_DATE(CAST(col AS STRING), 'yyyyMMdd').
- For CAST_INVALID_INPUT or CAST_OVERFLOW: replace CAST with TRY_CAST. Real data has dirty values like 'VARIAS', 'N/A', longs exceeding INT range. TRY_CAST handles them by returning NULL.
    Example: CAST(CLAVE_BODEGA AS BIGINT) → TRY_CAST(CLAVE_BODEGA AS BIGINT).
- For PARSE_SYNTAX_ERROR with INTERVAL: INTERVAL only accepts LITERALS like INTERVAL '7' DAY. For computed intervals use DATE_SUB(date, n) or DATE_ADD(date, n). Example: instead of "date - INTERVAL (expr) DAY" write "DATE_SUB(date, expr)".
- WEEKDAY(date, mode) is not supported. Use DAYOFWEEK(date) (1=Sunday..7=Saturday).
- EOMONTH is not supported. Use LAST_DAY(date).
- NESTED_AGGREGATE_FUNCTION: cannot nest aggregates. Move the inner aggregate to a scalar subquery or use an existing MEASURE().
- TABLE_OR_VIEW_NOT_FOUND: ALWAYS use fully qualified names in subqueries: use {_table_fqn} (the FQN of the source table, which may live in a different catalog/schema than this view).
- Columns from joined tables: FIRST try the dimension name from the list above. If not found, try referencing via the join alias (e.g., joinname.ColumnName). If still not found, try a creative alternative using columns and measures that DO exist.
- {"You may return SKIP only as absolute last resort." if is_last_attempt else "Do NOT return SKIP. Try a different approach — use dimensions, measures, join aliases, or simplify the expression."}

Return ONLY: {{"sql_expr": "fixed expression"}}{' or {{"sql_expr": "SKIP", "reason": "explanation"}}' if is_last_attempt else ''}"""
                try:
                    fix_result = call_claude(fix_prompt, max_tokens=1000)
                    fix_json = _parse_json_object(fix_result) or {}
                    current_expr = fix_json.get('sql_expr', current_expr)
                    if current_expr == 'SKIP':
                        reason = fix_json.get('reason', 'no expresable')
                        print(f"    -> SKIP: {reason}")
                        break
                    print(f"    -> fix: {current_expr[:80]}")
                except Exception as ce:
                    print(f"    -> error fix: {str(ce)[:80]}")
                    break

        if not success:
            skipped.append({'measure_name': measure_name, 'dax': dax, 'last_error': last_error if 'last_error' in dir() else ''})

        all_results.append({
            "View": view_name,
            "Original": measure_name,
            "Measure": mname if success else measure_name,
            "Status": "OK" if success else "SKIP",
            "DAX": dax,
        })

    print(f"\n  === {view_name}: {len(accepted)}/{len(mlist)} OK ===")
    if skipped:
        print(f"  Saltadas (YAML invalido): {len(skipped)}")
        for s in skipped:
            sname = s['measure_name'] if isinstance(s, dict) else s
            print(f"    - {sname}")
    if skipped_runtime:
        print(f"  Saltadas (RUNTIME error): {len(skipped_runtime)}")
        for s in skipped_runtime:
            print(f"    - {s['measure']}: {s['error'][:100]}")

    mv_views[base_table]['yaml'] = current_yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Segunda pasada: measures compuestas

# COMMAND ----------

# Recolectar measures ya creadas
created_measures = {}  # {measure_name_lower: view_name}
for r in all_results:
    if r['Status'] == 'OK':
        created_measures[r['Measure'].lower()] = r['View']

print(f"Measures creadas: {len(created_measures)}")

# Buscar measures no creadas que referencian measures existentes
composed = []
created_names = set(r['Measure'] for r in all_results)
created_originals = set(r['Original'] for r in all_results if r['Status'] == 'OK')
for _, row in measures_df.iterrows():
    name = row.get("Measure", "")
    dax = str(row.get("DAX", ""))
    m_lower = name.lower().replace(' ', '_').replace("'", "")

    # Skip si ya se creo (por nombre snake_case O por nombre original)
    if name in created_originals:
        continue
    if any(m_lower == cn.lower().replace(' ', '_') for cn in created_names):
        continue

    # Buscar referencias a measures existentes en el DAX
    refs = re.findall(r'\[([^\]]+)\]', dax)
    matched_refs = []
    for ref in refs:
        ref_lower = ref.lower().replace(' ', '_')
        if ref_lower in created_measures:
            matched_refs.append((ref, created_measures[ref_lower]))

    if matched_refs:
        composed.append({
            'measure_name': name,
            'dax': dax,
            'target_view': matched_refs[0][1],
            'references': matched_refs,
        })

print(f"Measures compuestas detectadas: {len(composed)}")

if composed:
    by_view = defaultdict(list)
    for c in composed:
        by_view[c['target_view']].append(c)

    for view_name, comp_measures in by_view.items():
        print(f"\n{'='*60}")
        print(f"{view_name}: {len(comp_measures)} measures compuestas")

        # Measures existentes en la view
        existing_in_view = [r['Measure'] for r in all_results if r['View'] == view_name and r['Status'] == 'OK']

        measures_to_add = ""
        for m in comp_measures:
            measures_to_add += f"\n- {m['measure_name']}: {m['dax'][:200]}"

        compose_prompt = f"""Convert these composed DAX measures to SQL. They reference existing measures.

Return ONLY a JSON array: {{"name": "snake_case", "sql_expr": "MEASURE(existing) / NULLIF(MEASURE(other), 0)", "display_name": "Name", "original_name": "Original PBI Measure Name", "format": null}}

EXISTING MEASURES (use MEASURE(name)):
{json.dumps(existing_in_view)}

NEW MEASURES:
{measures_to_add}

Rules:
- Use MEASURE(name) to reference existing measures
- Skip purely cosmetic measures (FORMAT, colors, icons)
- Return ONLY the JSON array."""

        json_result = call_claude(compose_prompt, max_tokens=4000)
        new_json = _parse_json_array(json_result)

        # Encontrar base_table para esta view
        base_table = None
        for bt, info in mv_views.items():
            if info['view_name'] == view_name:
                base_table = bt
                break

        if not base_table:
            continue

        current_yaml = mv_views[base_table]['yaml']

        # Collect existing names to avoid collisions
        existing_names = set()
        for line in current_yaml.split('\n'):
            stripped = line.strip()
            if stripped.startswith('- name:'):
                existing_names.add(stripped.replace('- name:', '').strip().strip("'\""))

        # Lookup original DAX por name (snake) y por display_name
        composed_dax_lookup = {}
        for c in comp_measures:
            mn = str(c.get('measure_name', '')).lower().strip()
            if mn:
                composed_dax_lookup[mn] = c.get('dax', '')

        for m in new_json:
            if m.get("is_dimension", False):
                continue
            mname = m['name']

            # Check for name collision — rename if needed
            if mname in existing_names:
                mname = f"m_{mname}"
            if mname in existing_names:
                mname = f"measure_{m['name']}"

            print(f"  Insertando compuesta: {mname}")

            block = _build_bare_measure_yaml(mname, m['sql_expr'], m.get('display_name'))
            if "measures:" not in current_yaml:
                test_yaml = current_yaml.rstrip() + "\nmeasures:\n" + block + "\n"
            else:
                test_yaml = current_yaml.rstrip() + "\n" + block + "\n"

            try:
                spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{test_yaml}\n$$""")
                current_yaml = test_yaml
                existing_names.add(mname)
                orig_name = m.get('original_name', m.get('display_name', mname))
                print(f"    OK")
                all_results.append({"View": view_name, "Original": orig_name, "Measure": mname, "Status": "OK", "DAX": composed_dax_lookup.get(str(orig_name).lower().strip(), "")})
            except Exception as e:
                error_msg = str(e)[:500]
                orig_name = m.get('original_name', m.get('display_name', mname))
                # One more try with more aggressive rename
                if 'names must be unique' in error_msg:
                    mname = f"composed_{m['name']}"
                    block = _build_bare_measure_yaml(mname, m['sql_expr'], m.get('display_name'))
                    test_yaml = current_yaml.rstrip() + "\n" + block + "\n"
                    try:
                        spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{test_yaml}\n$$""")
                        current_yaml = test_yaml
                        existing_names.add(mname)
                        print(f"    OK (renamed to {mname})")
                        all_results.append({"View": view_name, "Original": orig_name, "Measure": mname, "Status": "OK", "DAX": composed_dax_lookup.get(str(orig_name).lower().strip(), "")})
                        continue
                    except:
                        pass
                print(f"    x {error_msg[:200]}")
                all_results.append({"View": view_name, "Original": orig_name, "Measure": mname, "Status": "SKIP", "DAX": composed_dax_lookup.get(str(orig_name).lower().strip(), "")})

        mv_views[base_table]['yaml'] = current_yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Tercera pasada: measures fallidas con contexto de dimensions

# COMMAND ----------

# Recolectar measures que aun estan SKIP (no resueltas por la segunda pasada)
already_ok = set(r['Original'] for r in all_results if r['Status'] == 'OK')
failed_measures_by_view = defaultdict(list)
for base_table, info in mv_views.items():
    view_name = info['view_name']
    for r in all_results:
        if r['View'] == view_name and r['Status'] == 'SKIP' and r.get('Original', '') not in already_ok:
            # Buscar el DAX original
            for m in info.get('measures', measures_by_view.get(base_table, [])):
                if isinstance(m, dict) and m.get('measure_name') == r['Measure']:
                    failed_measures_by_view[view_name].append({
                        'measure_name': m['measure_name'],
                        'dax': m['dax'],
                        'base_table': base_table,
                    })
                    break

total_failed = sum(len(v) for v in failed_measures_by_view.values())
print(f"Measures fallidas para tercera pasada: {total_failed}")

if total_failed > 0:
    for view_name, failed_list in failed_measures_by_view.items():
        # Encontrar base_table y current_yaml
        base_table = None
        for bt, info in mv_views.items():
            if info['view_name'] == view_name:
                base_table = bt
                break
        if not base_table:
            continue

        current_yaml = mv_views[base_table]['yaml']

        # Extraer dimensions del YAML actual
        dims = []
        in_dims = False
        cur = {}
        for line in current_yaml.split('\n'):
            stripped = line.strip()
            if stripped == 'dimensions:' or stripped.startswith('dimensions:'):
                in_dims = True
                continue
            if in_dims:
                if stripped.startswith('- name:'):
                    if cur:
                        dims.append(cur)
                    cur = {'name': stripped.replace('- name:', '').strip().strip("'\""), 'expr': ''}
                elif stripped.startswith('expr:') and cur:
                    cur['expr'] = stripped.replace('expr:', '').strip().strip("'\"")
                elif stripped and not line.startswith(' ') and not line.startswith('\t') and ':' in stripped:
                    if cur:
                        dims.append(cur)
                        cur = {}
                    in_dims = False
        if cur:
            dims.append(cur)

        dims_text = "\n".join(f"  - {d['name']}: {d['expr']}" for d in dims)

        # Measures ya creadas en esta view
        existing_in_view = [r['Measure'] for r in all_results if r['View'] == view_name and r['Status'] == 'OK']

        # Existing names for collision detection
        existing_names = set()
        for line in current_yaml.split('\n'):
            stripped = line.strip()
            if stripped.startswith('- name:'):
                existing_names.add(stripped.replace('- name:', '').strip().strip("'\""))

        print(f"\n{'='*60}")
        print(f"{view_name}: {len(failed_list)} measures fallidas, reintentando con contexto de dimensions")
        print(f"  Dimensions disponibles: {len(dims)}")
        print(f"  Measures existentes: {len(existing_in_view)}")
        print(f"{'='*60}")

        for fm in failed_list:
            measure_name = fm['measure_name']
            dax = fm['dax']

            print(f"\n  RETRY: {measure_name}")

            retry_prompt = f"""Convert this DAX measure to SQL for a Databricks Metrics View.
This measure FAILED in a previous attempt. Use the dimensions and measures below to make it work.

Measure: {measure_name}
DAX: {dax}

ALL DIMENSIONS available in this Metric View — use ANY_VALUE(name) to reference pre-computed values:
{dims_text}

ALL MEASURES already created — use MEASURE(name) to reference:
{json.dumps(existing_in_view)}

INSTRUCTIONS:
- Match the DAX logic to existing dimensions and measures listed above.
- For RANKX/TOPN/window patterns: find the matching pre-computed dimension and use ANY_VALUE(dimension_name).
- For columns from joined tables: use the dimension name (e.g., if dimension "join_segmento" has expr "join.SEGMENTO", use join_segmento in aggregations).
- For ratios/percentages: use MEASURE(numerator) / NULLIF(MEASURE(denominator), 0).
- Every column reference MUST be inside an aggregate (SUM, COUNT, ANY_VALUE, etc.) or MEASURE().
- Apply the LOD patterns from the Metric View documentation (Fixed LOD, Coarser LOD, Window Measures) as needed.

Return ONLY: {{"name": "snake_case_name", "sql_expr": "expression", "display_name": "Human Name"}}
Or if truly impossible: {{"skip": true, "reason": "explanation"}}"""

            result = call_claude(retry_prompt, max_tokens=2000)
            m_json = _parse_json_object(result)
            if not m_json:
                print(f"    x Error parsing en RETRY. Respuesta cruda de Claude:")
                print(f"    ---\n{result[:2000]}\n    ---")
                continue

            if m_json.get('skip'):
                print(f"    SKIP: {m_json.get('reason', '')[:100]}")
                continue

            mname = m_json.get('name', measure_name.lower().replace(' ', '_'))
            sql_expr = m_json.get('sql_expr', '')

            if mname in existing_names:
                mname = f"r_{mname}"
            if mname in existing_names:
                mname = f"retry_{measure_name.lower().replace(' ', '_')}"

            block = _build_bare_measure_yaml(mname, sql_expr, m_json.get('display_name', measure_name))
            if "measures:" not in current_yaml:
                test_yaml = current_yaml.rstrip() + "\nmeasures:\n" + block + "\n"
            else:
                test_yaml = current_yaml.rstrip() + "\n" + block + "\n"

            try:
                spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{test_yaml}\n$$""")
                current_yaml = test_yaml
                existing_names.add(mname)
                existing_in_view.append(mname)
                # Update result from SKIP to OK
                for r in all_results:
                    if r['View'] == view_name and r['Measure'] == measure_name and r['Status'] == 'SKIP':
                        r['Status'] = 'OK'
                        r['Measure'] = mname
                        break
                print(f"    OK {mname}")
            except Exception as e:
                err = str(e)[:1000]
                print(f"    x {err}")

        mv_views[base_table]['yaml'] = current_yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resultados

# COMMAND ----------

results_df = pd.DataFrame(all_results)

if not results_df.empty:
    total = len(results_df)
    ok = len(results_df[results_df['Status'] == 'OK'])
    skip = len(results_df[results_df['Status'] == 'SKIP'])

    print(f"=== COBERTURA DE MEASURES ===")
    print(f"Total procesadas:  {total}")
    print(f"Creadas OK:        {ok} ({100*ok/total:.0f}%)")
    print(f"Saltadas:          {skip} ({100*skip/total:.0f}%)")
    print(f"Total en .pbix:    {len(measures_df)}")
    print(f"No procesadas:     {len(measures_df) - total}")

    print(f"\nPor view:")
    for view, group in results_df.groupby('View'):
        ok_v = len(group[group['Status'] == 'OK'])
        print(f"  {view}: {ok_v}/{len(group)} OK")

    # Reordenar columnas: Original primero para facilitar mapeo
    col_order = ['View', 'Original', 'Measure', 'Status']
    if 'DAX' in results_df.columns and 'DAX' not in col_order:
        col_order = col_order + ['DAX']
    results_df = results_df[[c for c in col_order if c in results_df.columns]]
    display(results_df)

    # Guardar cobertura
    spark.sql(f"drop table if exists {CATALOG}.{SCHEMA}.{_t('pbi_measure_coverage')}")
    spark.createDataFrame(results_df.astype(str)).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{_t('pbi_measure_coverage')}")
    print(f"\nOK {CATALOG}.{SCHEMA}.{_t('pbi_measure_coverage')}")
else:
    print("No hay resultados")
