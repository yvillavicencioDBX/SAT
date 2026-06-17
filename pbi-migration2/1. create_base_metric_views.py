# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Crear Metric Views base (source + joins + dimensions + LOD)
# MAGIC
# MAGIC Lee las tablas `pbi_*` del notebook 0 y para cada tabla base:
# MAGIC 1. Agrupa measures por tabla
# MAGIC 2. Detecta measures que necesitan Fixed LOD (window functions) y crea vistas pre-calculadas
# MAGIC 3. Claude genera el YAML base (source + joins + dimensions, sin measures)
# MAGIC 4. Intenta crear la Metric View sin measures para validar

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros

# COMMAND ----------

import pandas as pd
import json, re, requests, sys, unicodedata


def _ascii_snake(s):
    """Normaliza a snake_case ASCII puro: 'proyección_futuro' -> 'proyeccion_futuro'.
    Trata 'ñ' como 'ni' (Año -> anio) para coincidir con la transliteración natural
    en español y la convención de Claude."""
    if not s:
        return 'col'
    pre = str(s).replace('ñ', 'ni').replace('Ñ', 'NI')
    nfd = unicodedata.normalize('NFD', pre)
    ascii_s = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    snake = re.sub(r'[^a-zA-Z0-9_]+', '_', ascii_s).strip('_').lower()
    return snake or 'col'


def _sanitize_metric_view_yaml(yaml_text):
    """Quita dimensions/measures sin 'name' o sin 'expr'. Devuelve (yaml_limpio, dropped_list).

    SOLO aplica a items dentro de las secciones `dimensions:` y `measures:`.
    Los items de `joins:` NUNCA se dropean (tienen `source`/`on`, no `expr`).
    """
    lines = yaml_text.split('\n')
    out = []
    dropped = []
    i = 0
    n = len(lines)
    current_section = None   # 'dimensions' | 'measures' | 'joins' | None
    while i < n:
        line = lines[i]
        stripped = line.strip()

        # Detectar inicio de sección a nivel raíz (sin indentación)
        if line and not line[0].isspace():
            if stripped.startswith('dimensions:'):
                current_section = 'dimensions'
            elif stripped.startswith('measures:'):
                current_section = 'measures'
            elif stripped.startswith('joins:'):
                current_section = 'joins'
            elif stripped.endswith(':') and not stripped.startswith('#'):
                # Otra clave raíz (version, source, etc.)
                current_section = None

        # Detectar inicio de item con `- name:` SOLO si estamos en dim/measures
        m = re.match(r'^(\s*)- name\s*:\s*(.+?)\s*$', line)
        if m and current_section in ('dimensions', 'measures'):
            indent = m.group(1)
            item_lines = [line]
            i += 1
            item_indent_min = len(indent) + 2
            while i < n:
                ln = lines[i]
                if not ln.strip():
                    item_lines.append(ln); i += 1; continue
                if re.match(r'^\s*-\s', ln) and (len(ln) - len(ln.lstrip())) <= len(indent):
                    break
                if (len(ln) - len(ln.lstrip())) < item_indent_min and ln.strip():
                    break
                item_lines.append(ln); i += 1
            has_expr = any(re.match(r'^\s+expr\s*:\s*\S', l) for l in item_lines)
            name_val = m.group(2).strip().strip("'\"")
            if has_expr:
                out.extend(item_lines)
            else:
                dropped.append(name_val)
        else:
            out.append(line)
            i += 1
    return '\n'.join(out), dropped


def _postprocess_yaml_defensively(yaml_text, source_types_by_col=None):
    """Post-procesa el YAML antes del CREATE VIEW para arreglar errores comunes
    determinísticamente. Reduce reintentos contra Claude.

    Aplica:
    1. Cierra display_name: "..." sin cerrar al final de línea (truncamiento de Claude)
    2. Envuelve identificadores no-ASCII con backticks en `expr:` y similares
    3. Elimina `format: { type: date }` de dimensions cuyo expr es una columna STRING
    """
    if not yaml_text:
        return yaml_text
    lines = yaml_text.split('\n')
    out = []

    # 1. Arreglar comillas dobles no cerradas en display_name/comment/etc.
    fixed_quotes = 0
    for ln in lines:
        # Detectar `<key>: "<contenido_sin_cierre>` al final de la línea
        m = re.match(r'^(\s*(?:display_name|comment|description|title|format)\s*:\s*)"([^"]*?)$', ln)
        if m and not ln.rstrip().endswith('"'):
            new_ln = f'{m.group(1)}"{m.group(2)}"'
            out.append(new_ln)
            fixed_quotes += 1
            continue
        out.append(ln)
    if fixed_quotes:
        print(f"  [postprocess] cerradas {fixed_quotes} comillas en display_name/comment")

    yaml_text = '\n'.join(out)

    # 2. Envolver identificadores no-ASCII con backticks en `expr:` lines
    fixed_idents = 0
    out2 = []
    for ln in yaml_text.split('\n'):
        m = re.match(r'^(\s*expr\s*:\s*)(.+?)\s*$', ln)
        if not m:
            out2.append(ln)
            continue
        prefix, expr_val = m.group(1), m.group(2)
        # Si el valor está totalmente entre comillas, no tocar
        if (expr_val.startswith('"') and expr_val.endswith('"')) or \
           (expr_val.startswith("'") and expr_val.endswith("'")):
            out2.append(ln)
            continue
        # Buscar tokens identificadores con caracteres no-ASCII y envolverlos
        def _wrap_non_ascii(mt):
            tok = mt.group(0)
            # Si ya está entre backticks, dejar
            if tok.startswith('`') and tok.endswith('`'):
                return tok
            # Si tiene cualquier char no-ASCII letter/digit/underscore (incluye acentos, ñ, etc.)
            if re.search(r'[^\x00-\x7F]', tok):
                return f'`{tok}`'
            return tok
        # Token: secuencia de chars que pueden formar un identificador (incl. unicode)
        # Excluyendo cosas dentro de strings.
        # Heurística simple: aplicar a palabras separadas por whitespace/operators y
        # detectar las que contienen no-ASCII.
        new_expr = re.sub(r'(?<!`)\b[\wÀ-ſ]+\b(?!`)', _wrap_non_ascii, expr_val)
        if new_expr != expr_val:
            fixed_idents += 1
            out2.append(prefix + new_expr)
        else:
            out2.append(ln)
    if fixed_idents:
        print(f"  [postprocess] envueltos {fixed_idents} identificadores no-ASCII con backticks")

    yaml_text = '\n'.join(out2)

    # 2a. Renombrar joins llamados `source` (palabra reservada que colisiona con
    # el alias automático que Lakeview asigna a la tabla source del MV).
    # El error típico: `source.X is ambiguous, could be source.X, source.X`.
    src_join_renamed = 0
    new_lines = []
    for ln in yaml_text.split('\n'):
        # Detectar `- name: source` o `- name: 'source'` adentro de un bloque joins
        m = re.match(r'^(\s*- name\s*:\s*)["\']?source["\']?\s*$', ln, re.IGNORECASE)
        if m:
            new_lines.append(f"{m.group(1)}source_join")
            src_join_renamed += 1
        else:
            new_lines.append(ln)
    if src_join_renamed:
        print(f"  [postprocess] renombrados {src_join_renamed} joins 'source' → 'source_join'")
    yaml_text = '\n'.join(new_lines)

    # 2b. Deduplicar dimensions/measures con el mismo `name` (deja la 1ra ocurrencia)
    deduped_lines = []
    seen_names = {'dimensions': set(), 'measures': set()}
    current_section = None
    in_lines = yaml_text.split('\n')
    i = 0
    n = len(in_lines)
    duped = 0
    while i < n:
        line = in_lines[i]
        stripped = line.strip()
        if line and not line[0].isspace():
            if stripped.startswith('dimensions:'):
                current_section = 'dimensions'
            elif stripped.startswith('measures:'):
                current_section = 'measures'
            else:
                current_section = None
        m = re.match(r'^(\s*)- name\s*:\s*(.+?)\s*$', line)
        if m and current_section in ('dimensions', 'measures'):
            indent = m.group(1)
            name_val = m.group(2).strip().strip("'\"`")
            # Recoger el item completo
            item_lines = [line]
            j = i + 1
            item_indent_min = len(indent) + 2
            while j < n:
                ll = in_lines[j]
                if not ll.strip():
                    item_lines.append(ll); j += 1; continue
                if re.match(r'^\s*-\s', ll) and (len(ll) - len(ll.lstrip())) <= len(indent):
                    break
                if (len(ll) - len(ll.lstrip())) < item_indent_min and ll.strip():
                    break
                item_lines.append(ll); j += 1
            if name_val.lower() in seen_names[current_section]:
                duped += 1  # skip esta repetición
            else:
                seen_names[current_section].add(name_val.lower())
                deduped_lines.extend(item_lines)
            i = j
        else:
            deduped_lines.append(line)
            i += 1
    if duped:
        print(f"  [postprocess] deduplicados {duped} items con `name` repetido")
    yaml_text = '\n'.join(deduped_lines)

    # 3. Eliminar `format: type:date` de dims cuya expr apunta a una columna STRING
    if source_types_by_col:
        # Construir índice case-insensitive
        norm_types = {c.lower(): t for c, t in source_types_by_col.items()}
        # Parse simple: por cada bloque "- name: ... expr: ... format: ...", chequear
        new_lines = []
        i = 0
        ll = yaml_text.split('\n')
        n = len(ll)
        in_dimensions = False
        format_dropped = 0
        while i < n:
            line = ll[i]
            stripped = line.strip()
            if line and not line[0].isspace():
                in_dimensions = stripped.startswith('dimensions:')
            # Detectar inicio de un dim item
            m = re.match(r'^(\s*)- name\s*:\s*([\w`]+)', line)
            if m and in_dimensions:
                indent = m.group(1)
                item_lines = [line]
                j = i + 1
                while j < n:
                    ll2 = ll[j]
                    if not ll2.strip():
                        item_lines.append(ll2); j += 1; continue
                    if re.match(r'^\s*-\s', ll2) and (len(ll2) - len(ll2.lstrip())) <= len(indent):
                        break
                    if (len(ll2) - len(ll2.lstrip())) < len(indent) + 2 and ll2.strip():
                        break
                    item_lines.append(ll2); j += 1

                # Buscar expr y format dentro del item
                expr_val = None
                has_format_date = False
                format_idx_range = None
                for k, il in enumerate(item_lines):
                    em = re.match(r'^\s+expr\s*:\s*(.+?)\s*$', il)
                    if em:
                        expr_val = em.group(1).strip().strip("'\"`")
                    if re.search(r'format\s*:', il) and 'date' in il.lower():
                        # Capturar bloque format
                        format_idx_range = [k]
                        # Si format es inline { type: date } toma 1 línea
                        if '{' in il and '}' in il:
                            has_format_date = True
                        else:
                            # Multi-line format block: capturar líneas hasta dedent
                            kk = k + 1
                            f_indent = len(il) - len(il.lstrip()) + 2
                            while kk < len(item_lines):
                                ill = item_lines[kk]
                                if ill.strip() == '': break
                                if (len(ill) - len(ill.lstrip())) < f_indent: break
                                if 'date' in ill.lower():
                                    has_format_date = True
                                format_idx_range.append(kk)
                                kk += 1

                # Si la columna referenciada por expr es STRING, quitar el format
                if has_format_date and expr_val:
                    # Sacar la columna real de la expr (puede ser `col` o table.col o solo col)
                    # Tomar el último identificador
                    last_id_match = re.search(r'([\wÀ-ſ]+)$', expr_val.replace('`', ''))
                    if last_id_match:
                        last_col = last_id_match.group(1).lower()
                        col_type = norm_types.get(last_col, '')
                        if 'string' in col_type.lower():
                            # Eliminar líneas del format block
                            item_lines = [il for k2, il in enumerate(item_lines) if k2 not in format_idx_range]
                            format_dropped += 1

                new_lines.extend(item_lines)
                i = j
            else:
                new_lines.append(line)
                i += 1
        if format_dropped:
            print(f"  [postprocess] eliminados {format_dropped} format:type:date de dims STRING")
        yaml_text = '\n'.join(new_lines)

    return yaml_text


from collections import defaultdict

# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("pbix_path", "/Volumes/migracion_pbix/default/pbix/KPI_coach_digital.pbix", "Path del .pbix")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("catalog", "migracion_pbix", "Catalogo destino")
dbutils.widgets.text("schema", "couch", "Schema destino")
dbutils.widgets.text("data_locations", "", "Ubicaciones de datos (lista catalog.schema separada por coma; vacío=usar destino)")
dbutils.widgets.text("module_path", "", "Path modulos (vacío = derivar del usuario actual)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4", "Endpoint LLM")

pbix_path = dbutils.widgets.get("pbix_path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATA_LOCATIONS_RAW = dbutils.widgets.get("data_locations").strip()
MODULE_PATH = dbutils.widgets.get("module_path").strip() or f"/Workspace/Users/{_CURRENT_USER}/powerbi-model-analyzer"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")


RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
# Lista de (catalog, schema) donde buscar las tablas de datos.
# Si data_locations está vacío, usa el catálogo/schema destino (comportamiento previo).
DATA_LOCATIONS = []
if DATA_LOCATIONS_RAW:
    for loc in DATA_LOCATIONS_RAW.split(','):
        loc = loc.strip()
        if not loc:
            continue
        if '.' not in loc:
            print(f"  ⚠ '{loc}' no es 'catalog.schema', salteando")
            continue
        c, s = loc.split('.', 1)
        DATA_LOCATIONS.append((c.strip(), s.strip()))
if not DATA_LOCATIONS:
    DATA_LOCATIONS = [(CATALOG, SCHEMA)]

print(f"PBIX: {pbix_path}")
print(f"Destino: {CATALOG}.{SCHEMA}")
print(f"Ubicaciones de datos:")
for c, s in DATA_LOCATIONS:
    print(f"  - {c}.{s}")
print(f"LLM: {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer datos del notebook 0 (todo de UC, sin tocar el .pbix)

# COMMAND ----------

# Measures con DAX completo
measures_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_measures')}").toPandas()
print(f"{len(measures_df)} measures")

# Relaciones
try:
    rels_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_relationships')}").toPandas()
    print(f"{len(rels_df)} relaciones")
except:
    rels_df = pd.DataFrame()
    print("0 relaciones")

# Slicers
try:
    page_filters_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_page_filters')}").toPandas()
    print(f"{len(page_filters_df)} slicers")
except:
    page_filters_df = pd.DataFrame()
    print("0 slicers")

# Calculated columns (DAX per-row). Filtramos jerarquias de fecha auto-generadas y DAX vacio.
try:
    calc_cols_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_calculated_columns')}").toPandas()
    auto_date_re = re.compile(r'^(LocalDateTable_|DateTableTemplate_)', re.IGNORECASE)
    before_n = len(calc_cols_df)
    calc_cols_df = calc_cols_df[
        ~calc_cols_df['TableName'].astype(str).str.match(auto_date_re, na=False)
        & calc_cols_df['Expression'].astype(str).str.strip().ne('')
        & calc_cols_df['Expression'].astype(str).str.strip().str.lower().ne('none')
    ].reset_index(drop=True)
    print(f"{len(calc_cols_df)} columnas calculadas (filtradas {before_n - len(calc_cols_df)} auto-date / vacias)")
except Exception as e:
    calc_cols_df = pd.DataFrame()
    print(f"0 columnas calculadas ({str(e)[:100]})")

# Construir measure_pages y slicers_by_page desde pbi_measures
measure_pages = {}
for _, row in measures_df.iterrows():
    name = row.get('Measure', '')
    pages_str = row.get('Paginas_donde_se_usa', '')
    if pages_str and pages_str != '(no usada en visuales)':
        measure_pages[name] = set(p.strip() for p in pages_str.split(','))

slicers_by_page = {}
if not page_filters_df.empty:
    for _, row in page_filters_df.iterrows():
        page = row.get('Pagina', '')
        slicer = row.get('Slicer', '')
        if page not in slicers_by_page:
            slicers_by_page[page] = []
        slicers_by_page[page].append(slicer)

# Tablas de datos en UC (no pbi_* ni mv_*).
# Itera todas las DATA_LOCATIONS (catalog.schema) y construye:
#   existing_tables[tname] = [col_names]
#   existing_table_types[tname] = {col: type}
#   table_to_fqn[tname] = "catalog.schema.tname" (path completo para el source de la MV)
existing_tables = {}
existing_table_types = {}
table_to_fqn = {}

for c_loc, s_loc in DATA_LOCATIONS:
    try:
        rows = spark.sql(f"SHOW TABLES IN {c_loc}.{s_loc}").collect()
    except Exception as e:
        print(f"  ⚠ No se puede listar {c_loc}.{s_loc}: {str(e)[:120]}")
        continue
    for r in rows:
        tname = r.tableName
        if tname.startswith(("pbi_", "mv_", "v_dashboard_", "lod_")):
            continue
        # Si ya está la tabla (de una location previa), no duplicar
        if tname in existing_tables:
            print(f"  ⚠ Tabla '{tname}' duplicada — usando {table_to_fqn[tname]} (saltando {c_loc}.{s_loc}.{tname})")
            continue
        try:
            cols = spark.sql(f"DESCRIBE {c_loc}.{s_loc}.{tname}").collect()
            col_names = [x.col_name for x in cols if not x.col_name.startswith('#')]
            col_types = {x.col_name: x.data_type for x in cols if not x.col_name.startswith('#')}
            existing_tables[tname] = col_names
            existing_table_types[tname] = col_types
            table_to_fqn[tname] = f"{c_loc}.{s_loc}.{tname}"
        except Exception as e:
            print(f"  SKIP {c_loc}.{s_loc}.{tname}: {str(e)[:100]}")

def _fqn(tname):
    """Devuelve el FQN catalog.schema.table para una tabla detectada.
    Si no se conoce, asume que está en el catálogo destino."""
    return table_to_fqn.get(tname, f"{CATALOG}.{SCHEMA}.{tname}")

print(f"\nTablas de datos descubiertas: {len(existing_tables)}")
for tname, cols in existing_tables.items():
    print(f"  {table_to_fqn.get(tname, tname)}: {len(cols)} columnas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Importar docs y construir system prompt

# COMMAND ----------

sys.path.insert(0, MODULE_PATH)
from metrics_view_docs import METRICS_VIEW_DOCS
from dax_function_reference import get_relevant_dax_docs

DAX_DOCS = get_relevant_dax_docs(measures_df)
print(f"DAX docs: {len(DAX_DOCS)} chars ({DAX_DOCS.count('###')} funciones)")

SYSTEM_PROMPT = f"""You are an expert in Databricks Metrics Views YAML.

Here is the COMPLETE official Databricks Metrics View documentation:

{METRICS_VIEW_DOCS}

Here is the DAX function reference for the functions used in this .pbix file:

{DAX_DOCS}

You generate valid Metrics View YAML following the documentation exactly.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. call_claude

# COMMAND ----------

# Modelos que NO aceptan el parámetro 'temperature' (extended thinking).
# Si Databricks agrega más, este set crece — pero el fallback automático abajo también
# lo maneja sin tocar código.
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

    # Fallback: si el modelo rechaza temperature dinámicamente, reintenta sin
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agrupar measures por tabla base

# COMMAND ----------

def detect_base_table(dax_expression, measure_table):
    """Detecta la tabla base de una measure a partir de su DAX."""
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
    excluded = {'true', 'false', 'blank'}
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
    """Find ALL table names referenced in a DAX expression."""
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
        r"CALCULATE\([^,]+,\s*'?([^')\[]+)'?\[",
        r"'([^']+)'\[",
    ]
    excluded = {'true', 'false', 'blank'}
    tables = []
    for pattern in patterns:
        for m in re.finditer(pattern, dax, re.IGNORECASE):
            for g in range(1, m.lastindex + 1 if m.lastindex else 2):
                try:
                    t = m.group(g)
                    if t:
                        t = t.strip()
                        if t.lower() not in excluded and not any(c in t for c in '()=<>'):
                            tables.append(t)
                except:
                    pass
    return tables


def _match_to_uc_table(table_name, existing_tables):
    """Try to match a PBI table name to a UC table name."""
    normalized = table_name.lower().replace(' ', '_').replace("'", "")
    if normalized in existing_tables:
        return normalized
    for tname in existing_tables:
        if normalized.replace('_', '') == tname.replace('_', ''):
            return tname
        if normalized.replace('_', '') in tname.replace('_', '') or tname.replace('_', '') in normalized.replace('_', ''):
            return tname
    return None


def resolve_with_claude_batch(unresolved_measures, available_tables):
    """Send unresolved measures to Claude in a single batch call.
    Returns dict {measure_name: uc_table_name or None}."""
    if not unresolved_measures:
        return {}

    payload = [
        {"name": m["name"], "dax": m["dax"], "measure_table": m["measure_table"]}
        for m in unresolved_measures
    ]

    prompt = f"""You are mapping Power BI DAX measures to base tables in Unity Catalog.

Available UC tables (use EXACT names from this list; if no good match, return null):
{json.dumps(sorted(available_tables), indent=2)}

For each measure below, decide which UC table is its base (the table whose rows/columns it primarily aggregates).
Consider the DAX expression and the measure's home table. Composed measures (referencing other measures via [brackets]) should resolve to the table of the underlying aggregation.

Measures:
{json.dumps(payload, indent=2)}

Return ONLY a valid JSON object mapping each measure name to a UC table name (or null):
{{"measure_name_1": "uc_table_1", "measure_name_2": null, ...}}
No markdown fences, no explanations."""

    try:
        result = call_claude(prompt, max_tokens=4000)
        if "```" in result:
            m = re.search(r'\{.*\}', result, re.DOTALL)
            result = m.group() if m else result
        mapping = json.loads(result)
        # Validate: only return values that are in available_tables
        clean = {}
        avail_set = set(available_tables)
        for k, v in mapping.items():
            if v and v in avail_set:
                clean[k] = v
            else:
                clean[k] = None
        return clean
    except Exception as e:
        print(f"  [WARN] Claude batch resolve failed: {str(e)[:200]}")
        return {m["name"]: None for m in unresolved_measures}


def group_measures_by_table(measures_df, measure_pages, slicers_by_page, existing_tables):
    """Agrupa measures por tabla base detectada desde la DAX.

    Pipeline:
      1. detect_base_table (regex) → si encuentra y matchea UC, listo.
      2. _find_all_table_refs (regex amplia) → busca cualquier tabla en el DAX.
      3. resolve_with_claude_batch → último recurso, una sola llamada para todos.
    """
    measures_by_base = defaultdict(list)
    skipped = []
    method_counts = {"regex": 0, "regex_alt": 0, "llm": 0, "self": 0, "unresolved": 0}

    # Pasada 1: lo que se pueda con regex; lo demás se acumula para Claude.
    resolved = {}            # {measure_name: (base_display, base_normalized, method)}
    unresolved = []          # [{name, dax, measure_table}]
    measure_meta = {}        # {measure_name: (table, dax)}

    for _, row in measures_df.iterrows():
        name = row.get("Measure", row.get("Name", ""))
        table = row.get("Tabla", row.get("TableName", ""))
        dax = str(row.get("DAX", row.get("Expression", "")))
        measure_meta[name] = (table, dax)

        base = detect_base_table(dax, table)
        if base:
            base_normalized = base.lower().replace(' ', '_').replace("'", "")
            if existing_tables:
                if base_normalized not in existing_tables:
                    matched = _match_to_uc_table(base, existing_tables)
                    if matched:
                        resolved[name] = (base, matched, "regex")
                        continue
                    # base extraida pero no existe en UC → buscar otras refs
                    dax_tables = _find_all_table_refs(dax, table)
                    for dt in dax_tables:
                        matched = _match_to_uc_table(dt, existing_tables)
                        if matched:
                            resolved[name] = (dt, matched, "regex_alt")
                            break
                    if name in resolved:
                        continue
                else:
                    resolved[name] = (base, base_normalized, "regex")
                    continue
            else:
                resolved[name] = (base, base_normalized, "regex")
                continue

        # Si llegamos aqui, regex no convergio. A la cola de Claude.
        if name and dax:
            unresolved.append({"name": name, "dax": dax, "measure_table": table})
        elif table:
            # Sin DAX no podemos preguntar nada; usa measure_table como ultimo intento.
            base_normalized = table.lower().replace(' ', '_').replace("'", "")
            matched = base_normalized if base_normalized in existing_tables else _match_to_uc_table(table, existing_tables)
            if matched:
                resolved[name] = (table, matched, "self")
            else:
                skipped.append(name)
        else:
            skipped.append(name)

    # Pasada 2: una sola llamada batched a Claude con los unresolved.
    if unresolved and existing_tables:
        print(f"\n[LLM] resolviendo {len(unresolved)} measures sin match por regex...")
        llm_map = resolve_with_claude_batch(unresolved, list(existing_tables))
        for um in unresolved:
            uc = llm_map.get(um["name"])
            if uc:
                resolved[um["name"]] = (uc, uc, "llm")
                print(f"  [LLM] '{um['name']}' -> '{uc}'")
            else:
                skipped.append(um["name"])
                print(f"  [SKIP] '{um['name']}' — sin tabla resoluble")
    elif unresolved:
        # No tenemos catalogo de UC para validar — usamos measure_table.
        for um in unresolved:
            t = um["measure_table"]
            if t:
                resolved[um["name"]] = (t, t.lower().replace(' ', '_').replace("'", ""), "self")
            else:
                skipped.append(um["name"])

    # Materializar agrupacion
    for name, (base_display, base_normalized, method) in resolved.items():
        method_counts[method] = method_counts.get(method, 0) + 1
        table, dax = measure_meta[name]
        pages = sorted(measure_pages.get(name, set()))
        page_slicer_list = set()
        for p in pages:
            page_slicer_list.update(slicers_by_page.get(p, []))

        measures_by_base[base_normalized].append({
            'measure_name': name,
            'measure_table': table,
            'dax': dax,
            'base_table': base_display,
            'pages': ", ".join(pages) if pages else "(no usada)",
            'page_slicers': ", ".join(sorted(page_slicer_list)) if page_slicer_list else "(sin slicers)",
            'method': method,
        })

    total_grouped = sum(len(v) for v in measures_by_base.values())
    method_counts["unresolved"] = len(skipped)

    print(f"\n=== RESUMEN ===")
    print(f"Total: {len(measures_df)} | Agrupadas: {total_grouped} | Descartadas: {len(skipped)}")
    print(f"Por metodo: regex={method_counts['regex']} | regex_alt={method_counts['regex_alt']} | llm={method_counts['llm']} | self={method_counts['self']} | sin_resolver={method_counts['unresolved']}")
    for base, mlist in measures_by_base.items():
        print(f"  {base}: {len(mlist)} measures")
    if skipped:
        print(f"\nDescartadas (revisar): {skipped[:10]}{'...' if len(skipped) > 10 else ''}")
    # Retornar también el dict de measure_meta (table, dax) para los skipped, y la lista skipped
    return measures_by_base, skipped, measure_meta

# COMMAND ----------

measures_by_base, _skipped_measures, _measure_meta_all = group_measures_by_table(measures_df, measure_pages, slicers_by_page, existing_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Persistir el mapping measure → metric view
# MAGIC
# MAGIC Esta tabla es la fuente de verdad de qué measure va a qué MV. Notebook 2 la lee directo
# MAGIC en vez de re-descubrir. Si una measure quedó sin asignar, aquí queda registrada con
# MAGIC `target_mv=NULL` para que la puedas editar a mano antes de notebook 2.

# COMMAND ----------

_mapping_rows = []
for base_table, mlist in measures_by_base.items():
    # Nombre del MV final (con sufijo si aplica)
    _mv_safe = _t(f"mv_{base_table}".replace(' ', '_').replace("'", ""))
    _target_mv = f"{CATALOG}.{SCHEMA}.{_mv_safe}"
    for m in mlist:
        _mapping_rows.append({
            'pbi_measure_name': m['measure_name'],
            'pbi_table': m.get('measure_table', ''),
            'base_table': base_table,
            'target_mv': _target_mv,
            'dax': m['dax'],
            'assignment_method': m.get('method', '?'),
            'pages': m.get('pages', ''),
            'page_slicers': m.get('page_slicers', ''),
        })

# Skipped measures (sin tabla resoluble) — quedan con target_mv vacío para revisión manual
for _skipped_name in _skipped_measures:
    _t_pbi, _dax_pbi = _measure_meta_all.get(_skipped_name, ('', ''))
    _mapping_rows.append({
        'pbi_measure_name': _skipped_name,
        'pbi_table': _t_pbi,
        'base_table': '',
        'target_mv': '',
        'dax': _dax_pbi,
        'assignment_method': 'unresolved',
        'pages': '',
        'page_slicers': '',
    })

if _mapping_rows:
    _mapping_df = spark.createDataFrame(_mapping_rows)
    _mapping_fqn = f"{CATALOG}.{SCHEMA}.{_t('measure_to_view_mapping')}"
    _mapping_df.write.mode("overwrite").saveAsTable(_mapping_fqn)
    print(f"Guardado {len(_mapping_rows)} filas en {_mapping_fqn}")
    print(f"  Con target_mv: {sum(1 for r in _mapping_rows if r['target_mv'])}")
    print(f"  Sin target (revisar manualmente): {sum(1 for r in _mapping_rows if not r['target_mv'])}")
else:
    print("(no hay measures para mapear)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Detectar measures que necesitan Fixed LOD

# COMMAND ----------

# Detección de measures que necesitan Fixed LOD via Claude.
# Reemplaza la heurística por regex (limitada a 5 patrones) por una clasificación
# semántica que entiende casos como: ALLEXCEPT, REMOVEFILTERS, SUMX con tabla virtual
# (ADDCOLUMNS/SUMMARIZE), CALCULATE con cambio de grano, time intelligence avanzado, etc.

LOD_CLASSIFY_PROMPT = """You are an expert in translating PowerBI DAX to Databricks Metric Views.

Classify EACH measure below: does it need a "Fixed LOD" pre-computation (a view with
window functions or pre-aggregated columns) BEFORE the Metric View, or can it be
expressed as a regular Metric View measure (sum/avg + window/filter)?

A measure NEEDS Fixed LOD when its DAX implies any of:
  - ranking/ordering across rows: RANKX, TOPN, ROW_NUMBER-style logic
  - row-level back-reference: EARLIER, EARLIEST
  - percentiles/medians on a row-iterator: PERCENTILEX, MEDIANX
  - SUMX/AVERAGEX/MAXX/MINX over a virtual table created with ADDCOLUMNS/SUMMARIZE/SELECTCOLUMNS
    (because the iterator's grain differs from the source row grain)
  - CALCULATE that REMOVES filters via ALL/ALLEXCEPT/REMOVEFILTERS to compute a denominator
    at a coarser grain than the row (typical "% of grand total", "% of category")
  - GROUPBY, SUMMARIZECOLUMNS (DAX produces a grouped table)
  - time intelligence that requires a custom date window not expressible as a Metric View
    `window` (e.g., PARALLELPERIOD with offset, DATESINPERIOD with arbitrary length, custom YTD)
  - any expression where the natural SQL translation requires window functions over a partition
    that the Metric View framework cannot express directly

A measure does NOT need Fixed LOD (i.e., is "regular") when:
  - Plain SUM/AVG/MIN/MAX/COUNT/DISTINCTCOUNT
  - CALCULATE with a simple FILTER on the same grain (translates to Metric View measure FILTER)
  - Standard time-intelligence (DATEADD ±N, SAMEPERIODLASTYEAR, TOTALYTD/TOTALMTD/TOTALQTD)
    — Metric Views handle these via `window` clause
  - DIVIDE/ratio of two atomic measures
  - CASE WHEN logic at row level

For each measure return one JSON object:
{
  "name": "<measure_name>",
  "needs_lod": true/false,
  "reason": "<short explanation>",
  "lod_columns": [    // ONLY if needs_lod=true; SQL expressions to pre-compute as columns of a LOD view
    {
      "column_name": "snake_case_lod_col",
      "column_sql": "<SQL expression using only the source table; no MEASURE() refs>",
      "comment": "what it represents"
    }
  ]
}

Output ONLY a JSON array. No markdown fences. No explanation outside the JSON.

Measures to classify:
"""

def _parse_claude_lod_response(clean: str, batch_idx: int = 0) -> list:
    """Parsea la respuesta de Claude. Maneja truncamiento y JSON malformado."""
    # Quitar fences ```json ... ```
    if clean.startswith("```"):
        lines = clean.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        clean = "\n".join(lines)

    # Intento 1 — parse directo
    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        last_char = clean.rstrip()[-1:] if clean.rstrip() else ''
        truncated = last_char not in (']', '}')
        print(
            f"  ⚠ batch {batch_idx}: parse falló en char {e.pos} (línea {e.lineno}). "
            f"Termina en {last_char!r}. {'TRUNCADO' if truncated else 'malformado'}."
        )

    # Intento 2 — extraer array embebido con regex
    m = re.search(r'\[.*\]', clean, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    # Intento 3 — reparar truncamiento cerrando objetos/arrays abiertos
    repaired = clean.rstrip().rstrip(',')
    open_brace = repaired.count('{') - repaired.count('}')
    open_brack = repaired.count('[') - repaired.count(']')
    # Si el último elemento quedó incompleto, recortar hasta el último } válido
    if open_brace > 0:
        last_close = repaired.rfind('}')
        if last_close > 0:
            repaired = repaired[:last_close + 1]
            open_brace = repaired.count('{') - repaired.count('}')
            open_brack = repaired.count('[') - repaired.count(']')
    repaired += '}' * open_brace + ']' * open_brack
    try:
        items = json.loads(repaired)
        print(f"  ✓ batch {batch_idx}: recuperado tras truncamiento ({len(items)} items)")
        return items
    except json.JSONDecodeError as e:
        print(f"  ✗ batch {batch_idx}: irreparable. Últimos 200 chars: {clean[-200:]!r}")
        return []


def _classify_lod_batch(measures_list: list, batch_idx: int = 0) -> list:
    """Clasifica un batch de measures. Devuelve lista de dicts."""
    payload_lines = []
    for m in measures_list:
        payload_lines.append(
            f"- name: {m['measure_name']}\n  base_table: {m.get('base_table','?')}\n  dax: {m['dax']}"
        )
    prompt = LOD_CLASSIFY_PROMPT + "\n".join(payload_lines)
    result = call_claude(prompt, max_tokens=8000)
    return _parse_claude_lod_response(result.strip(), batch_idx)


def classify_lod_with_claude(measures_list, batch_size: int = 10):
    """Devuelve {measure_name: {needs_lod, reason, lod_columns}}.

    Procesa en batches de `batch_size` measures para evitar truncamiento de
    respuesta cuando hay muchas measures. Con DAX largo, 10 measures por batch
    típicamente generan ~3-5 KB de JSON, muy por debajo del límite de tokens.
    """
    if not measures_list:
        return {}

    n = len(measures_list)
    n_batches = (n + batch_size - 1) // batch_size
    print(f"Clasificando {n} measures en {n_batches} batches de {batch_size}…")

    all_items = []
    for i in range(0, n, batch_size):
        batch = measures_list[i:i + batch_size]
        idx = i // batch_size + 1
        print(f"  Batch {idx}/{n_batches} ({len(batch)} measures)…", end=" ")
        items = _classify_lod_batch(batch, batch_idx=idx)
        print(f"{len(items)} clasificadas")
        all_items.extend(items)

    return {it['name']: it for it in all_items if isinstance(it, dict) and it.get('name')}

# Recolectar todas las measures (con su base_table) y clasificar en una sola llamada
all_measures_for_lod = []
for base_table, mlist in measures_by_base.items():
    for m in mlist:
        all_measures_for_lod.append({**m, 'base_table': base_table})

print(f"Clasificando {len(all_measures_for_lod)} measures con Claude para detectar LOD…")
lod_classification = classify_lod_with_claude(all_measures_for_lod)
print(f"  Respuestas: {len(lod_classification)}")

lod_candidates = {}  # {base_table: [measures que necesitan LOD]}
for base_table, mlist in measures_by_base.items():
    cands = []
    for m in mlist:
        cls = lod_classification.get(m['measure_name'])
        if cls and cls.get('needs_lod'):
            cands.append({
                **m,
                'lod_reasons': [cls.get('reason', 'needs LOD')],
                'lod_columns_suggested': cls.get('lod_columns', []),
            })
    if cands:
        lod_candidates[base_table] = cands

print(f"\nTablas con measures LOD: {len(lod_candidates)}")
for bt, cands in lod_candidates.items():
    print(f"  {bt}: {len(cands)} measures")
    for c in cands:
        print(f"    - {c['measure_name']}: {c['lod_reasons'][0][:120]}")
        print(f"      DAX: {c['dax'][:100]}")

if not lod_candidates:
    print("Ninguna measure requiere Fixed LOD — todas se pueden resolver con measures regulares o window measures.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5c. Crear vistas LOD con columnas pre-calculadas

# COMMAND ----------

lod_info = {}  # {base_table: {'view_name': str, 'lod_columns': [{'column_name', 'column_sql', 'for_measure', 'comment'}]}}

for base_table, candidates in lod_candidates.items():
    catalog_table = base_table
    if base_table not in existing_tables:
        matched = _match_to_uc_table(base_table, existing_tables)
        if matched:
            catalog_table = matched
        else:
            print(f"  SKIP {base_table}: no existe en UC")
            continue

    source_fqn = _fqn(catalog_table)
    cols = existing_tables.get(catalog_table, [])
    col_types = existing_table_types.get(catalog_table, {})
    cols_detail = "\n".join(f"  - {c}: {col_types.get(c, 'unknown')}" for c in cols)

    # Relaciones para contexto
    related_info = ""
    base_orig = candidates[0].get('base_table', '') if candidates else ''
    if hasattr(rels_df, 'iterrows'):
        for _, rel in rels_df.iterrows():
            from_t = str(rel.get('FromTableName', ''))
            to_t = str(rel.get('ToTableName', ''))
            from_c = str(rel.get('FromColumnName', ''))
            to_c = str(rel.get('ToColumnName', ''))
            if from_t == base_orig or to_t == base_orig:
                related_pbi = to_t if from_t == base_orig else from_t
                rt_match = _match_to_uc_table(related_pbi, existing_tables)
                if rt_match:
                    rt_cols = existing_tables.get(rt_match, [])
                    rt_types = existing_table_types.get(rt_match, {})
                    rt_detail = ", ".join(f"{c}({rt_types.get(c,'')})" for c in rt_cols[:15])
                    related_info += f"\n  Relationship: {from_t}[{from_c}] -> {to_t}[{to_c}]"
                    related_info += f"\n  UC table: {_fqn(rt_match)} -- Columns: {rt_detail}"

    measures_info = "\n".join(f"- {c['measure_name']}: {c['dax']}" for c in candidates)

    # Reusar las columnas LOD ya sugeridas por Claude en la clasificación de 5b
    # (evita una segunda llamada redundante).
    suggested_lines = []
    for c in candidates:
        for sug in (c.get('lod_columns_suggested') or []):
            cn = sug.get('column_name', '')
            cs = sug.get('column_sql', '')
            if cn and cs:
                suggested_lines.append(f"  - column_name: {cn}\n    column_sql: {cs}\n    for_measure: {c['measure_name']}")
    suggested_block = "\n".join(suggested_lines) if suggested_lines else "  (none — generate from scratch)"

    prompt = f"""These DAX measures need pre-computed LOD columns (window functions, sub-aggregations, etc.).
Generate the SQL columns to add to a LOD view that wraps the source.

The LOD view will be: `CREATE VIEW lod_<table> AS SELECT src.*, <your_columns> FROM {source_fqn} src`

Source table: {source_fqn}
Columns AVAILABLE in `src`:
{cols_detail}
{f"Relationships (FYI ONLY — DO NOT use these in column_sql; LOD operates on the source table directly):{related_info}" if related_info else ""}

DAX measures that need LOD pre-computation:
{measures_info}

Pre-classification already provided these LOD column suggestions (USE/REFINE these if valid):
{suggested_block}

CRITICAL constraints:
- column_sql can ONLY reference columns from the source table listed above (`src.<col>` or just `<col>`).
- DO NOT reference joined dimension tables (e.g., `dt.date`, `dim_tiempo.year`). Those tables are not joined in the LOD view.
- If a measure's DAX references a column from a related dim table (e.g., `'Dim Tiempo'[date]`), find the corresponding FK column in the source's "Columns" list above and use that. The source typically has a foreign key with the same semantic meaning (a date column, an id column, etc.). Pick whichever column from the source list matches.
- Use `src.` prefix for clarity (e.g., `src.<source_column>`).

Return a JSON array of LOD columns to add:
[
  {{
    "column_name": "unique_snake_case_name",
    "column_sql": "RANK() OVER (ORDER BY src.col DESC)",
    "for_measure": "original measure name",
    "comment": "brief explanation"
  }}
]

Rules:
- column_sql must be valid Databricks SQL (window function with OVER clause, sub-aggregation, etc.)
- Use ONLY columns from the source table listed above
- If multiple measures can share the same column, generate it once
- column_name must be snake_case ASCII (no accents, no spaces)
- Return ONLY the JSON array, NOTHING ELSE — no markdown fences, no commentary, no trailing text after the closing `]`
- Return empty array [] if none actually need LOD after analysis"""

    print(f"\n--- {base_table}: analizando {len(candidates)} measures LOD ---")

    def _parse_json_array(txt):
        """Parser robusto: encuentra el primer ARRAY de objetos válido en el texto.
        Tolera fences ```...```, texto antes/después, y arrays falsos como [m1] in
        prose. Usa json.JSONDecoder.raw_decode que ignora trailing data."""
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
                # Solo aceptar si es un array de objetos (lo que esperamos)
                if isinstance(obj, list):
                    return obj
            except json.JSONDecodeError:
                pass
            i = idx + 1
        return []

    try:
        result = call_claude(prompt, max_tokens=2000)
        lod_columns = _parse_json_array(result)
    except Exception as e:
        print(f"  Error parsing LOD columns: {str(e)[:150]}")
        # Fallback: usar directamente las suggested de la clasificación 5b
        fallback = []
        for c in candidates:
            for sug in (c.get('lod_columns_suggested') or []):
                if sug.get('column_name') and sug.get('column_sql'):
                    fallback.append({**sug, 'for_measure': c['measure_name']})
        if fallback:
            print(f"  → fallback: usando {len(fallback)} sugerencias de la clasificación 5b")
            lod_columns = fallback
        else:
            continue

    if not lod_columns:
        print(f"  Sin columnas LOD necesarias tras analisis")
        continue

    # Detectar columnas que ya existen en la source: las VAMOS A REEMPLAZAR.
    # Una calc col cuya forma normalizada (ASCII snake_case lower) colisione con
    # N columnas source debe excluir TODAS las N para evitar ambigüedad.
    def _norm(s): return _ascii_snake(s).lower()
    source_norm_index = {}
    for c in existing_tables.get(catalog_table, []):
        source_norm_index.setdefault(_norm(c), []).append(c)
    cols_to_replace = []
    for lc in lod_columns:
        col_name = lc.get('column_name', '')
        matched = source_norm_index.get(_norm(col_name), [])
        # Almacenar la lista de cols a reemplazar EN EL OBJETO mismo, para que
        # secciones posteriores (5d con calc cols) reapliquen el EXCEPT.
        lc['replaces_source_cols'] = matched
        if matched:
            cols_to_replace.extend(matched)
            print(f"  ↻ {col_name}: reemplazará {matched} en {catalog_table}")
    cols_to_replace = sorted(set(cols_to_replace))

    # Construir y crear la vista LOD
    select_extras = ", ".join(f"{lc['column_sql']} AS {lc['column_name']}" for lc in lod_columns)
    if cols_to_replace:
        excepts = ", ".join(f"`{c}`" for c in cols_to_replace)
        lod_sql = f"SELECT src.* EXCEPT ({excepts}), {select_extras} FROM {source_fqn} src"
    else:
        lod_sql = f"SELECT src.*, {select_extras} FROM {source_fqn} src"
    # _t() debe envolver el nombre completo, no el prefijo: lod_X_<run_id> en lugar de lod__<run_id>X
    lod_view_name = f"{CATALOG}.{SCHEMA}.{_t(f'lod_{catalog_table}')}"

    created = False
    for attempt in range(3):
        try:
            # Reconstruir el SQL desde lod_columns (asegura coherencia tras fix)
            select_extras = ", ".join(f"{lc['column_sql']} AS {lc['column_name']}" for lc in lod_columns)
            if cols_to_replace:
                excepts = ", ".join(f"`{c}`" for c in cols_to_replace)
                lod_sql = f"SELECT src.* EXCEPT ({excepts}), {select_extras} FROM {source_fqn} src"
            else:
                lod_sql = f"SELECT src.*, {select_extras} FROM {source_fqn} src"
            spark.sql(f"CREATE OR REPLACE VIEW {lod_view_name} AS {lod_sql}")
            print(f"  OK {lod_view_name}" + (f" (fix {attempt})" if attempt > 0 else ""))
            for lc in lod_columns:
                print(f"    + {lc['column_name']}: {lc['column_sql'][:80]}")
            lod_info[base_table] = {
                'view_name': lod_view_name,
                'lod_columns': lod_columns,
            }
            created = True
            break
        except Exception as e:
            error_msg = str(e)[:500]
            print(f"  x intento {attempt+1}: {error_msg[:300]}")
            if attempt < 2:
                # Pedir a Claude que devuelva la LISTA de columnas corregida
                # (no el SQL completo — así actualizamos lod_columns coherentemente)
                cols_json = json.dumps(lod_columns, indent=2)
                fix_prompt = f"""Fix the following LOD column definitions. Return the SAME structure (JSON array of objects with column_name, column_sql, for_measure, comment) but with corrected SQL expressions.

Error when creating view `SELECT src.*, ... FROM {source_fqn} src`:
{error_msg}

Source columns available in `src`:
{cols}

Current LOD columns (each has invalid `column_sql`):
{cols_json}

CRITICAL: each `column_sql` MUST reference ONLY `src.<col>` columns listed above.
- USE THE EXACT COLUMN NAME (preserve case as listed). Example: `src.MessageTime` NOT `src.message_time`. Common error: lowercasing CamelCase columns.
- If the error message says "Did you mean: `src`.`MessageTime`", use that EXACT name.
- Do NOT use aliases like `dt.`, `dim_tiempo.`, joined tables — those are NOT available.
- If the original DAX referenced a related dim column (e.g., date from dim_tiempo), substitute with the corresponding source column (e.g., the fact's own foreign-key date column from the list above).
- Keep `column_name`, `for_measure`, `comment` unchanged. Fix only `column_sql`.

Return ONLY a JSON array, no markdown."""
                try:
                    fix_r = call_claude(fix_prompt, max_tokens=2000)
                    new_cols = _parse_json_array(fix_r)
                    if new_cols and isinstance(new_cols, list):
                        # Validar que cada item tenga las claves esperadas
                        valid = []
                        for nc in new_cols:
                            if nc.get('column_name') and nc.get('column_sql'):
                                valid.append({
                                    'column_name': nc['column_name'],
                                    'column_sql': nc['column_sql'],
                                    'for_measure': nc.get('for_measure', ''),
                                    'comment': nc.get('comment', ''),
                                })
                        if valid:
                            lod_columns = valid
                            print(f"  -> fix: {len(valid)} columnas corregidas")
                        else:
                            print(f"  -> fix devolvió array vacío, abortando")
                            break
                    else:
                        print(f"  -> fix no devolvió JSON array válido, abortando")
                        break
                except Exception as e2:
                    print(f"  -> error en fix: {str(e2)[:200]}")
                    break

    if not created:
        print(f"  FAIL: no se pudo crear vista LOD para {base_table}")

print(f"\nVistas LOD creadas: {len(lod_info)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5d-bis. Traducir columnas calculadas (DAX per-row) e inyectarlas en las vistas

# COMMAND ----------

def translate_calc_cols_batch(table_name_pbi, source_fqn, cols_detail, related_info, calc_cols_subset):
    """Translate DAX calc cols to per-row SQL expressions using Claude (batched per table)."""
    if calc_cols_subset.empty:
        return []

    payload = [
        {"pbi_name": str(r['ColumnName']), "dax": str(r['Expression']).strip()}
        for _, r in calc_cols_subset.iterrows()
    ]

    # Sibling calc cols (mismo grupo) — para que Claude pueda inlinear dependencias entre ellas
    sibling_calc_cols = "\n".join(
        f"  - [{r['ColumnName']}]: {str(r['Expression']).strip()[:200]}"
        for _, r in calc_cols_subset.iterrows()
    )

    prompt = f"""Translate Power BI calculated columns (DAX) to per-row SQL expressions for Databricks Spark SQL.

Source table (PBI name): {table_name_pbi}
Source table (UC fully qualified): {source_fqn}
Source columns (used as src.<col>):
{cols_detail}
{related_info if related_info else ""}

OTHER calc cols defined on this same table (you can INLINE these definitions
when a calc col references another in this list — SQL SELECT does not allow
referencing sibling aliases):
{sibling_calc_cols}

Calculated columns to translate:
{json.dumps(payload, indent=2, ensure_ascii=False)}

Rules — each must become a per-row expression valid as: SELECT src.*, <expr> AS <column_name>
- column_name: lowercase snake_case, alphanumeric + underscores, no spaces, no accents
- 'Table'[Col] referring to the source table -> src.<col_snake_lower>
- [Column] without prefix -> src.<col_snake_lower>
- TODAY()/NOW() -> current_date() / current_timestamp()
- UTCNOW() -> current_timestamp()
- IF(cond, a, b) -> CASE WHEN cond THEN a ELSE b END (nest as deep as needed)
- BLANK() -> NULL
- YEAR/MONTH/DAY/WEEKNUM -> year() / month() / day() / weekofyear()
- EOMONTH(d, n) -> last_day(add_months(d, n))
- DATEADD(d, n, DAY) -> date_add(d, n)
- FORMAT(d, "MMMM") -> date_format(d, 'MMMM')
- "&" concat -> concat(...) or ||
- LOOKUPVALUE(target, key1, value1[, key2, value2]) ->
    (SELECT target FROM <related_uc> lk WHERE lk.key1 = src.value1 [AND lk.key2 = src.value2] LIMIT 1)

WINDOW FUNCTIONS — these are first-class in Spark SQL and the canonical translation
for CALCULATE + EARLIER patterns. Do NOT mark these as "complex" — translate them:

- CALCULATE(SUM([x]), FILTER(t, [k] = EARLIER([k])))
    -> SUM(src.x) OVER (PARTITION BY src.k)
- CALCULATE(SUM([x]), FILTER(t, [k1]=EARLIER([k1]) && [k2]=EARLIER([k2])))
    -> SUM(src.x) OVER (PARTITION BY src.k1, src.k2)
- CALCULATE(MAX([d]), FILTER(t, [d] < EARLIER([d])))
    -> MAX(src.d) OVER (ORDER BY src.d ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
- CALCULATE(SUM([x]), FILTER(t, [fecha] = EARLIER([AA])))   -- year-over-year style
    -> SUM(src.x) OVER (PARTITION BY <id keys> ORDER BY src.fecha
        RANGE BETWEEN INTERVAL 1 YEAR PRECEDING AND INTERVAL 1 YEAR PRECEDING)
- RANKX(ALL(t), [m]) -> RANK() OVER (ORDER BY <m> DESC)
- For CALCULATE that references the SAME table with EARLIER: always use OVER (PARTITION BY ...).
  Never use a correlated subquery against the same source table — window functions are cheaper.

CROSS-TABLE references (other table than the source):
- 'OtherTable'[Col] without aggregation -> correlated subquery against the related UC table:
    (SELECT col FROM <other_uc_table> lk WHERE lk.key = src.key LIMIT 1)
- CALCULATE that aggregates an OTHER table:
    use a correlated scalar subquery joining on the relationship keys.

COLUMN NAME CASE — CRITICAL:
- Use the EXACT column names AS LISTED in the source columns block above (preserve case, do not lowercase, do not snake_case).
- Example: if source has `MessageTime`, write `src.MessageTime` (NOT `src.message_time`).
- If the column has spaces, accents, %, or other special chars, backtick-quote in SQL: SUM(`Mes_Año`). Otherwise no backticks.

STATUS field — be permissive. EVERY column reference resolves to one of:
  (a) a source column listed above   -> use src.<col_exact_case>
  (b) a column of a related UC table -> use correlated subquery
  (c) another calc col of THIS table  -> INLINE its DAX expression (do not alias-reference)

In Power BI a "slicer" is just a column the user filters on at runtime. In Databricks
that same column is a Metric View dimension. There is no "slicer state" to translate —
just translate the column reference and let the dashboard apply the filter at query time.

- "ok"     : straightforward expression, window functions, LOOKUPVALUE joins, slicer-bound
             columns (just reference src.<col>).
- "review" : SQL is valid but might be expensive at scale (e.g., correlated subquery against
             a wide fact table). Still emit the SQL; flag for performance review.
- "skip"   : ONLY when a referenced column truly does NOT exist in (a), (b), or (c) above
             — for example a What-If parameter that has no backing column, or a reference
             to a table not present in UC. Set comment="missing: <col>".

Return ONLY a JSON array (no markdown fences, no commentary):
[
  {{"pbi_name": "<original>", "column_name": "<snake>", "sql_expr": "<sql>", "status": "ok|review|skip", "comment": "<brief>"}}
]"""

    try:
        result = call_claude(prompt, max_tokens=4000)
        if "```" in result:
            m = re.search(r'\[.*\]', result, re.DOTALL)
            result = m.group() if m else result
        return json.loads(result)
    except Exception as e:
        print(f"  [WARN] calc cols translation failed for {table_name_pbi}: {str(e)[:200]}")
        return []


calc_cols_pending = []  # Para revisión manual

if not calc_cols_df.empty:
    print(f"\n=== Procesando {len(calc_cols_df)} columnas calculadas ===")

    for pbi_table, group_df in calc_cols_df.groupby('TableName'):
        # Match PBI table name -> UC table
        normalized = str(pbi_table).lower().replace(' ', '_').replace("'", "")
        uc_match = normalized if normalized in existing_tables else _match_to_uc_table(str(pbi_table), existing_tables)

        if not uc_match:
            print(f"  SKIP '{pbi_table}': no existe en UC ({len(group_df)} calc cols)")
            for _, cc in group_df.iterrows():
                calc_cols_pending.append({
                    'TableName': str(pbi_table),
                    'ColumnName': str(cc['ColumnName']),
                    'Expression': str(cc['Expression']),
                    'status': 'no_uc_table',
                    'comment': f'no UC table match for {pbi_table}'
                })
            continue

        source_fqn = _fqn(uc_match)
        cols = existing_tables.get(uc_match, [])
        col_types = existing_table_types.get(uc_match, {})
        cols_detail = "\n".join(f"  - {c}: {col_types.get(c, 'unknown')}" for c in cols)

        # Related tables (para LOOKUPVALUE u otros cross-table)
        related_info = ""
        if hasattr(rels_df, 'iterrows'):
            for _, rel in rels_df.iterrows():
                from_t = str(rel.get('FromTableName', ''))
                to_t = str(rel.get('ToTableName', ''))
                from_c = str(rel.get('FromColumnName', ''))
                to_c = str(rel.get('ToColumnName', ''))
                if from_t == pbi_table or to_t == pbi_table:
                    related_pbi = to_t if from_t == pbi_table else from_t
                    rt_match = _match_to_uc_table(related_pbi, existing_tables)
                    if rt_match:
                        rt_cols = existing_tables.get(rt_match, [])
                        rt_detail = ", ".join(rt_cols[:20])
                        related_info += f"\nRelated UC table {_fqn(rt_match)}: {rt_detail}"
                        related_info += f"\n  PBI relationship: {from_t}[{from_c}] <-> {to_t}[{to_c}]"

        print(f"\n  '{pbi_table}' -> {uc_match}: traduciendo {len(group_df)} calc cols")
        translated = translate_calc_cols_batch(str(pbi_table), source_fqn, cols_detail, related_info, group_df)

        ok_cols = []
        # Construir índice de columnas source agrupadas por su forma normalizada
        # (snake_case ASCII en lowercase). Una calc col cuya forma normalizada
        # colisione con N columnas source debe excluir las N para evitar ambigüedad.
        def _norm(s): return _ascii_snake(s).lower()
        source_norm_index = {}
        for c in cols:
            source_norm_index.setdefault(_norm(c), []).append(c)

        for tr in translated:
            status = tr.get('status', 'skip')
            pbi_name = tr.get('pbi_name', '')
            has_sql = bool(tr.get('sql_expr')) and bool(tr.get('column_name'))

            # ok y review se inyectan; skip solo se loguea
            if status in ('ok', 'review') and has_sql:
                clean_name = _ascii_snake(tr['column_name'])
                tr['column_name'] = clean_name
                # Todas las cols source que normalizan a clean_name deben excluirse
                replaces_list = source_norm_index.get(clean_name.lower(), [])
                ok_cols.append({
                    'column_name': clean_name,
                    'column_sql': tr['sql_expr'],
                    'for_measure': '(calc col)',
                    'comment': tr.get('comment', ''),
                    'is_calc_col': True,
                    'replaces_source_cols': replaces_list,  # plural: lista de cols a excluir
                })
                marker = '↻' if replaces_list else ('+' if status == 'ok' else '~')
                tag = f"[{status} replace {len(replaces_list)}]" if replaces_list else f"[{status}]"
                print(f"    {marker} {tag} {tr['column_name']}: {str(tr['sql_expr'])[:80]}")

                # review tambien va a pending para auditoria
                if status == 'review':
                    orig = group_df[group_df['ColumnName'] == pbi_name]
                    expr = str(orig['Expression'].iloc[0]) if not orig.empty else ''
                    calc_cols_pending.append({
                        'TableName': str(pbi_table),
                        'ColumnName': pbi_name,
                        'Expression': expr,
                        'status': 'review',
                        'comment': f"AUTO-INJECTED. {tr.get('comment', '')}"[:300]
                    })
            else:
                # skip o respuesta sin sql -> solo log
                orig = group_df[group_df['ColumnName'] == pbi_name]
                expr = str(orig['Expression'].iloc[0]) if not orig.empty else ''
                calc_cols_pending.append({
                    'TableName': str(pbi_table),
                    'ColumnName': pbi_name,
                    'Expression': expr,
                    'status': status,
                    'comment': tr.get('comment', '')[:300]
                })
                print(f"    - [{status}] {pbi_name}: {tr.get('comment', '')[:80]}")

        if not ok_cols:
            continue

        # Inyectar en lod_info: append si ya existe, crear nuevo si no
        if uc_match in lod_info:
            lod_info[uc_match]['lod_columns'].extend(ok_cols)
            view_name = lod_info[uc_match]['view_name']
            all_cols = lod_info[uc_match]['lod_columns']
        else:
            view_name = f"{CATALOG}.{SCHEMA}.{_t(f'lod_{uc_match}')}"
            lod_info[uc_match] = {'view_name': view_name, 'lod_columns': ok_cols}
            all_cols = ok_cols

        # Recrear la vista (idempotente, incluye calc cols + LOD cols si las habia).
        # Si alguna columna calc reemplaza una o más source cols, excluirlas con SELECT * EXCEPT.
        cols_to_replace = []
        for lc in all_cols:
            cols_to_replace.extend(lc.get('replaces_source_cols') or ([lc['replaces_source_col']] if lc.get('replaces_source_col') else []))
        cols_to_replace = sorted(set(cols_to_replace))
        select_extras = ", ".join(f"{lc['column_sql']} AS {lc['column_name']}" for lc in all_cols)
        if cols_to_replace:
            excepts = ", ".join(f"`{c}`" for c in cols_to_replace)
            sql = f"SELECT src.* EXCEPT ({excepts}), {select_extras} FROM {source_fqn} src"
        else:
            sql = f"SELECT src.*, {select_extras} FROM {source_fqn} src"
        try:
            spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
            print(f"  OK refresh {view_name} ({len(all_cols)} cols totales)")
        except Exception as e:
            print(f"  FAIL refresh {view_name}: {str(e)[:200]}")
            # Pasar las nuevas a pending
            for cc in ok_cols:
                calc_cols_pending.append({
                    'TableName': str(pbi_table),
                    'ColumnName': cc['column_name'],
                    'Expression': cc['column_sql'],
                    'status': 'sql_error',
                    'comment': str(e)[:300]
                })

# Persistir pendientes para revisión manual
if calc_cols_pending:
    pending_df = spark.createDataFrame(pd.DataFrame(calc_cols_pending).astype(str))
    pending_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{_t('pbi_calc_cols_pending')}")
    print(f"\n{len(calc_cols_pending)} calc cols pendientes -> {CATALOG}.{SCHEMA}.{_t('pbi_calc_cols_pending')}")
else:
    print(f"\nTodas las calc cols se tradujeron exitosamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5d. Analizar columnas requeridas por las measures de cada tabla

# COMMAND ----------

def _extract_dax_columns(dax):
    """Extract column references from DAX: 'Table'[Column] and [Column] patterns."""
    if not dax:
        return set()
    cols = set()
    # 'Table'[Column] or Table[Column]
    for m in re.finditer(r"'?([^'\[\]]+)'?\[([^\]]+)\]", dax):
        cols.add((m.group(1).strip(), m.group(2).strip()))
    # Standalone [Column] (no table prefix)
    for m in re.finditer(r"(?<!\w)\[([^\]]+)\]", dax):
        cols.add(('', m.group(1).strip()))
    return cols

def _normalize_col_name(name):
    """Normalize a column name for fuzzy matching."""
    n = name.lower().replace(' ', '_').replace("'", "")
    # Common PBI -> UC substitutions
    n = n.replace('%', 'pct').replace('#', 'num').replace('$', 'usd').replace('&', 'and')
    n = re.sub(r'[^a-z0-9_]', '', n)
    return n

def _map_column_to_uc(col_name, table_hint, base_table_cols, related_tables_map):
    """Map a PBI column name to a UC column. Returns (uc_table, uc_column) or None."""
    col_norm = _normalize_col_name(col_name)
    col_stripped = col_norm.replace('_', '')
    # Check in base table
    for uc_col in base_table_cols:
        uc_norm = _normalize_col_name(uc_col)
        if uc_norm == col_norm or uc_norm.replace('_', '') == col_stripped:
            return (None, uc_col)  # None = base table
    # Check in related tables
    for rt_name, rt_info in related_tables_map.items():
        for uc_col in rt_info['cols']:
            uc_norm = _normalize_col_name(uc_col)
            if uc_norm == col_norm or uc_norm.replace('_', '') == col_stripped:
                return (rt_name, uc_col)
    # Partial match: PBI name contained in UC name or vice versa
    for uc_col in base_table_cols:
        uc_stripped = _normalize_col_name(uc_col).replace('_', '')
        if col_stripped in uc_stripped or uc_stripped in col_stripped:
            return (None, uc_col)
    for rt_name, rt_info in related_tables_map.items():
        for uc_col in rt_info['cols']:
            uc_stripped = _normalize_col_name(uc_col).replace('_', '')
            if col_stripped in uc_stripped or uc_stripped in col_stripped:
                return (rt_name, uc_col)
    return None

# Build required columns map per base table
required_columns = {}  # {base_table: {'base': set(), 'related': {rt_name: set()}}}

for base_table, mlist in measures_by_base.items():
    catalog_table = base_table
    if base_table not in existing_tables:
        matched = _match_to_uc_table(base_table, existing_tables)
        if matched:
            catalog_table = matched
        else:
            continue

    base_cols = existing_tables.get(catalog_table, [])

    # Build related tables map from relationships
    base_orig = mlist[0].get('base_table', '') if mlist else ''
    related_map = {}  # {uc_table_name: {'pbi_name': ..., 'cols': [...], 'from_col': ..., 'to_col': ..., 'cardinality': ...}}
    if hasattr(rels_df, 'iterrows'):
        for _, rel in rels_df.iterrows():
            from_t = str(rel.get('FromTableName', ''))
            to_t = str(rel.get('ToTableName', ''))
            from_c = str(rel.get('FromColumnName', ''))
            to_c = str(rel.get('ToColumnName', ''))
            cardinality = str(rel.get('Cardinality', '')).strip()
            cross_filter = str(rel.get('CrossFilteringBehavior', '')).strip()
            is_active = str(rel.get('IsActive', '1')).strip()
            if is_active == '0' or is_active.lower() == 'false':
                continue
            if from_t == base_orig or to_t == base_orig:
                related_pbi = to_t if from_t == base_orig else from_t
                rt_match = _match_to_uc_table(related_pbi, existing_tables)
                if rt_match and rt_match != catalog_table:
                    related_map[rt_match] = {
                        'pbi_name': related_pbi,
                        'cols': existing_tables.get(rt_match, []),
                        'from_col': from_c,
                        'to_col': to_c,
                        'from_table': from_t,
                        'to_table': to_t,
                        'cardinality': cardinality,
                        'cross_filter': cross_filter,
                    }

    # Detect cross-fact references: measures that name another fact table directly.
    # If that fact shares natural keys with the base, add it to related_map as a
    # cross-fact join over all common keys (so its columns become reachable).
    referenced_pbi_tables = set()
    for m in mlist:
        for table_hint, _ in _extract_dax_columns(m['dax']):
            if table_hint:
                referenced_pbi_tables.add(table_hint.strip())

    already_related_pbi_names = {info['pbi_name'] for info in related_map.values()}
    already_related_pbi_names.add(base_orig)

    base_cols_lower = {c.lower(): c for c in base_cols}
    for ref_pbi in referenced_pbi_tables:
        if ref_pbi in already_related_pbi_names:
            continue
        rt_match = _match_to_uc_table(ref_pbi, existing_tables)
        if not rt_match or rt_match == catalog_table or rt_match in related_map:
            continue
        ref_cols = existing_tables.get(rt_match, [])
        common_keys = []
        for c in ref_cols:
            if c.lower() in base_cols_lower:
                common_keys.append((base_cols_lower[c.lower()], c))
        if not common_keys:
            continue
        related_map[rt_match] = {
            'pbi_name': ref_pbi,
            'cols': ref_cols,
            'from_col': common_keys[0][0],
            'to_col': common_keys[0][1],
            'from_table': base_orig,
            'to_table': ref_pbi,
            'cardinality': 'cross_fact',
            'cross_filter': '',
            'common_keys': common_keys,
            'is_cross_fact': True,
        }
        print(f"  [cross-fact] {base_orig} <-> {ref_pbi} via {[k[0] for k in common_keys]}")

    # Extract all column references from all measures
    needed_base = set()
    needed_related = defaultdict(set)  # {uc_table: set(uc_columns)}
    unmapped = []

    for m in mlist:
        dax_cols = _extract_dax_columns(m['dax'])
        for table_hint, col_name in dax_cols:
            mapping = _map_column_to_uc(col_name, table_hint, base_cols, related_map)
            if mapping:
                rt_name, uc_col = mapping
                if rt_name is None:
                    needed_base.add(uc_col)
                else:
                    needed_related[rt_name].add(uc_col)
            else:
                unmapped.append(f"{table_hint}[{col_name}]" if table_hint else f"[{col_name}]")

    req = {'base': needed_base, 'related': dict(needed_related), 'related_map': related_map, 'unmapped': list(set(unmapped))}
    required_columns[base_table] = req

    # Report
    print(f"\n{base_table}:")
    print(f"  Base columns needed: {len(needed_base)}")
    for rt, rt_cols in needed_related.items():
        print(f"  From {rt}: {sorted(rt_cols)}")
    if unmapped:
        print(f"  UNMAPPED: {list(set(unmapped))[:10]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Claude genera YAML base por tabla (source + joins + dimensions)

# COMMAND ----------

base_yamls = {}

for base_table, mlist in measures_by_base.items():
    catalog_table = base_table
    if base_table not in existing_tables:
        matched = _match_to_uc_table(base_table, existing_tables)
        if matched:
            catalog_table = matched
        else:
            print(f"  SKIP {base_table}: no existe en UC")
            continue

    # Determinar source: vista LOD o tabla original
    has_lod = base_table in lod_info
    if has_lod:
        source_fqn = lod_info[base_table]['view_name']
        lod_cols = lod_info[base_table]['lod_columns']
        lod_cols_info = "\n\nPre-computed LOD columns (already in source, add each one as a dimension):"
        for lc in lod_cols:
            lod_cols_info += f"\n  - {lc['column_name']}: {lc.get('comment', lc['column_sql'][:80])}"
    else:
        source_fqn = _fqn(catalog_table)
        lod_cols_info = ""

    cols = existing_tables.get(catalog_table, [])
    col_types = existing_table_types.get(catalog_table, {})
    cols_detail = "\n".join(f"  - {c}: {col_types.get(c, 'unknown')}" for c in cols)

    # Relaciones relevantes (con detalle completo de columnas)
    related_info = ""
    req = required_columns.get(base_table, {})
    related_map = req.get('related_map', {})
    needed_related = req.get('related', {})

    for rt_name, rt_info in related_map.items():
        rt_cols = existing_tables.get(rt_name, [])
        rt_types = existing_table_types.get(rt_name, {})
        rt_detail = "\n".join(f"    - {c}: {rt_types.get(c, 'unknown')}" for c in rt_cols)
        from_t = rt_info['from_table']
        to_t = rt_info['to_table']
        from_c = rt_info['from_col']
        to_c = rt_info['to_col']
        cardinality = rt_info.get('cardinality', '?')
        cross_filter = rt_info.get('cross_filter', '?')
        related_info += f"\n  Relationship: {from_t}[{from_c}] -> {to_t}[{to_c}]"
        related_info += f"\n  Cardinality: {cardinality} | CrossFilter: {cross_filter}"
        related_info += f"\n  UC table: {_fqn(rt_name)}"
        if rt_info.get('is_cross_fact'):
            ck = rt_info.get('common_keys', [])
            keys_str = ", ".join(f"{a} = {b}" for a, b in ck)
            related_info += f"\n  ** CROSS-FACT JOIN ** — Common keys: {keys_str}"
            related_info += "\n  ** This is NOT a Power BI relationship; it's inferred from common natural keys."
            related_info += "\n  ** The 'on' clause MUST AND together ALL common keys to keep grain correct."
        related_info += f"\n  Columns:\n{rt_detail}"
        # Highlight which columns the measures actually need
        needed_from_this = needed_related.get(rt_name, set())
        if needed_from_this:
            related_info += f"\n  ** REQUIRED by measures: {sorted(needed_from_this)} — MUST be included as dimensions via JOIN **"

    # Measures que iran en esta view
    measures_summary = ""
    for m in mlist[:20]:
        measures_summary += f"\n  - {m['measure_name']}: {m['dax'][:120]}"

    # Build instruction: ALL relationships → joins (always), required cols are mandatory.
    all_joins_instruction = ""
    if related_map:
        all_joins_instruction = (
            "\n\nCRITICAL — ALL POWER BI RELATIONSHIPS MUST BE JOINS:\n"
            "Every relationship listed in `Related tables` above represents a Power BI "
            "model relationship. You MUST create a `joins:` entry for EACH ONE, even if "
            "no measure currently references columns from that table. Reasons:\n"
            "  - Dashboard consumers will filter/group by these dimensions\n"
            "  - Future measures may reference them\n"
            "  - The PBI semantics depends on these joins being active\n\n"
            "For each related table you must:\n"
            "1. Add a `joins:` entry with name (snake_case, alias of the related table), "
            "source, and 'on' clause\n"
            "2. Expose dimension columns following the CARDINALITY RULE below\n"
            "3. Skip irrelevant columns: long free-text descriptions, lat/lon, internal IDs, "
            "audit timestamps — use judgement, prioritize columns useful for slicing\n\n"
            "CARDINALITY RULE (CRITICAL — prevents double-counting bugs):\n"
            "Each relationship has a `Cardinality` field shown above. Apply these rules:\n"
            "  - `M:1` (many-to-one) — typical star schema fact→dim. SAFE: expose all useful "
            "categorical/date columns of the related dim as dimensions.\n"
            "  - `1:1` — also safe: expose useful columns as dimensions.\n"
            "  - `1:M` (one-to-many) — base table is the `1` side. SAFE for dimensions, BUT "
            "any SUM/COUNT in measures over the joined table will inflate. Add the join, "
            "expose the columns, but ADD A COMMENT in the join entry warning consumers.\n"
            "  - `M:M` (many-to-many) — UNSAFE for aggregations. Add the join (so the join "
            "exists for filtering semantics), but DO NOT expose columns of the related table "
            "as dimensions — exposing them would cause double-counting in any SUM/AVG. "
            "Add a `# CARDINALITY: M:M — only for filtering, columns not exposed` comment.\n"
            "  - `Both` cross-filter direction: treat with caution like M:M unless you can "
            "verify there is exactly one row per key on each side.\n"
            "  - `cross_fact` — fact-to-fact join inferred from shared natural keys (NOT a "
            "PBI relationship). The `on` clause MUST AND together ALL listed common keys, "
            "e.g.: `'on': source.fechacorte = other.fechacorte AND source.idestacion = other.idestacion AND source.idproducto = other.idproducto`. "
            "Do NOT expose categorical dimensions of this fact (would cause double-counting). "
            "Only expose the fact's measure columns AS DIMENSIONS so measures can reference "
            "them via `other.column_name` (downstream SUM/AVG will aggregate them). "
            "Add a `# CROSS-FACT JOIN — keys: ...` comment in the join entry.\n"
        )
        if needed_related:
            all_joins_instruction += "\nColumns required by measures (MUST be present as dimensions):\n"
            for rt_name, rt_cols in needed_related.items():
                all_joins_instruction += f"  - {rt_name}: {sorted(rt_cols)}\n"

    prompt = f"""Generate a Databricks Metrics View YAML with ONLY source, joins, and dimensions (NO measures).
This YAML will be the base structure where measures will be added later one by one.

Source table: {source_fqn}
Columns:
{cols_detail}
{lod_cols_info}
{f"Related tables:{related_info}" if related_info else "No related tables."}
{all_joins_instruction}
These measures will be added later (use this to decide which dimensions are relevant):
{measures_summary}

RULES:
- version: 1.1
- source: {source_fqn}
- dimensions: one per column that is useful for segmenting/filtering. Use these naming rules STRICTLY:
  * If column name uses ONLY ASCII letters/digits/underscore (e.g., "fecha_corte", "amount"):
      name: <column_name.lower()>
      expr: <column_name>            # plain, no backticks
  * If column name has SPACES, ACCENTS, Ñ, %, /, or any non-ASCII char (e.g., "Mes_Año", "Año Fiscal", "Cumplimiento %"):
      name: <ASCII snake_case lower> # transliterate: á→a, é→e, í→i, ó→o, ú→u, ñ→n, drop other non-alphanum
      expr: "`<original>`"           # backtick-quote ORIGINAL, AND wrap the WHOLE expr in DOUBLE QUOTES (YAML requirement)
  * EXAMPLES:
      - For column "Mes_Año":           name: mes_ano       expr: "`Mes_Año`"      display_name: "Mes Año"
      - For column "Año Fiscal":        name: ano_fiscal    expr: "`Año Fiscal`"   display_name: "Año Fiscal"
      - For column "Cumplimiento %":    name: cumplimiento  expr: "`Cumplimiento %`"  display_name: "Cumplimiento"
- CRITICAL — NEVER start a YAML value with a backtick character. Backticks MUST be inside double quotes.
  WRONG: expr: `Mes_Año`        (YAML parser fails: "found character '`' that cannot start any token")
  RIGHT: expr: "`Mes_Año`"
- display_name: human-readable, from the original column name (keep accents/spaces)
- DO NOT include a measures section — it will be added later
- For date/timestamp columns, also add a month-level dimension (DATE_TRUNC)
- joins: include ONE JOIN ENTRY PER POWER BI RELATIONSHIP listed above (even if no current measure uses them — see the CRITICAL section). 'on' key must be quoted.
- For joined table dimensions: prefix name with join alias (e.g., joinname_column)
- EVERY column marked as REQUIRED above MUST appear as a dimension (via join). If you skip a required column, the measures that reference it will fail.
- Valid dimension fields ONLY: name, expr, display_name, comment, format, synonyms
- Valid join fields ONLY: name, source, 'on', using, joins
- YAML SAFETY (CRITICAL — incorrectly quoted strings break the parser):
    * For ANY string value containing `:`, `#`, `&`, `*`, `[`, `]`, `{{`, `}}`, `,`, `?`, `!`, `|`, `>`, `'`, `"` — wrap the WHOLE value in double quotes and escape internal `"` as `\\"`.
    * Examples:
        display_name: "Budget Goal"            # safe (no special chars)
        display_name: "Budget Goal: 2024"      # OK — wrapped in quotes
        display_name: "He said \\"hi\\""        # internal double-quote escaped
    * NEVER write `display_name: "Budget Goal:` (unclosed quote) or split a value across lines without using the YAML `|` or `>` block scalar.
    * Output the YAML in FULL — never truncate. If approaching the token limit, prioritize fewer dimensions but keep the YAML structurally valid.
{"- IMPORTANT: The source is a LOD view with pre-computed window columns. Add each LOD column as a dimension (name=column_name, expr=column_name). These will be used by measures with ANY_VALUE() later." if has_lod else ""}
- Return ONLY the YAML. No markdown fences, no explanations.
- Add a dummy measure so the view can be validated:
  measures:
    - name: __row_count
      expr: "COUNT(1)"
"""

    print(f"\n{'='*60}")
    print(f"Generando YAML base para: {base_table} ({len(mlist)} measures pendientes)")
    if has_lod:
        print(f"  Source: {source_fqn} (LOD view con {len(lod_cols)} columnas pre-calculadas)")
    if needed_related:
        for rt, rt_cols in needed_related.items():
            print(f"  Required from {rt}: {sorted(rt_cols)}")
    print(f"{'='*60}")

    yaml_text = call_claude(prompt, max_tokens=4000)
    print(yaml_text[:500])

    base_yamls[base_table] = {
        'yaml': yaml_text,
        'catalog_table': catalog_table,
        'measures': mlist,
        'lod_columns': lod_info.get(base_table, {}).get('lod_columns', []),
        'required_columns': req,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar cada YAML base creando la Metric View

# COMMAND ----------

validated_yamls = {}

for base_table, info in base_yamls.items():
    yaml_text = info['yaml']
    catalog_table = info['catalog_table']
    # Sufijar con run_id para aislar corridas. _t() expande "mv_X" → "mv_X_<run_id>" si run_id está set.
    safe_name = _t(f"mv_{base_table}".replace(' ', '_').replace("'", ""))
    view_name = f"{CATALOG}.{SCHEMA}.{safe_name}"

    print(f"\n--- {view_name} ---")

    # Tipos de columnas de la tabla source (para el postprocess que limpia format:date en STRING cols)
    src_types = {}
    if catalog_table in existing_table_types:
        src_types.update(existing_table_types[catalog_table])
    # También tipos de tablas relacionadas (con prefijo dim_X_<col>)
    for rt_name, rt_types in existing_table_types.items():
        if rt_name == catalog_table:
            continue
        for c, t in rt_types.items():
            src_types[f"{rt_name}_{c}"] = t

    MAX_ATTEMPTS = 5
    for attempt in range(MAX_ATTEMPTS):
        try:
            # Sanitizar: dropear dimensions/measures sin 'expr' antes del create
            yaml_to_use, dropped = _sanitize_metric_view_yaml(yaml_text)
            if dropped:
                print(f"  [sanitize] dropped sin expr: {dropped}")
            # Post-procesamiento defensivo: arreglar comillas, identificadores no-ASCII, format:date inválido
            yaml_to_use = _postprocess_yaml_defensively(yaml_to_use, source_types_by_col=src_types)
            create_sql = f"""CREATE OR REPLACE VIEW {view_name}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_to_use}
$$"""
            # En el último intento, imprimir el YAML completo para diagnóstico
            if attempt == MAX_ATTEMPTS - 1:
                print(f"  [debug] YAML del último intento:\n{yaml_to_use[:6000]}\n---")
            spark.sql(create_sql)
            yaml_text = yaml_to_use  # Persistir el limpio
            print(f"  OK" + (f" (fix {attempt})" if attempt > 0 else ""))
            validated_yamls[base_table] = {
                'yaml': yaml_text,
                'catalog_table': catalog_table,
                'view_name': view_name,
                'measures': info['measures'],
                'lod_columns': info.get('lod_columns', []),
            }
            break
        except Exception as e:
            error_msg = str(e)[:500]
            print(f"  x intento {attempt+1}: {error_msg[:300]}")

            if attempt < MAX_ATTEMPTS - 1:
                fix_prompt = f"""Fix this Databricks Metrics View YAML. It has NO measures yet, only source + joins + dimensions + a dummy __row_count measure.

ERROR:
{error_msg}

CURRENT YAML:
{yaml_text}

RULES:
- Valid dimension fields ONLY: name, expr, display_name, comment, format, synonyms
- Valid join fields ONLY: name, source, 'on', using, joins
- 'on' must be quoted in YAML
- No "type" field in dimensions
- Keep the dummy measure __row_count
- Return ONLY the fixed YAML. No markdown fences."""

                try:
                    yaml_text = call_claude(fix_prompt, max_tokens=4000)
                    print(f"  -> Claude corrigio, reintentando...")
                except Exception as ce:
                    print(f"  -> Error en fix: {str(ce)[:100]}")
                    break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Verificar que TODAS las columnas requeridas estan en el YAML

# COMMAND ----------

def _extract_yaml_dimension_names(yaml_text):
    """Extract all dimension names from a YAML string."""
    names = set()
    in_dims = False
    for line in yaml_text.split('\n'):
        stripped = line.strip()
        if stripped == 'dimensions:' or stripped.startswith('dimensions:'):
            in_dims = True
            continue
        if in_dims:
            if stripped.startswith('- name:'):
                name = stripped.replace('- name:', '').strip().strip("'\"")
                names.add(name.lower())
            elif stripped and not line.startswith(' ') and not line.startswith('\t') and ':' in stripped:
                in_dims = False
    return names

def _extract_yaml_dimension_exprs(yaml_text):
    """Extract all dimension exprs from a YAML string."""
    exprs = set()
    in_dims = False
    for line in yaml_text.split('\n'):
        stripped = line.strip()
        if stripped == 'dimensions:' or stripped.startswith('dimensions:'):
            in_dims = True
            continue
        if in_dims:
            if stripped.startswith('expr:'):
                expr = stripped.replace('expr:', '').strip().strip("'\"")
                exprs.add(expr.lower())
            elif stripped and not line.startswith(' ') and not line.startswith('\t') and ':' in stripped:
                in_dims = False
    return exprs

for base_table, info in list(validated_yamls.items()):
    req = required_columns.get(base_table, {})
    needed_related = req.get('related', {})
    if not needed_related:
        continue

    yaml_text = info['yaml']
    dim_names = _extract_yaml_dimension_names(yaml_text)
    dim_exprs = _extract_yaml_dimension_exprs(yaml_text)
    all_dims = dim_names | dim_exprs

    missing = []
    for rt_name, rt_cols in needed_related.items():
        for col in rt_cols:
            col_lower = col.lower()
            # Check if the column appears in any dimension (name or expr)
            found = any(col_lower in d or col_lower.replace('_', '') in d.replace('_', '') for d in all_dims)
            if not found:
                missing.append((rt_name, col))

    if missing:
        print(f"\n{info['view_name']}: {len(missing)} columnas requeridas FALTANTES")
        for rt, col in missing:
            print(f"  MISSING: {col} (from {rt})")

        # Ask Claude to add the missing columns
        related_map = req.get('related_map', {})
        missing_instructions = "Add these MISSING columns as dimensions. They are in related tables that MUST be joined:\n"
        missing_by_table = defaultdict(list)
        for rt, col in missing:
            missing_by_table[rt].append(col)
        for rt, cols in missing_by_table.items():
            rt_info = related_map.get(rt, {})
            missing_instructions += f"\nTable: {_fqn(rt)} (columns: {cols})"
            if rt_info:
                missing_instructions += f"\nJoin: {rt_info['from_table']}[{rt_info['from_col']}] -> {rt_info['to_table']}[{rt_info['to_col']}]"
            # Include ALL columns from this table for reference
            all_rt_cols = existing_tables.get(rt, [])
            all_rt_types = existing_table_types.get(rt, {})
            missing_instructions += f"\nAll columns: " + ", ".join(f"{c}({all_rt_types.get(c, '')})" for c in all_rt_cols)

        fix_prompt = f"""This Metrics View YAML is missing dimensions that the measures need. Add them.

{missing_instructions}

CURRENT YAML:
{yaml_text}

RULES:
- If the related table is NOT already joined, add a join for it
- For each missing column, add a dimension with name=joinname_column_name (lowercase), expr=joinname.ColumnName
- 'on' must be quoted in YAML
- Valid dimension fields ONLY: name, expr, display_name, comment, format, synonyms
- Valid join fields ONLY: name, source, 'on', using, joins
- Keep ALL existing dimensions and joins intact
- Keep the dummy measure __row_count
- Return ONLY the complete fixed YAML. No markdown fences."""

        try:
            fixed_yaml = call_claude(fix_prompt, max_tokens=4000)
            # Re-validate
            view_name = info['view_name']
            spark.sql(f"""CREATE OR REPLACE VIEW {view_name} WITH METRICS LANGUAGE YAML AS $$\n{fixed_yaml}\n$$""")
            validated_yamls[base_table]['yaml'] = fixed_yaml
            # Verify again
            new_dims = _extract_yaml_dimension_names(fixed_yaml) | _extract_yaml_dimension_exprs(fixed_yaml)
            still_missing = [(rt, col) for rt, col in missing if not any(col.lower() in d or col.lower().replace('_', '') in d.replace('_', '') for d in new_dims)]
            if still_missing:
                print(f"  WARNING: aun faltan {len(still_missing)}: {still_missing}")
            else:
                print(f"  OK: {len(missing)} columnas agregadas y validadas")
        except Exception as e:
            print(f"  ERROR al agregar columnas faltantes: {str(e)[:200]}")
    else:
        print(f"{info['view_name']}: todas las columnas requeridas presentes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen

# COMMAND ----------

print(f"=== METRIC VIEWS BASE ===")
print(f"Total tablas: {len(base_yamls)}")
print(f"Validadas OK: {len(validated_yamls)}")
print(f"Fallidas: {len(base_yamls) - len(validated_yamls)}")
print(f"Con Fixed LOD: {sum(1 for v in validated_yamls.values() if v.get('lod_columns'))}")
print()
for base_table, info in validated_yamls.items():
    lod_tag = f" [LOD: {len(info['lod_columns'])} cols]" if info.get('lod_columns') else ""
    print(f"  OK  {info['view_name']} ({len(info['measures'])} measures pendientes){lod_tag}")
for base_table in base_yamls:
    if base_table not in validated_yamls:
        print(f"  FAIL {base_table}")
