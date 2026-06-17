# Databricks notebook source
# MAGIC %md
# MAGIC # 5c. Refinar páginas del dashboard con Claude (estilo Genie)
# MAGIC
# MAGIC Itera página por página el `.lvdash.json` y le pide a Claude que mejore:
# MAGIC
# MAGIC 1. **Layout** — distribución 12-col de la grilla, agrupación lógica de widgets
# MAGIC 2. **Format y colores** — percent / currency / miles, semáforos en KPIs comparativos
# MAGIC 3. **Títulos** — display names en español humano, no técnico (`counter_pct_uso` → `% Uso Semanal`)
# MAGIC
# MAGIC Es post-procesamiento — corre después de notebook 5 (genera dashboard) y 5b (refine_dashboard).

# COMMAND ----------

# DBTITLE 1,Parámetros
import json, re, time, base64, requests, uuid

try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("run_id", "", "Sufijo run_id (vacío = sin sufijo)")
dbutils.widgets.text("dashboard_path", "", "Path .lvdash.json (vacío = ~/SAT/Dashboard.lvdash.json)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6", "Endpoint LLM (sonnet-4-6 es buen balance)")
dbutils.widgets.text("only_page", "", "(opcional) Solo refinar esta página por displayName; vacío = todas")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()
RUN_ID  = dbutils.widgets.get("run_id").strip()
SUFFIX  = f"_{RUN_ID}" if RUN_ID else ""
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint").strip() or "databricks-claude-sonnet-4-6"
ONLY_PAGE = dbutils.widgets.get("only_page").strip()

print(f"Catálogo:  {CATALOG}.{SCHEMA}")
print(f"Dashboard: {DASHBOARD_PATH}")
print(f"LLM:       {LLM_ENDPOINT}")
if ONLY_PAGE:
    print(f"Solo página: {ONLY_PAGE}")

# COMMAND ----------

# DBTITLE 1,Cliente LLM con retry y manejo de temperature
_MODELS_NO_TEMPERATURE = {'databricks-claude-opus-4-7'}
_RETRY_STATUS = {502, 503, 504, 429}
_MAX_RETRIES = 5

def _post_with_retry(url, headers, payload, timeout=180):
    delay = 2
    last_resp = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code not in _RETRY_STATUS:
                return resp
            last_resp = resp
            print(f"    [retry {attempt+1}/{_MAX_RETRIES}] {resp.status_code} esperando {delay}s…")
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"    [retry {attempt+1}/{_MAX_RETRIES}] red ({type(e).__name__}), {delay}s…")
        time.sleep(delay)
        delay = min(delay * 2, 60)
    return last_resp

def call_claude(system_prompt, user_prompt, max_tokens=12000):
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    url = f"https://{host}/serving-endpoints/{LLM_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
    }
    if LLM_ENDPOINT not in _MODELS_NO_TEMPERATURE:
        payload["temperature"] = 0.2
    resp = _post_with_retry(url, headers, payload)
    if resp.status_code == 400 and 'temperature' in resp.text.lower():
        payload.pop('temperature', None)
        resp = _post_with_retry(url, headers, payload)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        lines = content.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines)
    return content.strip()

# COMMAND ----------

# DBTITLE 1,Cargar el dashboard actual
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

resp = requests.get(
    f"https://{host}/api/2.0/workspace/export",
    params={"path": DASHBOARD_PATH, "format": "AUTO"},
    headers={"Authorization": f"Bearer {token}"}, timeout=30,
)
resp.raise_for_status()
existing_json = base64.b64decode(resp.json()["content"]).decode("utf-8")
dashboard = json.loads(existing_json)

print(f"Dashboard leído: {DASHBOARD_PATH}")
print(f"  Datasets: {len(dashboard.get('datasets', []))}")
print(f"  Pages:    {len(dashboard.get('pages', []))}")
for p in dashboard.get('pages', []):
    print(f"    - {p.get('displayName','?')}: {len(p.get('layout',[]))} widgets")

# COMMAND ----------

# DBTITLE 1,Construir contexto de datasets (dimensions + measures por dataset)
# Cada dataset apunta a una v_dashboard_*. Necesitamos saber qué columnas tiene y qué measures.
dataset_columns = {}  # {dataset_name: [(col, type), ...]}
for ds in dashboard.get('datasets', []):
    ds_name = ds.get('name', '')
    # Intentar extraer la tabla source del query
    query = ' '.join(ds.get('queryLines', [])) if ds.get('queryLines') else ds.get('query', '')
    # Buscar "FROM catalog.schema.view"
    m = re.search(r'FROM\s+(\S+)', query, re.IGNORECASE)
    src = m.group(1).strip().rstrip(',').rstrip(';') if m else None
    if not src:
        continue
    try:
        cols = spark.sql(f"DESCRIBE {src}").collect()
        dataset_columns[ds_name] = [
            (c.col_name, c.data_type or '')
            for c in cols if c.col_name and not c.col_name.startswith('#')
        ]
    except Exception as e:
        print(f"  ⚠ no pude describir {src} para dataset {ds_name}: {str(e)[:80]}")

print(f"\nDatasets con columnas resueltas: {len(dataset_columns)}")
for ds, cols in dataset_columns.items():
    print(f"  {ds}: {len(cols)} columnas")

# COMMAND ----------

# DBTITLE 1,Prompt del sistema (rol y reglas de refinamiento)
SYSTEM_PROMPT = """You are a Databricks Lakeview dashboard UX expert. Your job is to take an existing
page's layout (list of widget JSON objects) and REFINE it without changing the data the widgets show.

You refine THREE dimensions:

1. LAYOUT (12-column grid)
   - KPI counters (single-number cards): group in a single row at the top, each 3 wide, height 2.
   - Bar / pie / line charts: 6 wide × 4 tall when standalone, or 4 wide for side-by-side comparison.
   - Tables: 12 wide (full width) × 6-8 tall.
   - Text/markdown headers: 12 wide × 1 tall, on rows separating sections.
   - Don't overlap widgets. Y must be coherent (next row = previous y + height).
   - Use the full width (12) — no empty right columns.

2. FORMAT (per encoding)
   - Measure name contains '%' or 'pct' or 'porcent' → format: {"type": "number-percent", "decimalPlaces": {"type": "exact", "places": 1}}
   - Measure name contains 'monto', 'venta', 'compensac', 'premio', 'dinero', '$' → format: {"type": "number-currency", "currencyCode": "MXN", "decimalPlaces": {"type": "exact", "places": 0}, "abbreviation": "compact"}
   - Plain counts/totals → format: {"type": "number-plain", "abbreviation": "compact"} (so 1500000 → 1.5M)
   - Dates → format: {"type": "date-time", "dateTimeFormat": "yyyy-MM-dd"}

3. TITLES & DISPLAY NAMES
   - Replace technical names like 'counter_pct_uso_semanal' or 'sum_compensacion_logrado' with human Spanish:
       'counter_pct_uso_semanal'   → '% Uso Semanal'
       'sum_compensacion_logrado'  → 'Compensación Lograda'
       'avg_cumplimiento_actual'   → 'Cumplimiento Promedio Actual'
   - Spec.frame.title (top of widget) and encoding displayName (axis labels, columns).
   - Sentence case. No CAPS LOCK.

4. (Optional but encouraged) COLORS — for measures that signal performance:
   - Compliance % or alcance: traffic-light rules in encoding.style with thresholds 0.7 and 0.9
   - Signed deltas (variación, diferencia): red if <0, green if >=0

CONSTRAINTS:
- Do NOT change the dataset, the field expressions, the widget types, or the queries. Only metadata + layout + format.
- Preserve widget name, queries[].name, query.datasetName, query.fields[].name, query.fields[].expression.
- Return ONLY a JSON array of refined widget objects (same shape as input). No markdown, no commentary.
"""

# COMMAND ----------

# DBTITLE 1,Función: refinar una página
def refine_page(page):
    page_name = page.get('displayName', '?')
    layout = page.get('layout', [])
    if not layout:
        print(f"  (página vacía — skip)")
        return False

    # Identificar datasets usados en esta página
    used_datasets = set()
    for entry in layout:
        for q in entry.get('widget', {}).get('queries', []):
            ds = q.get('query', {}).get('datasetName', '')
            if ds:
                used_datasets.add(ds)

    # Contexto: datasets disponibles + sus columnas
    datasets_block = ""
    for ds_name in sorted(used_datasets):
        cols = dataset_columns.get(ds_name, [])
        if cols:
            datasets_block += f"\n  Dataset '{ds_name}':\n"
            for c, t in cols[:60]:
                datasets_block += f"    - {c}: {t}\n"
        else:
            datasets_block += f"\n  Dataset '{ds_name}': (columnas no resueltas)\n"

    # Layout actual de la página, compactado
    current_layout_json = json.dumps(layout, ensure_ascii=False, indent=2)
    if len(current_layout_json) > 30000:
        # Truncar si es muy grande
        current_layout_json = current_layout_json[:30000] + "\n  ... [truncated]\n"

    user_prompt = f"""Refine this Lakeview dashboard page.

Page name (do not change): "{page_name}"

Datasets used in this page and their columns:
{datasets_block}

CURRENT LAYOUT (array of widget+position objects):
{current_layout_json}

Apply the refinement rules from the system prompt:
- Reorganize layout in the 12-column grid (group KPI counters at top, etc.)
- Apply correct format (percent/currency/plain) based on field names
- Replace technical names with human Spanish display names and titles
- (Optional) Add traffic-light colors for compliance %, signed style for deltas

Return ONLY the refined JSON array (same length, same widget types, same datasets/fields).
"""
    raw = call_claude(SYSTEM_PROMPT, user_prompt, max_tokens=16000)
    # Parse — el output debe ser un array JSON
    try:
        if raw.startswith('{'):
            # Por si devuelve un objeto envolviendo, extraer array
            m = re.search(r'\[.*\]', raw, re.DOTALL)
            if m: raw = m.group()
        refined = json.loads(raw)
        if not isinstance(refined, list):
            print(f"  ✗ Claude no devolvió un array; tipo={type(refined).__name__}")
            return False
    except Exception as e:
        print(f"  ✗ parse error: {str(e)[:120]}")
        print(f"    raw[:400]: {raw[:400]}")
        return False

    # Validación básica: misma cantidad de widgets non-text (text widgets pueden agregarse)
    orig_non_text = sum(1 for w in layout if 'spec' in w.get('widget', {}))
    new_non_text  = sum(1 for w in refined if 'spec' in w.get('widget', {}))
    if new_non_text < orig_non_text:
        print(f"  ⚠ refinamiento perdió widgets ({orig_non_text} → {new_non_text}); descartando")
        return False

    page['layout'] = refined
    print(f"  ✓ refinada: {len(refined)} widgets (era {len(layout)})")
    return True

# COMMAND ----------

# DBTITLE 1,Refinar cada página
refined_count = 0
for page in dashboard.get('pages', []):
    page_name = page.get('displayName', '?')
    if ONLY_PAGE and page_name.strip().lower() != ONLY_PAGE.strip().lower():
        continue
    print(f"\n{'='*70}")
    print(f"Página: {page_name}")
    print('='*70)
    if refine_page(page):
        refined_count += 1

print(f"\n\nTotal páginas refinadas: {refined_count}")

# COMMAND ----------

# DBTITLE 1,Guardar el dashboard refinado
dashboard_json_str = json.dumps(dashboard, indent=2, ensure_ascii=False)
content_b64 = base64.b64encode(dashboard_json_str.encode('utf-8')).decode('utf-8')

resp = requests.post(
    f"https://{host}/api/2.0/workspace/import",
    json={"path": DASHBOARD_PATH, "format": "AUTO",
          "content": content_b64, "overwrite": True},
    headers={"Authorization": f"Bearer {token}"}, timeout=30,
)

if resp.status_code == 200:
    print(f"✓ Dashboard refinado guardado en: {DASHBOARD_PATH}")
else:
    print(f"✗ Error guardando ({resp.status_code}): {resp.text[:300]}")

# COMMAND ----------

# DBTITLE 1,Resumen
print(f"\n{'='*70}")
print("REFINAMIENTO COMPLETADO")
print('='*70)
for p in dashboard.get('pages', []):
    layout = p.get('layout', [])
    types = {}
    for w in layout:
        wt = w.get('widget', {}).get('spec', {}).get('widgetType', 'text')
        types[wt] = types.get(wt, 0) + 1
    type_summary = ', '.join(f'{n} {t}' for t, n in sorted(types.items()))
    print(f"  {p.get('displayName','?')}: {len(layout)} widgets ({type_summary})")

