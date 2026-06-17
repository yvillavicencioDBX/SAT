# Databricks notebook source
# MAGIC %md
# MAGIC # Humanizar Títulos del Dashboard
# MAGIC
# MAGIC Lee el dashboard existente y usa Claude para convertir los títulos técnicos
# MAGIC de los widgets a nombres legibles en español.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

import json, requests, base64

# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("dashboard_path", "", "Path del dashboard (vacío = derivar del usuario actual)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4", "Endpoint LLM")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")


RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
print(f"Dashboard: {DASHBOARD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer dashboard

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

resp = requests.get(
    f"https://{host}/api/2.0/workspace/export",
    params={"path": DASHBOARD_PATH, "format": "AUTO"},
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)
resp.raise_for_status()
existing_json = base64.b64decode(resp.json()["content"]).decode("utf-8")
dashboard = json.loads(existing_json)

print(f"✓ Dashboard leído")
for p in dashboard.get('pages', []):
    print(f"  {p.get('displayName', '?')}: {len(p.get('layout', []))} widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extraer títulos actuales

# COMMAND ----------

titles_to_fix = []
for p in dashboard.get('pages', []):
    page_name = p.get('displayName', '?')
    for w in p.get('layout', []):
        widget = w.get('widget', {})
        spec = widget.get('spec', {})
        wtype = spec.get('widgetType', '')
        frame = spec.get('frame', {})
        title = frame.get('title', '')
        widget_name = widget.get('name', '')

        # Títulos de widgets con spec
        if wtype and title:
            titles_to_fix.append({
                'page': page_name,
                'widget_name': widget_name,
                'widget_type': wtype,
                'current_title': title,
                'location': 'frame.title',
            })

        # DisplayNames en encodings
        encodings = spec.get('encodings', {})
        for enc_key, enc_val in encodings.items():
            if isinstance(enc_val, dict) and 'displayName' in enc_val:
                dn = enc_val['displayName']
                titles_to_fix.append({
                    'page': page_name,
                    'widget_name': widget_name,
                    'widget_type': wtype,
                    'current_title': dn,
                    'location': f'encodings.{enc_key}.displayName',
                })
            elif isinstance(enc_val, list):  # columns array in tables
                for i, col in enumerate(enc_val):
                    if isinstance(col, dict) and 'displayName' in col:
                        titles_to_fix.append({
                            'page': page_name,
                            'widget_name': widget_name,
                            'widget_type': wtype,
                            'current_title': col['displayName'],
                            'location': f'encodings.{enc_key}[{i}].displayName',
                        })

        # Títulos de texto
        textbox = widget.get('textbox_spec', '')
        if isinstance(textbox, str) and textbox:
            titles_to_fix.append({
                'page': page_name,
                'widget_name': widget_name,
                'widget_type': 'text',
                'current_title': textbox,
                'location': 'textbox_spec',
            })
        # multilineTextboxSpec
        mts = widget.get('multilineTextboxSpec', {})
        if isinstance(mts, dict) and 'lines' in mts:
            for i, line in enumerate(mts['lines']):
                titles_to_fix.append({
                    'page': page_name,
                    'widget_name': widget_name,
                    'widget_type': 'text',
                    'current_title': line,
                    'location': f'multilineTextboxSpec.lines[{i}]',
                })

        # Descripción de filtros (label visible)
        if wtype and 'filter' in wtype:
            desc = frame.get('description', '')
            if desc:
                titles_to_fix.append({
                    'page': page_name,
                    'widget_name': widget_name,
                    'widget_type': wtype,
                    'current_title': desc,
                    'location': 'frame.description',
                })

print(f"Títulos encontrados: {len(titles_to_fix)}")
for t in titles_to_fix[:10]:
    print(f"  [{t['widget_type']}] {t['current_title'][:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pedir a Claude que humanice los títulos

# COMMAND ----------

titles_list = "\n".join([
    f"- \"{t['current_title']}\" (widget: {t['widget_type']}, page: {t['page']}, location: {t['location']})"
    for t in titles_to_fix
])

prompt = f"""Convert these dashboard widget titles to human-friendly Spanish labels.

Current titles:
{titles_list}

RULES:
- Return a JSON array where each element is: {{"original": "current title", "humanized": "new title"}}
- Make titles clear, descriptive, and in proper Spanish
- For technical column names like "transmittingcountry" → "País Transmisor"
- For "receivingcountry" → "País Receptor"
- For "reportingperiod" → "Periodo de Reporte"
- For "timestamp" → "Fecha"
- For "marca_rfc" → "Marca RFC"
- For "anio_fiscal" → "Año Fiscal"
- For counter titles like "counter_fat_repunc" → "Total de Registros"
- For bar titles, keep the original meaning but make readable: "Contribuyente Localizado" stays
- For table column headers, use proper capitalization and Spanish
- For text/title widgets, keep markdown (#) but humanize the text
- Do NOT change titles that are already human-friendly
- Output ONLY the JSON array, no markdown fences
"""


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

url = f"https://{host}/serving-endpoints/{LLM_ENDPOINT}/invocations"
_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# Modelos que NO aceptan 'temperature' (extended thinking, ej. opus-4-7)
_MODELS_NO_TEMPERATURE = {'databricks-claude-opus-4-7'}
_payload = {
    "messages": [
        {"role": "system", "content": "You translate technical dashboard labels to human-friendly Spanish. Output ONLY valid JSON."},
        {"role": "user", "content": prompt},
    ],
    "max_tokens": 8000,
}
if LLM_ENDPOINT not in _MODELS_NO_TEMPERATURE:
    _payload["temperature"] = 0.1
resp = _post_with_retry(url, _headers, _payload, 120)
# Fallback: si el modelo rechaza temperature dinámicamente
if resp.status_code == 400 and 'temperature' in resp.text.lower():
    _payload.pop('temperature', None)
    resp = _post_with_retry(url, _headers, _payload, 120)
resp.raise_for_status()
content = resp.json()["choices"][0]["message"]["content"].strip()
if content.startswith("```"):
    lines = content.split("\n")[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    content = "\n".join(lines)

translations = json.loads(content)
print(f"✓ {len(translations)} traducciones recibidas")
for t in translations[:5]:
    print(f"  \"{t['original'][:30]}\" → \"{t['humanized']}\"")

# Crear diccionario de traducción
title_map = {t['original']: t['humanized'] for t in translations}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aplicar títulos humanizados

# COMMAND ----------

updated_dashboard = json.loads(json.dumps(dashboard))
changes = 0

for p in updated_dashboard.get('pages', []):
    for w in p.get('layout', []):
        widget = w.get('widget', {})
        spec = widget.get('spec', {})
        frame = spec.get('frame', {})

        # frame.title
        if 'title' in frame and frame['title'] in title_map:
            old = frame['title']
            frame['title'] = title_map[old]
            changes += 1

        # encodings displayNames
        encodings = spec.get('encodings', {})
        for enc_key, enc_val in encodings.items():
            if isinstance(enc_val, dict) and 'displayName' in enc_val:
                if enc_val['displayName'] in title_map:
                    enc_val['displayName'] = title_map[enc_val['displayName']]
                    changes += 1
            elif isinstance(enc_val, list):
                for col in enc_val:
                    if isinstance(col, dict) and 'displayName' in col:
                        if col['displayName'] in title_map:
                            col['displayName'] = title_map[col['displayName']]
                            changes += 1

        # textbox_spec
        if 'textbox_spec' in widget and widget['textbox_spec'] in title_map:
            widget['textbox_spec'] = title_map[widget['textbox_spec']]
            changes += 1

        # multilineTextboxSpec
        mts = widget.get('multilineTextboxSpec', {})
        if isinstance(mts, dict) and 'lines' in mts:
            for i, line in enumerate(mts['lines']):
                if line in title_map:
                    mts['lines'][i] = title_map[line]
                    changes += 1

        # frame.description (filtros)
        if 'description' in frame and frame['description'] in title_map:
            frame['description'] = title_map[frame['description']]
            changes += 1

        # Counters con target: agregar porcentaje y cambiar SUM→AVG para grand totals
        if wtype == 'counter':
            target = encodings.get('target', {})
            if target and isinstance(target, dict) and 'fieldName' in target:
                # Agregar porcentaje
                if 'change' not in target:
                    target['change'] = {"type": "percent"}
                    changes += 1

                # Cambiar SUM→AVG en el field del target (grand totals)
                target_field = target.get('fieldName', '')
                if target_field.startswith('sum(') and 'global' in target_field.lower():
                    new_field = target_field.replace('sum(', 'avg(', 1)
                    target['fieldName'] = new_field
                    # También cambiar en los fields del query
                    for q in widget.get('queries', []):
                        query = q.get('query', {})
                        for f in query.get('fields', []):
                            if f.get('name') == target_field:
                                f['name'] = new_field
                                f['expression'] = f['expression'].replace('SUM(', 'AVG(', 1)
                    changes += 1
                    print(f"  + Counter {widget.get('name', '?')}: target SUM→AVG + porcentaje")

print(f"✓ {changes} títulos, descripciones y porcentajes actualizados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar dashboard

# COMMAND ----------

dashboard_json_str = json.dumps(updated_dashboard, indent=2, ensure_ascii=False)
content_b64 = base64.b64encode(dashboard_json_str.encode('utf-8')).decode('utf-8')

resp = requests.post(
    f"https://{host}/api/2.0/workspace/import",
    json={
        "path": DASHBOARD_PATH,
        "format": "AUTO",
        "content": content_b64,
        "overwrite": True,
    },
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)

if resp.status_code == 200:
    print(f"✓ Dashboard actualizado: {DASHBOARD_PATH}")
else:
    print(f"✗ Error ({resp.status_code}): {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen

# COMMAND ----------

print(f"{'='*60}")
print(f"TÍTULOS HUMANIZADOS")
print(f"{'='*60}")
print(f"\nDashboard: {DASHBOARD_PATH}")
print(f"Cambios: {changes}")
print()
for t in translations:
    if t['original'] != t['humanized']:
        print(f"  \"{t['original'][:40]}\" → \"{t['humanized']}\"")
