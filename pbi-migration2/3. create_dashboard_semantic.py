# Databricks notebook source
# MAGIC %md
# MAGIC # Crear Capa Semántica del Dashboard
# MAGIC
# MAGIC Lee los SQLs de `dashboard_view_sqls` y genera el `.lvdash.json` con los datasets correctos.

# COMMAND ----------

# Nota: el path del dashboard ahora viene del widget `dashboard_path` (vacío = derivar del user actual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "migracion_pbix", "Catálogo")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("schema", "couch", "Schema")
dbutils.widgets.text("dashboard_path", "", "Path del dashboard (vacío = derivar del usuario actual)")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"


RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
print(f"Catálogo: {CATALOG}.{SCHEMA}")
print(f"Dashboard: {DASHBOARD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer SQLs de la tabla

# COMMAND ----------

import json

sql_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('dashboard_view_sqls')}").toPandas()
print(f"{len(sql_df)} vistas encontradas:")
for _, row in sql_df.iterrows():
    print(f"  {row['vista_dashboard']}: {row['num_dimensiones']} dims, {row['num_measures']} measures")
display(sql_df[['vista_dashboard', 'metric_view', 'num_dimensiones', 'num_measures', 'measures']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generar datasets del dashboard
# MAGIC
# MAGIC Cada SQL de la tabla se convierte en un dataset del `.lvdash.json`.
# MAGIC El query del dataset es el SELECT interno (sin el CREATE VIEW).

# COMMAND ----------

import re

datasets = []

# 3a. Datasets — uno por cada v_dashboard_* (Metric View)
for _, row in sql_df.iterrows():
    vista = row['vista_dashboard']
    mv = row['metric_view']
    sql = row['sql']
    measures = row['measures']
    dims = row['dimensiones']

    # Extraer el SELECT del CREATE VIEW
    select_idx = sql.upper().find('SELECT')
    if select_idx >= 0:
        select_sql = sql[select_idx:]
    else:
        select_sql = sql

    # Nombre del dataset: derivar del nombre de la vista
    view_short = vista.replace(f'{CATALOG}.{SCHEMA}.', '')
    ds_name = view_short.replace('v_dashboard_', 'ds_')

    # Display name legible
    display_name = view_short.replace('v_dashboard_', '').replace('_', ' ').title()

    datasets.append({
        "name": ds_name,
        "displayName": display_name,
        "queryLines": [select_sql]
    })

    print(f"Dataset: {ds_name}")
    print(f"  Display: {display_name}")
    print(f"  Query: {select_sql[:100]}...")
    print()

# 3b. Pages — una por cada pagina real de Power BI (desde pbi_visuals)
# GUARDRAIL: verificar que pbi_visuals existe en el MISMO catalog/schema que dashboard_view_sqls.
# Si en la celda anterior cambiaste el widget catalog/schema, esto detecta la inconsistencia.
_check_visuals = spark.sql(f"""
    SELECT COUNT(*) c FROM {CATALOG}.{SCHEMA}.{_t('pbi_visuals')}
""").collect()[0]['c']
if _check_visuals == 0:
    raise RuntimeError(
        f"pbi_visuals en {CATALOG}.{SCHEMA} está vacío. "
        f"Verifica que catalog/schema apuntan al mismo PBIX que dashboard_view_sqls. "
        f"Si cambiaste el widget mid-run, re-corre TODAS las celdas desde el principio."
    )

pages = []
seen_ids = set()
try:
    pbi_pages_df = spark.sql(f"""
        SELECT page, MIN(CAST(page_order AS INT)) AS page_order
        FROM {CATALOG}.{SCHEMA}.{_t('pbi_visuals')}
        WHERE page IS NOT NULL AND page != '?'
        GROUP BY page
        ORDER BY page_order
    """).toPandas()
    for _, row in pbi_pages_df.iterrows():
        pbi_name = str(row['page']).strip()
        if not pbi_name:
            continue
        # Slug seguro: lowercase, alfanumerico + underscore
        page_id = re.sub(r'[^a-zA-Z0-9_]+', '_', pbi_name.lower()).strip('_') or 'page'
        # Garantizar unicidad
        base = page_id
        n = 1
        while page_id in seen_ids:
            n += 1
            page_id = f"{base}_{n}"
        seen_ids.add(page_id)
        pages.append({
            "name": page_id,
            "displayName": pbi_name,
            "pageType": "PAGE_TYPE_CANVAS",
            "layoutVersion": "GRID_V1"
        })
    print(f"Pages desde PBI: {len(pages)}")
    for p in pages:
        print(f"  '{p['displayName']}' -> name={p['name']}")
except Exception as e:
    print(f"WARN: no pude leer pbi_visuals ({str(e)[:120]}); fallback a una page por dataset")
    for ds in datasets:
        page_id = ds['name'].replace('ds_', '') or 'page'
        pages.append({
            "name": page_id,
            "displayName": ds['displayName'],
            "pageType": "PAGE_TYPE_CANVAS",
            "layoutVersion": "GRID_V1"
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generar el .lvdash.json

# COMMAND ----------

dashboard = {
    "datasets": datasets,
    "pages": pages,
    "uiSettings": {
        "theme": {
            "widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"
        },
        "applyModeEnabled": False
    }
}

dashboard_json = json.dumps(dashboard, indent=2, ensure_ascii=False)
print(dashboard_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar el dashboard en el workspace

# COMMAND ----------

import requests, base64

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

# Codificar el JSON en base64
content_b64 = base64.b64encode(dashboard_json.encode('utf-8')).decode('utf-8')

# Subir al workspace via API REST
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
    print(f"✓ Dashboard guardado en: {DASHBOARD_PATH}")
else:
    print(f"✗ Error ({resp.status_code}): {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumen

# COMMAND ----------

print(f"{'='*60}")
print(f"CAPA SEMÁNTICA DEL DASHBOARD")
print(f"{'='*60}")
print(f"\nDashboard: {DASHBOARD_PATH}")
print(f"Datasets: {len(datasets)}")
print(f"Páginas: {len(pages)}")
print()
for ds in datasets:
    print(f"  {ds['name']}: {ds['displayName']}")
