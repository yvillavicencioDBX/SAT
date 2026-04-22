# Databricks notebook source
# MAGIC %md
# MAGIC # Crear Capa Semántica del Dashboard
# MAGIC
# MAGIC Lee los SQLs de `dashboard_view_sqls` y genera el `.lvdash.json` con los datasets correctos.

# COMMAND ----------

#/Workspace/Users/yolanda.villavicencioibanez@databricks.com/SAT/KPI Coach Digital.lvdash.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

dbutils.widgets.text("catalog", "migracion_pbix", "Catálogo")
dbutils.widgets.text("schema", "couch", "Schema")
dbutils.widgets.text("dashboard_path", 
                     "/Workspace/Users/yolanda.villavicencioibanez@databricks.com/SAT/FATCA CRS Dashboard3.lvdash.json",
                     #"/Workspace/Users/yolanda.villavicencioibanez@databricks.com/SAT/KPI Coach Digital.lvdash.json", 
                     "Path del dashboard")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path")

print(f"Catálogo: {CATALOG}.{SCHEMA}")
print(f"Dashboard: {DASHBOARD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer SQLs de la tabla

# COMMAND ----------

import json

sql_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.dashboard_view_sqls").toPandas()
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

datasets = []
pages = []

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

    # Página correspondiente
    page_name = view_short.replace('v_dashboard_', '').replace('__', '_')
    pages.append({
        "name": page_name,
        "displayName": display_name,
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1"
    })

    print(f"Dataset: {ds_name}")
    print(f"  Display: {display_name}")
    print(f"  Query: {select_sql[:100]}...")
    print()

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
