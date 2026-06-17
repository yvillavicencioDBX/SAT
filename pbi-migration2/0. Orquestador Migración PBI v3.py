# Databricks notebook source
# MAGIC %md
# MAGIC # Orquestador — Migración Power BI a Databricks
# MAGIC
# MAGIC Cada celda llama a un notebook con sus parámetros visibles.
# MAGIC Corre celda por celda o todo de corrido.

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/sat_reportes/retenciones_fix_test/adls_sim/bulk")
print(f"Total archivos: {len(files)}")
spark.table("sat_reportes.retenciones_fix_test._bulk_guids").count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

# DBTITLE 1,nfddgeuungejcgjrnlvrgunnterhr
# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("pbix_path", "/Volumes/migracion_pbix/default/pbix/KPI_coach_digital.pbix", "Path del archivo .pbix")
dbutils.widgets.text("catalog", "migracion_pbix", "Catálogo destino en Unity Catalog")
dbutils.widgets.text("schema", "couch", "Schema destino")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo). Útil para múltiples .pbix en el mismo schema.")
dbutils.widgets.text("data_locations", "", "Ubicaciones de tablas físicas (lista catalog.schema separada por coma; vacío = usar destino)")
dbutils.widgets.text("dashboard_path", "", "Path del dashboard .lvdash.json (vacío = derivar del usuario actual)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4", "Endpoint del LLM")
dbutils.widgets.text("module_path", "", "Path de módulos Python (vacío = derivar del usuario actual)")

PBIX_PATH = dbutils.widgets.get("pbix_path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RUN_ID = dbutils.widgets.get("run_id").strip()
DATA_LOCATIONS = dbutils.widgets.get("data_locations")
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
MODULE_PATH = dbutils.widgets.get("module_path").strip() or f"/Workspace/Users/{_CURRENT_USER}/powerbi-model-analyzer"

BASE = f"/Users/{_CURRENT_USER}/SAT/pbi-migration2"  # ruta del bundle pbi-migration2

print(f"PBIX:      {PBIX_PATH}")
print(f"Catálogo:  {CATALOG}.{SCHEMA}")
print(f"Run ID:    {RUN_ID or '(sin sufijo)'}")
print(f"Data locs: {DATA_LOCATIONS or f'(usar destino {CATALOG}.{SCHEMA})'}")
print(f"Dashboard: {DASHBOARD_PATH}")
print(f"LLM:       {LLM_ENDPOINT}")
print(f"Módulos:   {MODULE_PATH}")
print(BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 0 — Extraer modelo del PBIX

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/0. extract_pbix_model",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "pbix_path": PBIX_PATH,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "data_locations": DATA_LOCATIONS,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 — Crear Metric Views base

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/1. create_base_metric_views",
    timeout_seconds=1800,
    arguments={
        "data_locations": DATA_LOCATIONS,
        "run_id": RUN_ID,
        "pbix_path": PBIX_PATH,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "module_path": MODULE_PATH,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1e — Crear diccionario de nombres PBI ↔ Databricks
# MAGIC
# MAGIC Genera `pbi_name_translator` con el mapeo de cada columna/measure/calc col del
# MAGIC modelo PBI a su nombre snake_case en las MVs. Útil para los pasos siguientes
# MAGIC (translation de DAX, generación de measures, mapping de filtros).

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/4. extract_visuals",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "pbix_path": PBIX_PATH,
        "catalog": CATALOG,
        "schema": SCHEMA,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2a — Agregar measures a las Metric Views
# MAGIC
# MAGIC IMPORTANTE: el LLM nombra measures algorítmicamente. El traductor (1e) se construye DESPUÉS,
# MAGIC con introspección de las MVs ya completas. (Antes el orden era 1e → 2, pero 1e necesita ver
# MAGIC las measures en las MVs para mapearlas correctamente; sin measures todo daba NO MATCH.)

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/2. create_measures",
    timeout_seconds=3600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "data_locations": DATA_LOCATIONS,
        "module_path": MODULE_PATH,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2.5 — Verificar measures y arreglar runtime errors con Claude
# MAGIC
# MAGIC Ejecuta cada measure (`SELECT MEASURE(...) FROM mv`) para detectar runtime errors como
# MAGIC `CAST_OVERFLOW`, `CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`. Las que fallan se pasan a
# MAGIC Claude con el error en contexto para corrección automática. Idempotente.

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/2.5 verify_and_fix_measures",
    timeout_seconds=3600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1f — Crear diccionario PBI ↔ Databricks (DESPUÉS de notebook 2)
# MAGIC
# MAGIC Genera `pbi_name_translator` mapeando nombres PBI a los snake_case reales de las MVs.
# MAGIC Ahora las MVs tienen las measures, así que el matching puede ser exacto.

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/1e. create_name_translator (param)",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2b — Crear vistas SQL para el dashboard

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/2.1 create_dashboard_views",
    timeout_seconds=1800,
    arguments={
        "data_locations": DATA_LOCATIONS,
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2c — Crear views por hoja con joins de filtros

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/2c. add_filter_joins_per_page (param)",
    timeout_seconds=600,
    arguments={
        "data_locations": DATA_LOCATIONS,
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 — Crear skeleton del dashboard (datasets + páginas)

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/3. create_dashboard_semantic",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4 — Extraer visuales del PBIX

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/4. extract_visuals",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "pbix_path": PBIX_PATH,
        "catalog": CATALOG,
        "schema": SCHEMA,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4b-1 — Extraer propiedades de visuales (sort, colores)

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/4b. extract_visual_props",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "pbix_path": PBIX_PATH,
        "catalog": CATALOG,
        "schema": SCHEMA,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 5 — Generar dashboard con Claude
# MAGIC
# MAGIC (Nota: el paso "4b. create_name_translator" se removió del orquestador porque
# MAGIC era redundante con "1e. create_name_translator (param)" que ya corrió antes
# MAGIC y produce el mismo `pbi_name_translator`.)

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/5. generate_dashboard (formacion-visuales)",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 5c — Refinar páginas con Claude (estilo Genie)
# MAGIC
# MAGIC Toma cada página del dashboard y la mejora: layout 12-col, format/colores,
# MAGIC títulos en español. Es post-procesamiento estético, no toca data ni queries.

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/5c. refine_pages_with_claude",
    timeout_seconds=1800,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
        "llm_endpoint": LLM_ENDPOINT,
    }
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 6 — Agregar filtros del Power BI

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/6. add_dashboard_filters",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 7 — Aplicar estilos

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/7. apply_styles",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 8 — Humanizar títulos con LLM

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/8. humanize_titles (param)",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
        "llm_endpoint": LLM_ENDPOINT,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 9 — Catálogo de artefactos + seguimiento del dashboard
# MAGIC
# MAGIC Genera un reporte que explica qué creó el pipeline (qué tabla/vista contiene qué)
# MAGIC y queries a `system.*` para tracking de uso, performance, errores y costo del dashboard.

# COMMAND ----------

dbutils.notebook.run(
    f"{BASE}/10. catalog_y_seguimiento",
    timeout_seconds=600,
    arguments={
        "run_id": RUN_ID,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "dashboard_path": DASHBOARD_PATH,
        "days_back": "30",
    }
)
