# Databricks notebook source
# MAGIC %md
# MAGIC # 10. Catálogo de artefactos + Seguimiento del dashboard
# MAGIC
# MAGIC Dos secciones:
# MAGIC
# MAGIC **Parte A — Catálogo del proceso**: explica qué tabla/vista creó cada paso del pipeline,
# MAGIC qué contiene, y cómo consultarla con un ejemplo SQL ejecutable.
# MAGIC
# MAGIC **Parte B — Seguimiento del dashboard**: queries a las `system.*` para entender
# MAGIC uso, performance, errores y costo del dashboard generado.

# COMMAND ----------

# DBTITLE 1,Parámetros
import re

try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo destino")
dbutils.widgets.text("schema", "default", "Schema destino")
dbutils.widgets.text("run_id", "", "Sufijo run_id (vacío = sin sufijo)")
dbutils.widgets.text("dashboard_path", "", "Path del .lvdash.json (vacío = ~/SAT/Dashboard.lvdash.json)")
dbutils.widgets.text("days_back", "30", "Días hacia atrás para análisis de uso (system tables)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()
RUN_ID  = dbutils.widgets.get("run_id").strip()
SUFFIX  = f"_{RUN_ID}" if RUN_ID else ""
def _t(name): return f"{name}{SUFFIX}"
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"
DAYS_BACK = int(dbutils.widgets.get("days_back") or "30")

print(f"Catálogo:  {CATALOG}.{SCHEMA}")
print(f"Run ID:    {RUN_ID or '(sin sufijo)'}")
print(f"Dashboard: {DASHBOARD_PATH}")
print(f"Días:      {DAYS_BACK}")

# Algunas operaciones (SHOW VIEWS IN cat.schema) requieren que el catálogo esté activo.
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE A — Catálogo de artefactos del pipeline
# MAGIC
# MAGIC Por cada paso del pipeline se materializa una o más tablas/vistas. Aquí están listadas
# MAGIC en orden cronológico de creación, con la descripción de cada una.

# COMMAND ----------

# DBTITLE 1,A1. Tablas de control creadas por notebook 0 (extract_pbix_model)
print("""
TABLA: pbi_measures
  Qué tiene: cada DAX measure del .pbix con su tabla, página, slicers que la filtran
  Cuándo: paso 0 (extract_pbix_model)
  Columnas: Measure, Tabla, DAX, Pages, Slicers, ...

TABLA: pbi_relationships
  Qué tiene: relaciones entre tablas del modelo PBI (FromTable, ToTable, FromColumn, ToColumn, IsActive)
  Cuándo: paso 0

TABLA: pbi_page_filters
  Qué tiene: slicers que filtran cada página

TABLA: pbi_context_filters
  Qué tiene: filtros CALCULATE/FILTER detectados dentro del DAX

TABLA: pbi_calculated_columns
  Qué tiene: calc cols DAX (TableName, ColumnName, Expression)

TABLA: pbi_table_mapping
  Qué tiene: mapeo de cada tabla PBI a su tabla en Unity Catalog (FQN completo)
""")

# Mostrar conteos
_t0_tables = ['pbi_measures', 'pbi_relationships', 'pbi_page_filters',
              'pbi_context_filters', 'pbi_calculated_columns', 'pbi_table_mapping']
for t in _t0_tables:
    try:
        n = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.{SCHEMA}.{_t(t)}").collect()[0]['c']
        print(f"  {_t(t)}: {n} filas")
    except Exception as e:
        print(f"  {_t(t)}: (no existe — {str(e)[:60]})")

# COMMAND ----------

# DBTITLE 1,A1. Ejemplo — Top 10 measures más complejas (más caracteres DAX)
spark.sql(f"""
    SELECT Measure, Tabla, LENGTH(DAX) AS dax_chars, SUBSTR(DAX, 1, 80) AS dax_snippet
    FROM {CATALOG}.{SCHEMA}.{_t('pbi_measures')}
    ORDER BY dax_chars DESC
    LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,A2. Notebook 1 → Metric Views base + measure_to_view_mapping
print("""
VISTA: mv_<table>{SUFFIX}  (Metric Views base — una por tabla física relevante)
  Qué tiene: capa semántica gobernada por UC con dimensions, joins y measures.
             Cada measure es la traducción SQL de un DAX.
  Cuándo: paso 1 (create_base_metric_views) + paso 2 (create_measures)
  Cómo se consulta: SELECT MEASURE(<name>) FROM <mv> GROUP BY <dim>

VISTA: lod_<table>{SUFFIX}  (Vistas LOD — pre-cómputo de window functions)
  Qué tiene: extensión de la tabla source con columnas pre-calculadas (window functions
             que las Metric Views no pueden ejecutar directamente). Usada como source de mv_.

TABLA: measure_to_view_mapping{SUFFIX}
  Qué tiene: mapeo (resuelto en notebook 1) de cada PBI measure a su MV destino + DAX original
  Columnas: pbi_measure_name, pbi_table, base_table, target_mv, dax, assignment_method
""".format(SUFFIX=SUFFIX))

# Listar las MVs creadas
mvs = spark.sql(f"SHOW VIEWS IN {CATALOG}.{SCHEMA}").collect()
mvs_run = [r.viewName for r in mvs
           if r.viewName.startswith('mv_') and (not SUFFIX or r.viewName.endswith(SUFFIX))]
print(f"Metric Views de esta corrida ({len(mvs_run)}):")
for m in mvs_run:
    print(f"  {CATALOG}.{SCHEMA}.{m}")

# COMMAND ----------

# DBTITLE 1,A2. Ejemplo — Mapping measure → MV (cuántas measures por view)
spark.sql(f"""
    SELECT target_mv,
           COUNT(*) AS measures,
           COUNT(CASE WHEN target_mv = '' THEN 1 END) AS sin_asignar
    FROM {CATALOG}.{SCHEMA}.{_t('measure_to_view_mapping')}
    GROUP BY target_mv
    ORDER BY measures DESC
""").display()

# COMMAND ----------

# DBTITLE 1,A3. Notebook 4 → visuales del PBI (page-level)
print("""
TABLA: pbi_visuals{SUFFIX}
  Qué tiene: cada visual del .pbix con su página, tipo, título, posición.
  Cuándo: paso 4 (extract_visuals)

TABLA: pbi_visual_fields{SUFFIX}
  Qué tiene: campos (column o measure) que usa cada visual
  Columnas: visual_id, page, role, table, column, measure_name

TABLA: pbi_visual_props{SUFFIX}
  Qué tiene: propiedades adicionales (sort, color, format) por visual
""".format(SUFFIX=SUFFIX))

for t in ['pbi_visuals', 'pbi_visual_fields', 'pbi_visual_props']:
    try:
        n = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.{SCHEMA}.{_t(t)}").collect()[0]['c']
        print(f"  {_t(t)}: {n} filas")
    except Exception as e:
        print(f"  {_t(t)}: (no existe)")

# COMMAND ----------

# DBTITLE 1,A3. Ejemplo — Visuales por página y tipo
spark.sql(f"""
    SELECT page, visual_type, COUNT(*) AS n
    FROM {CATALOG}.{SCHEMA}.{_t('pbi_visuals')}
    WHERE page IS NOT NULL AND page != '?'
    GROUP BY page, visual_type
    ORDER BY page, visual_type
""").display()

# COMMAND ----------

# DBTITLE 1,A4. Notebook 1e → traductor de nombres PBI ↔ Databricks
print("""
TABLA: pbi_name_translator{SUFFIX}
  Qué tiene: para cada nombre PBI (column o measure) qué nombre snake_case tiene en las MVs
  Columnas: pbi_table, pbi_name, pbi_type (column/measure), databricks_name, metric_view, match_method
  Match method: exact, partial, flat, fuzzy(%), NO MATCH
  Uso: los notebooks downstream (5, 8) lo consultan para mapear nombres al generar widgets/títulos
""".format(SUFFIX=SUFFIX))

# COMMAND ----------

# DBTITLE 1,A4. Ejemplo — cobertura del traductor por tabla
spark.sql(f"""
    SELECT pbi_table,
           COUNT(*) AS total,
           SUM(CASE WHEN match_method = 'exact'   THEN 1 ELSE 0 END) AS exact_match,
           SUM(CASE WHEN match_method = 'partial' THEN 1 ELSE 0 END) AS partial_match,
           SUM(CASE WHEN match_method LIKE 'fuzzy%' THEN 1 ELSE 0 END) AS fuzzy_match,
           SUM(CASE WHEN match_method = 'NO MATCH' THEN 1 ELSE 0 END) AS no_match
    FROM {CATALOG}.{SCHEMA}.{_t('pbi_name_translator')}
    GROUP BY pbi_table
    ORDER BY no_match DESC, total DESC
""").display()

# COMMAND ----------

# DBTITLE 1,A5. Notebook 2.1 → dashboard_view_sqls
print("""
TABLA: dashboard_view_sqls{SUFFIX}
  Qué tiene: por cada MV, el SQL de la vista que va a alimentar a Lakeview (con SELECT
             explícito de dimensions + MEASURE(measure) para cada measure)
  Columnas: vista_dashboard, metric_view, dimensiones, measures, sql

VISTA: v_dashboard_<table>{SUFFIX}
  Qué tiene: una fila por combinación de dimensions + valores de measures evaluados.
             Es la fuente directa de los datasets del Lakeview.
""".format(SUFFIX=SUFFIX))

# Listar v_dashboard_ creadas
vds = spark.sql(f"SHOW VIEWS IN {CATALOG}.{SCHEMA}").collect()
vds_run = [r.viewName for r in vds
           if r.viewName.startswith('v_dashboard_') and (not SUFFIX or r.viewName.endswith(SUFFIX))]
print(f"Vistas v_dashboard_* de esta corrida ({len(vds_run)}):")
for v in vds_run:
    print(f"  {CATALOG}.{SCHEMA}.{v}")

# COMMAND ----------

# DBTITLE 1,A6. Notebook 2c → vistas por página con joins de filtros
print("""
VISTA: v_dashboard_page_<page_slug>{SUFFIX}
  Qué tiene: combina la v_dashboard_<table> + LEFT JOIN a dim_tables si la página tiene
             slicers cuya columna no está expuesta en la vista base.
  Uso: filters_dashboard usan esta vista para que los slicers de cada página funcionen.
""".format(SUFFIX=SUFFIX))

pages = [r.viewName for r in vds
         if r.viewName.startswith('v_dashboard_page_') and (not SUFFIX or r.viewName.endswith(SUFFIX))]
print(f"Vistas por página ({len(pages)}):")
for p in pages:
    print(f"  {CATALOG}.{SCHEMA}.{p}")

# COMMAND ----------

# DBTITLE 1,Resumen — todos los artefactos materializados por la corrida
all_objects = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
by_kind = {'pbi_': [], 'mv_': [], 'lod_': [], 'v_dashboard_page_': [], 'v_dashboard_': [],
           'measure_to_view_mapping': [], 'dashboard_view_sqls': []}
for r in all_objects:
    t = r.tableName
    if SUFFIX and not (t.endswith(SUFFIX) or not any(t.startswith(k) for k in by_kind)):
        # filtrar por sufijo (solo aplica si las tablas se sufijan)
        pass
    for kind in by_kind:
        if t.startswith(kind):
            if not SUFFIX or t.endswith(SUFFIX):
                by_kind[kind].append(t)
            break

print(f"\nResumen por tipo (run_id={RUN_ID or '(none)'}):")
for kind, tabs in by_kind.items():
    if tabs:
        print(f"  {kind}*: {len(tabs)} → {tabs[:3]}{' ...' if len(tabs) > 3 else ''}")

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE B — Seguimiento de uso y performance del dashboard
# MAGIC
# MAGIC Consultas a `system.*` para entender:
# MAGIC - **Quién** está usando el dashboard
# MAGIC - **Qué** queries son las más lentas / costosas
# MAGIC - **Cuándo** se usa más (horarios pico)
# MAGIC - **Errores** que están viendo los usuarios
# MAGIC - **Costo** (DBUs) de los queries del dashboard
# MAGIC
# MAGIC Requiere acceso a `system.query.history` y `system.access.audit`. Si no tienes,
# MAGIC pide a un admin que habilite `system` schema en tu workspace.

# COMMAND ----------

# DBTITLE 1,B1. Verificar acceso a system tables
_sys_tables_check = [
    ('system.query.history', 'Historial de queries — usuario, tiempo, bytes, costo'),
    ('system.access.audit', 'Auditoría de acceso a workspace objects'),
    ('system.billing.usage', 'Consumo de DBUs por workload'),
    ('system.compute.warehouses', 'SQL Warehouses (catálogo)'),
]
for t, desc in _sys_tables_check:
    try:
        spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
        print(f"  ✓ {t}  — {desc}")
    except Exception as e:
        print(f"  ✗ {t} — sin acceso ({str(e)[:80]})")

# COMMAND ----------

# DBTITLE 1,B2. Patrón para detectar queries del dashboard
# Las queries del dashboard tocan v_dashboard_* / v_dashboard_page_*.
# Filtramos system.query.history por queries que mencionen esas vistas.
_TABLE_LIKE_PATTERN = f"%{CATALOG}.{SCHEMA}.v_dashboard%{SUFFIX}%"
print(f"Patrón para identificar queries del dashboard:")
print(f"  statement_text LIKE '{_TABLE_LIKE_PATTERN}'")
print(f"  (Tabla a cazar: cualquier query que toque las vistas v_dashboard_*_{SUFFIX or ''})")

# COMMAND ----------

# DBTITLE 1,B3. Top 20 queries más lentas del dashboard (últimos N días)
spark.sql(f"""
    SELECT
      executed_by,
      SUBSTR(statement_text, 1, 120) AS query_snippet,
      total_duration_ms / 1000.0 AS duration_s,
      read_bytes / 1e9              AS read_gb,
      execution_status,
      start_time
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
    ORDER BY total_duration_ms DESC
    LIMIT 20
""").display()

# COMMAND ----------

# DBTITLE 1,B4. Usuarios activos — quiénes consultan el dashboard
spark.sql(f"""
    SELECT
      executed_by,
      COUNT(*)                        AS queries,
      COUNT(DISTINCT DATE(start_time))AS dias_activos,
      MAX(start_time)                 AS ultima_query,
      AVG(total_duration_ms) / 1000.0 AS avg_duration_s
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
    GROUP BY executed_by
    ORDER BY queries DESC
""").display()

# COMMAND ----------

# DBTITLE 1,B5. Errores recurrentes — queries que fallan
spark.sql(f"""
    SELECT
      execution_status,
      COUNT(*)                                 AS n,
      SUBSTR(MIN(error_message), 1, 200)       AS sample_error,
      MAX(start_time)                          AS ultima_vez
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
      AND execution_status != 'FINISHED'
    GROUP BY execution_status
    ORDER BY n DESC
""").display()

# COMMAND ----------

# DBTITLE 1,B6. Volumen diario — cuánto se usa
spark.sql(f"""
    SELECT
      DATE(start_time)                AS dia,
      COUNT(*)                        AS queries,
      COUNT(DISTINCT executed_by)     AS usuarios,
      SUM(total_duration_ms) / 1000.0 AS total_duration_s,
      SUM(read_bytes) / 1e9           AS total_read_gb
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
    GROUP BY DATE(start_time)
    ORDER BY dia DESC
""").display()

# COMMAND ----------

# DBTITLE 1,B7. Queries por hora del día — detectar horarios pico
spark.sql(f"""
    SELECT
      HOUR(start_time) AS hora,
      COUNT(*)         AS queries,
      AVG(total_duration_ms) / 1000.0 AS avg_duration_s
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
    GROUP BY HOUR(start_time)
    ORDER BY hora
""").display()

# COMMAND ----------

# DBTITLE 1,B8. Costo aproximado del dashboard (DBUs y USD)
# system.billing.usage contiene el costo por SKU. Filtramos al warehouse del dashboard.
spark.sql(f"""
    SELECT
      DATE(usage_start_time)             AS dia,
      sku_name,
      SUM(usage_quantity)                AS dbus,
      SUM(usage_quantity * 0.55)         AS usd_approx  -- $0.55/DBU aprox SQL Serverless; ajusta a tu pricing
    FROM system.billing.usage
    WHERE usage_start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
      AND sku_name LIKE '%SQL%'
    GROUP BY DATE(usage_start_time), sku_name
    ORDER BY dia DESC, dbus DESC
""").display()

# COMMAND ----------

# DBTITLE 1,B9. Top measures más usadas en el dashboard
# Parsea los statement_text para encontrar referencias MEASURE(name)
spark.sql(f"""
    WITH measure_refs AS (
      SELECT
        executed_by,
        regexp_extract(statement_text, 'MEASURE\\\\(`?([a-zA-Z0-9_]+)`?\\\\)', 1) AS measure_name,
        total_duration_ms
      FROM system.query.history
      WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {DAYS_BACK} DAYS
        AND statement_text LIKE '{_TABLE_LIKE_PATTERN}'
        AND statement_text LIKE '%MEASURE(%'
    )
    SELECT
      measure_name,
      COUNT(*)               AS queries,
      AVG(total_duration_ms) AS avg_duration_ms
    FROM measure_refs
    WHERE measure_name != ''
    GROUP BY measure_name
    ORDER BY queries DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusión
# MAGIC
# MAGIC Tienes ahora:
# MAGIC - **Parte A**: el catálogo completo de qué creó cada paso del pipeline y cómo consultarlo.
# MAGIC - **Parte B**: queries listos para entender uso, performance, errores y costo del dashboard
# MAGIC   a través de las system tables.
# MAGIC
# MAGIC Puedes:
# MAGIC 1. Programar este notebook como **job recurrente** (semanal) para tener un health-check.
# MAGIC 2. Usar las queries B3-B9 como base para un **dashboard de seguimiento** dedicado.
# MAGIC 3. Configurar alertas SQL para errores recurrentes (B5) o degradación de performance (B3).

