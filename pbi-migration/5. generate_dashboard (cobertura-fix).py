# Databricks notebook source
# MAGIC %md
# MAGIC # Generar Dashboard Databricks desde Power BI
# MAGIC
# MAGIC Lee `pbi_visuals`, `pbi_visual_fields` y `dashboard_view_sqls` para generar
# MAGIC un `.lvdash.json` que replique el dashboard de Power BI usando las Metrics Views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

import json, re, requests, base64

# Detecta el usuario actual para construir defaults dinámicos (no hardcoded)
try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("dashboard_path", "", "Path del dashboard (vacío = derivar del usuario actual)")
# Modelo de Claude vía Databricks Model Serving (Foundation Model APIs).
# Disponibles READY en este workspace (verificado):
#   databricks-claude-opus-4-7      ← más capaz (mejor para tareas complejas)
#   databricks-claude-opus-4-6
#   databricks-claude-opus-4-5
#   databricks-claude-opus-4-1
#   databricks-claude-sonnet-4-6    ← balance capacidad / velocidad
#   databricks-claude-sonnet-4-5
#   databricks-claude-sonnet-4
#   databricks-claude-haiku-4-5     ← más rápido / barato
dbutils.widgets.text("llm_endpoint", "databricks-claude-opus-4-7", "Endpoint LLM")
dbutils.widgets.text("max_tokens", "16000", "max_tokens del modelo")
dbutils.widgets.text("temperature", "0.1", "temperature del modelo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DASHBOARD_PATH = dbutils.widgets.get("dashboard_path").strip() or f"/Users/{_CURRENT_USER}/SAT/Dashboard.lvdash.json"
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint").strip() or "databricks-claude-opus-4-7"
MAX_TOKENS = int(dbutils.widgets.get("max_tokens") or "16000")
TEMPERATURE = float(dbutils.widgets.get("temperature") or "0.1")


RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
print(f"Catálogo: {CATALOG}.{SCHEMA}")
print(f"Dashboard: {DASHBOARD_PATH}")
print(f"Modelo:    {LLM_ENDPOINT}  (max_tokens={MAX_TOKENS}, temperature={TEMPERATURE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer tablas fuente

# COMMAND ----------

visuals_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_visuals')}").toPandas()
fields_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_visual_fields')}").toPandas()
sqls_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('dashboard_view_sqls')}").toPandas()

# Leer propiedades de los visuales (sort, colores, etc.)
try:
    visual_props_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_visual_props')}").toPandas()
    print(f"Propiedades de visuales: {len(visual_props_df)}")
except:
    visual_props_df = pd.DataFrame()
    print("⚠ Tabla pbi_visual_props no encontrada")

# Leer traductor de nombres PBI → Databricks
try:
    translator_df = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_name_translator')}").toPandas()
    _name_map = {}
    for _, t in translator_df.iterrows():
        if t.get('match_method', '') != 'NO MATCH' and t.get('databricks_name', ''):
            _name_map[(t['pbi_table'].lower(), t['pbi_name'].lower())] = t['databricks_name']
            _name_map[('', t['pbi_name'].lower())] = t['databricks_name']
    print(f"Traductor: {len(_name_map)} nombres mapeados")
except:
    _name_map = {}
    print("⚠ Traductor no disponible")

def _translate(table, name):
    if not _name_map:
        return name
    r = _name_map.get((table.lower(), name.lower()))
    if r: return r
    r = _name_map.get(('', name.lower()))
    if r: return r
    return name

print(f"Visuales: {len(visuals_df)}")
print(f"Campos: {len(fields_df)}")
print(f"SQLs de dashboard: {len(sqls_df)}")

# Filtrar solo visuales con datos (excluir image, textbox, shape, actionButton)
# Filtro INVERSO: excluir solo los que claramente no se pueden traducir.
# Decorativos (sin datos), botones, imágenes y custom visuals desconocidos.
# Todo lo demás (charts, tables, cards, pivots, etc.) entra al pipeline.
excluded_types = ['image', 'textbox', 'shape', 'actionButton', 'unknown']
# Custom visuals (nombres con números/IDs del marketplace) — patrón: termina con número largo
def _is_custom_visual(t):
    return bool(re.search(r'\d{10,}$', str(t)))

data_visuals = visuals_df[
    ~visuals_df['visual_type'].isin(excluded_types)
    & ~visuals_df['visual_type'].apply(_is_custom_visual)
].copy()
print(f"Visuales con datos: {len(data_visuals)}")
print(f"\nTipos de visual con datos:")
print(data_visuals['visual_type'].value_counts().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar contexto para Claude

# COMMAND ----------

# Mapeo Power BI tipo visual → Databricks tipo visual
PBI_TO_DATABRICKS = {
    'columnChart': 'bar',
    'clusteredColumnChart': 'bar',
    'stackedColumnChart': 'bar',
    'barChart': 'bar',
    'clusteredBarChart': 'bar',
    'stackedBarChart': 'bar',
    'lineChart': 'line',
    'pieChart': 'pie',
    'donutChart': 'pie',
    'gauge': 'counter',
    'card': 'counter',
    'kpi': 'counter',
    'multiRowCard': 'counter',
    'tableEx': 'table',
    'table': 'table',
    'tableExUnordered': 'table',
    'pivotTable': 'pivot',
    'matrix': 'pivot',
    'cardVisual': 'counter',
    # Mapas — Lakeview soporta pointMap (puntos lat/long) y choropleth (regiones coloreadas)
    'map': 'pointMap',
    'filledMap': 'choropleth',
    'azureMap': 'pointMap',
    'shapeMap': 'choropleth',
    'slicer': 'SLICER',
    'funnel': 'bar',
    'treemap': 'pie',
    'waterfallChart': 'bar',
    'scatterChart': 'scatter',
}

# Construir resumen de visuales por página
pages_summary = {}
for _, v in data_visuals.iterrows():
    page = v['page']
    if page not in pages_summary:
        pages_summary[page] = []

    vid = v['visual_id']
    vtype = v['visual_type']
    db_type = PBI_TO_DATABRICKS.get(vtype, vtype)
    title = v['title']

    # Campos de este visual
    vfields = fields_df[fields_df['visual_id'] == str(vid)]
    field_list = []
    for _, f in vfields.iterrows():
        if f['field_type'] == 'Measure':
            db_name = _translate(f.get('measure_table', f.get('table', '')), f['measure_name'])
            field_list.append(f"Measure: {db_name} (role: {f['role']})")
        elif f['field_type'] == 'Column':
            db_name = _translate(f['table'], f['column'])
            field_list.append(f"Column: {db_name} (role: {f['role']})")
        elif f['field_type'].startswith('Aggregation'):
            db_name = _translate(f['table'], f['column'])
            field_list.append(f"{f['field_type']}: {db_name} (role: {f['role']})")

    # Propiedades del visual (sort, colores, etc.)
    props_list = []
    if not visual_props_df.empty:
        vprops = visual_props_df[visual_props_df['visual_id'] == str(vid)]
        for _, p in vprops.iterrows():
            props_list.append(f"{p['property_type']}: {p['key']} = {p['value']}")

    pages_summary[page].append({
        'visual_id': vid,
        'pbi_type': vtype,
        'databricks_type': db_type,
        'title': title,
        'fields': field_list,
        'properties': props_list,
    })

# Construir resumen de datasets disponibles
datasets_summary = ""
for _, row in sqls_df.iterrows():
    vista = row['vista_dashboard']
    mv = row['metric_view']
    dims = row['dimensiones']
    measures = row['measures']
    # Extraer el SELECT del CREATE VIEW
    sql = row['sql']
    select_idx = sql.upper().find('SELECT')
    select_sql = sql[select_idx:] if select_idx >= 0 else sql
    datasets_summary += f"\nDataset: {vista}\n  Metrics View: {mv}\n  Dimensions: {dims}\n  Measures: {measures}\n  Query: {select_sql[:200]}...\n"

# Construir el prompt con los visuales por página
visuals_prompt = ""
for page, visuals in sorted(pages_summary.items()):
    visuals_prompt += f"\n\n=== PAGE: {page} ===\n"
    slicers = [v for v in visuals if v['databricks_type'] == 'SLICER']
    charts = [v for v in visuals if v['databricks_type'] != 'SLICER']

    if slicers:
        visuals_prompt += f"\nSlicers ({len(slicers)}):\n"
        for s in slicers:
            visuals_prompt += f"  - {', '.join(s['fields'])}\n"

    if charts:
        visuals_prompt += f"\nCharts/Tables ({len(charts)}):\n"
        for c in charts:
            visuals_prompt += f"  - [{c['databricks_type']}] {c['title'] or '(sin título)'}\n"
            for f in c['fields']:
                visuals_prompt += f"      {f}\n"
            if c.get('properties'):
                visuals_prompt += f"      Properties:\n"
                for prop in c['properties']:
                    visuals_prompt += f"        {prop}\n"

print("=== CONTEXTO PARA CLAUDE ===")
print(f"\nPáginas: {len(pages_summary)}")
for page, visuals in sorted(pages_summary.items()):
    slicers = [v for v in visuals if v['databricks_type'] == 'SLICER']
    charts = [v for v in visuals if v['databricks_type'] != 'SLICER']
    print(f"  {page}: {len(slicers)} slicers, {len(charts)} charts/tables")

print(f"\nDatasets: {len(sqls_df)}")
for _, row in sqls_df.iterrows():
    print(f"  {row['vista_dashboard']}: {row['measures']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Llamar a Claude para generar el dashboard

# COMMAND ----------

# Widget rules_path: si está vacío, busca REGLAS_DASHBOARD.md en el directorio del notebook
# (luego en el padre). Esto hace el pipeline portable a cualquier workspace/usuario.
import os
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _nb_dir  = '/'.join(_nb_path.split('/')[:-1])
    _parent  = '/'.join(_nb_dir.split('/')[:-1])
    _candidates = [
        f"/Workspace{_nb_dir}/REGLAS_DASHBOARD.md",
        f"/Workspace{_parent}/REGLAS_DASHBOARD.md",
    ]
    _default_rules = next((p for p in _candidates if os.path.exists(p)), _candidates[0])
except Exception:
    _default_rules = ""

try:
    dbutils.widgets.text("rules_path", "", "Path a REGLAS_DASHBOARD.md (vacío = junto al notebook o en el padre)")
except Exception:
    pass
RULES_PATH = dbutils.widgets.get("rules_path").strip() or _default_rules
if not RULES_PATH or not os.path.exists(RULES_PATH):
    raise FileNotFoundError(
        f"No encontré REGLAS_DASHBOARD.md. Probé: {_candidates if not RULES_PATH else [RULES_PATH]}. "
        f"Pásalo explícitamente via widget 'rules_path'."
    )
with open(RULES_PATH, 'r') as f:
    DASHBOARD_RULES = f.read()
print(f"✓ Reglas leídas de {RULES_PATH} ({len(DASHBOARD_RULES)} caracteres)")

# Ejemplos de widgets funcionales de un dashboard real + spec de todos los tipos
WIDGET_EXAMPLES = """
=== WIDGET JSON STRUCTURE (you MUST follow this exact structure for ALL widgets) ===

CRITICAL STRUCTURE — every widget follows this pattern:
{
  "widget": {
    "name": "unique_widget_id",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "dataset_name_here",
      "fields": [
        {"name": "field_alias", "expression": "SUM(`col`)  or  `col`"}
      ],
      "disaggregated": false_or_true
    }}],
    "spec": {
      "version": 2,
      "widgetType": "type_here",
      "encodings": { ... },
      "frame": {"showTitle": true, "title": "Widget Title"}
    }
  },
  "position": {"x": 0, "y": 0, "width": 6, "height": 4}
}

WRONG (will break): widget.queries[].datasetName  ← MISSING the .query wrapper
RIGHT: widget.queries[].query.datasetName

Each page MUST have "name" (short ID) AND "displayName" (human-readable).

=== COMPLETE WIDGET TYPE CATALOG (19 types) ===

All types use version: 2, disaggregated: false, fields with SUM(`col`) or aggregation expressions.
Exception: if you want raw/disaggregated data, use disaggregated: true and `col` without SUM.

--- 1. COUNTER ---
widgetType: "counter"
disaggregated: false, fields use SUM(`col`)
encodings: { "value": {"fieldName": "sum(col)"} }
Optional: "target" encoding for the global/max value (NOT "comparison")
fields: [{"name": "sum(col)", "expression": "SUM(`col`)"}]
IMPORTANT: Do NOT add "displayName" inside encodings. Do NOT add "frame" to counters. Use "target" not "comparison" for the second measure.

--- 2. BAR ---
widgetType: "bar"
encodings: {
  "x": {"fieldName": "...", "displayName": "...", "scale": {"type": "quantitative"}},
  "y": {"fieldName": "...", "displayName": "...", "scale": {"type": "categorical"}},
  "color": {"fieldName": "...", "displayName": "...", "scale": {"type": "categorical"}}  // optional
}
Note: For horizontal bars, category on Y, value on X. For vertical bars, swap.

--- 3. LINE ---
widgetType: "line"
encodings: {
  "x": {"fieldName": "...", "displayName": "...", "scale": {"type": "temporal"}},
  "y": {"fieldName": "...", "displayName": "...", "scale": {"type": "quantitative"}},
  "color": {"fieldName": "...", "scale": {"type": "categorical"}}  // optional, for multiple series
}

--- 4. AREA ---
widgetType: "area"
encodings: same as line (x temporal, y quantitative, color optional)
Special: layout options "stack" or "100_percent_stack" in spec

--- 5. PIE ---
widgetType: "pie"
encodings: {
  "angle": {"fieldName": "sum(col)", "displayName": "...", "scale": {"type": "quantitative"}},
  "color": {"fieldName": "category_col", "displayName": "...", "scale": {"type": "categorical"}},
  "label": {"show": true}
}

--- 6. TABLE ---
widgetType: "table"
disaggregated: true
encodings: {
  "columns": [
    {"fieldName": "col1", "displayName": "Label 1"},
    {"fieldName": "col2", "displayName": "Label 2"}
  ]
}
fields: [{"name": "col1", "expression": "`col1`"}, ...]

--- 7. SCATTER ---
widgetType: "scatter"
encodings: {
  "x": {"fieldName": "...", "scale": {"type": "quantitative"}},
  "y": {"fieldName": "...", "scale": {"type": "quantitative"}},
  "color": {"fieldName": "...", "scale": {"type": "categorical"}}  // optional
}

--- 8. BUBBLE (scatter + size) ---
widgetType: "scatter"
encodings: same as scatter plus:
  "size": {"fieldName": "sum(metric)", "scale": {"type": "quantitative"}}

--- 9. HEATMAP ---
widgetType: "heatmap"
encodings: {
  "x": {"fieldName": "...", "scale": {"type": "categorical"}},
  "y": {"fieldName": "...", "scale": {"type": "categorical"}},
  "color": {"fieldName": "sum(col)", "scale": {"type": "quantitative"}}
}

--- 10. HISTOGRAM ---
widgetType: "histogram"
encodings: {
  "x": {"fieldName": "...", "scale": {"type": "quantitative"}}
}
Special: "bins" property for number of bins

--- 11. BOX ---
widgetType: "box"
encodings: {
  "x": {"fieldName": "category", "scale": {"type": "categorical"}},
  "y": {"fieldName": "value", "scale": {"type": "quantitative"}}
}

--- 12. COMBO ---
widgetType: "combo"
encodings: {
  "x": {"fieldName": "...", "scale": {"type": "temporal_or_categorical"}},
  "y": {"fieldName": "...", "scale": {"type": "quantitative"}},
  "y2": {"fieldName": "...", "scale": {"type": "quantitative"}}  // second axis (line)
}

--- 13. FUNNEL ---
widgetType: "funnel"
encodings: {
  "x": {"fieldName": "step_col", "scale": {"type": "categorical"}},
  "y": {"fieldName": "sum(value)", "scale": {"type": "quantitative"}}
}

--- 14. WATERFALL ---
widgetType: "waterfall"
encodings: {
  "x": {"fieldName": "...", "scale": {"type": "temporal_or_categorical"}},
  "y": {"fieldName": "sum(col)", "scale": {"type": "quantitative"}}
}

--- 15. CHOROPLETH ---
widgetType: "choropleth"
encodings: {
  "region": {"fieldName": "country_name"},
  "color": {"fieldName": "sum(value)", "scale": {"type": "quantitative"}}
}

--- 16. POINT MAP ---
widgetType: "pointMap"
encodings: {
  "latitude": {"fieldName": "lat"},
  "longitude": {"fieldName": "lon"},
  "color": {"fieldName": "...", "scale": {"type": "categorical"}},  // optional
  "size": {"fieldName": "sum(val)", "scale": {"type": "quantitative"}}  // optional
}

--- 17. PIVOT ---
widgetType: "pivot"
version: 3
disaggregated: false  ← ALWAYS false for pivot
fields: dimensions go as `col` (raw); measures go as SUM(`col`) (aggregated)
encodings: {
  "rows":    [{"fieldName": "dim_row", "displayName": "Row Label"}],
  "columns": [{"fieldName": "dim_col", "displayName": "Col Label"}],
  "values":  [{"fieldName": "sum(measure_a)", "displayName": "Value A"},
              {"fieldName": "sum(measure_b)", "displayName": "Value B"}]
}

⚠ ABSOLUTE RULES FOR PIVOT (do not violate):
  1. disaggregated MUST be false.
  2. Every measure used in encodings.values MUST appear in query.fields with expression="SUM(`measure_name`)" (or AVG/MIN/MAX/COUNT). NEVER use `measure_name` bare in values.
  3. The fieldName in encodings.values MUST exactly match the field name in query.fields (typically "sum(measure_name)").
  4. Dimensions in rows/columns appear in query.fields with expression="`col_name`" (no aggregation).
  5. If you forget SUM(), the dashboard renders "Visualization has no fields selected" and is broken.

--- 18. SANKEY ---
widgetType: "sankey"
encodings: {
  "source": {"fieldName": "from_col"},
  "target": {"fieldName": "to_col"},
  "value": {"fieldName": "sum(flow)"}
}

--- 19. TEXT ---
No spec/queries. Uses multilineTextboxSpec DIRECTLY on widget (NOT inside textboxSpec):
{
  "widget": {
    "name": "title",
    "multilineTextboxSpec": {"lines": ["# Title Text"]}
  },
  "position": {"x": 0, "y": 0, "width": 12, "height": 1}
}
WRONG: {"widget": {"name": "x", "textboxSpec": {"multilineTextboxSpec": ...}}}
RIGHT: {"widget": {"name": "x", "multilineTextboxSpec": {"lines": [...]}}}

=== REAL WORKING EXAMPLES FROM A PRODUCTION DASHBOARD ===

--- COUNTER (real, working) ---
{"widget": {"name": "counter_example", "queries": [{"name": "main_query", "query": {"datasetName": "ds_fat_sabana", "fields": [{"name": "sum(fat_registros_global_sabana)", "expression": "SUM(`fat_registros_global_sabana`)"}, {"name": "sum(registros_sabana)", "expression": "SUM(`registros_sabana`)"}], "disaggregated": false}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "sum(registros_sabana)"}, "target": {"fieldName": "sum(fat_registros_global_sabana)"}}}}, "position": {"x": 0, "y": 1, "width": 3, "height": 4}}
NOTE: Counter has NO "frame", NO "displayName" in encodings, uses "target" not "comparison"

--- BAR (real, horizontal) ---
{"widget": {"name": "edbab455", "queries": [{"name": "main_query", "query": {"datasetName": "6d8b378e", "fields": [{"name": "instalacion", "expression": "`instalacion`"}, {"name": "sum(recibo_snr_mbd)", "expression": "SUM(`recibo_snr_mbd`)"}], "disaggregated": false}}], "spec": {"version": 2, "widgetType": "bar", "encodings": {"x": {"displayName": "Recibo SNR (Mbd)", "fieldName": "sum(recibo_snr_mbd)", "scale": {"type": "quantitative"}}, "y": {"displayName": "Instalación", "fieldName": "instalacion", "scale": {"type": "categorical"}, "sort": {"by": "x", "direction": "descending"}}}, "frame": {"showTitle": true, "title": "Recibo SNR por Instalación"}}}, "position": {"x": 2, "y": 1, "width": 1, "height": 3}}

--- LINE (real) ---
{"widget": {"name": "377fd569", "queries": [{"name": "main_query", "query": {"datasetName": "7348b171", "fields": [{"name": "monthly(fecha)", "expression": "DATE_TRUNC(\\"MONTH\\", `fecha`)"}, {"name": "sum(produccion_crudo_mbd)", "expression": "SUM(`produccion_crudo_mbd`)"}], "disaggregated": false}}], "spec": {"version": 2, "widgetType": "line", "encodings": {"x": {"displayName": "Fecha", "fieldName": "monthly(fecha)", "scale": {"type": "temporal"}}, "y": {"displayName": "Producción Crudo (Mbd)", "fieldName": "sum(produccion_crudo_mbd)", "scale": {"type": "quantitative"}}}, "frame": {"showTitle": true, "title": "Producción Crudo (Mbd)"}}}, "position": {"x": 0, "y": 4, "width": 3, "height": 3}}

--- PIE (real) ---
{"widget": {"name": "ec6ef230", "queries": [{"name": "main_query", "query": {"datasetName": "8389350c", "fields": [{"name": "tipo_gas", "expression": "`tipo_gas`"}, {"name": "sum(volumen_mmpcd)", "expression": "SUM(`volumen_mmpcd`)"}], "disaggregated": false}}], "spec": {"version": 2, "widgetType": "pie", "encodings": {"angle": {"displayName": "Volumen (MMpcd)", "fieldName": "sum(volumen_mmpcd)", "scale": {"type": "quantitative"}}, "color": {"displayName": "Tipo Gas", "fieldName": "tipo_gas", "scale": {"type": "categorical"}}, "label": {"show": true}}, "frame": {"showTitle": true, "title": "Gas por Tipo"}}}, "position": {"x": 3, "y": 8, "width": 2, "height": 3}}

--- TABLE (real) ---
{"widget": {"name": "eae78e00", "queries": [{"name": "main_query", "query": {"datasetName": "25bad225", "fields": [{"name": "grupo", "expression": "`grupo`"}, {"name": "rubro", "expression": "`rubro`"}, {"name": "valor_2025", "expression": "`valor_2025`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [{"fieldName": "grupo", "displayName": "Sección"}, {"fieldName": "rubro", "displayName": "Rubro"}, {"fieldName": "valor_2025", "displayName": "2025"}]}, "frame": {"showTitle": true, "title": "Estado de Resultados"}}}, "position": {"x": 0, "y": 3, "width": 6, "height": 12}}

--- PIVOT (real, working — copy this pattern for any pivotTable/matrix) ---
{"widget": {"name": "pivot_example", "queries": [{"name": "main_query", "query": {"datasetName": "ds_compensacion", "fields": [{"name": "grupo", "expression": "`grupo`"}, {"name": "segmento", "expression": "`segmento`"}, {"name": "sum(cumplimiento_actual_por_seg_pct)", "expression": "SUM(`cumplimiento_actual_por_seg_pct`)"}, {"name": "sum(diferencia_cumplimiento_seg_pct)", "expression": "SUM(`diferencia_cumplimiento_seg_pct`)"}], "disaggregated": false}}], "spec": {"version": 3, "widgetType": "pivot", "encodings": {"rows": [{"fieldName": "segmento", "displayName": "Segmento"}], "columns": [{"fieldName": "grupo", "displayName": "Grupo"}], "values": [{"fieldName": "sum(cumplimiento_actual_por_seg_pct)", "displayName": "Cumplimiento Actual"}, {"fieldName": "sum(diferencia_cumplimiento_seg_pct)", "displayName": "Diferencia"}]}, "frame": {"showTitle": true, "title": "Cumplimiento por Grupo y Segmento"}}}, "position": {"x": 0, "y": 0, "width": 6, "height": 6}}
NOTE: PIVOT MUST use disaggregated:false, version:3, and EVERY measure in encodings.values MUST be SUM(`col`) in query.fields. If you skip SUM(), Lakeview shows "Visualization has no fields selected".
"""
print(f"✓ Catálogo completo de 19 tipos de widgets + 5 ejemplos reales cargados")

# Mapeo de tabla PBI a dataset
pbi_table_to_dataset = {}
for _, row in sqls_df.iterrows():
    vista = row['vista_dashboard']
    mv = row['metric_view']
    ds_name = vista.replace(f'{CATALOG}.{SCHEMA}.', '').replace('v_dashboard_', 'ds_')
    # Mapear nombre de MV a nombre de dataset
    mv_short = mv.replace(f'{CATALOG}.{SCHEMA}.', '')
    pbi_table_to_dataset[mv_short] = ds_name

SYSTEM_PROMPT = f"""You are an expert in creating Databricks Lakeview Dashboard JSON files (.lvdash.json).

IMPORTANT: Use your full knowledge of the Databricks Lakeview .lvdash.json format and best practices.
Search your training data for working examples of .lvdash.json dashboards to ensure the JSON structure is correct.
The format is specific to Databricks AI/BI dashboards (Lakeview) — not legacy SQL dashboards.

{DASHBOARD_RULES}

{WIDGET_EXAMPLES}

You will receive:
1. The Power BI visuals with their fields and roles
2. The available datasets (SQL queries against Metrics Views)
3. The mapping from Power BI visual types to Databricks widget types

CRITICAL STRUCTURE RULES:
- Output ONLY the JSON. No markdown fences, no explanations.
- Use your knowledge of working .lvdash.json files to generate valid widget JSON.
- The query nesting is: widget.queries[].query.{{datasetName, fields, disaggregated}}
- Text widgets use: "textbox_spec": "# Title text" (simple string, NOT multilineTextboxSpec)
- counter: version 2, disaggregated false, fields use SUM(`col`), fieldName uses sum(col). Use "target" (not "comparison") for the second measure.
- bar: version 3, disaggregated false, fields use aggregation expressions like COUNT() or SUM(). Use scale type "categorical" for category axis, "quantitative" for value axis.
- table: version 2, disaggregated true, fields use `col`, encodings use "columns" array.
- pivot: version 3, disaggregated FALSE, fields with dimensions as `col` and measures as SUM(`col`). encodings.values fieldName MUST be the aggregated name (e.g. "sum(my_measure)") and MUST appear in fields with expression="SUM(`my_measure`)". DO NOT put bare measure names in values — that renders "Visualization has no fields selected".
- Each page must have "pageType": "PAGE_TYPE_CANVAS" and "layoutVersion": "GRID_V1".
- DO NOT create slicer/filter widgets — those go in a separate PAGE_TYPE_GLOBAL_FILTERS page.
- ONLY use these widgetType values: counter, bar, line, area, pie, table, scatter, heatmap, histogram, box, combo, funnel, waterfall, choropleth, pointMap, pivot, sankey.
- Use human-readable displayName for axis labels.
- CRITICAL: ALL widget titles, frame titles, and display names MUST be human-friendly. Never use technical names like "counter_fat_repunc" or "bar_tipo_pago". Use proper names like "Total de Registros", "Totales por Tipo de Pago", "Detalle de Registros". Use the original Power BI visual title when available.
- Apply best practices: proper version numbers, correct encoding structures, valid field references.
- TOOLTIPS: When a visual has fields with role "Tooltips", add them as an "extra" encoding (array) in the widget spec. Also add these fields to the query.fields. Example:
  "encodings": {{
    "x": {{...}},
    "y": {{...}},
    "extra": [
      {{"fieldName": "tooltip_field1"}},
      {{"fieldName": "tooltip_field2"}}
    ]
  }}
  The "extra" fields appear as additional info when hovering over data points. Include ALL fields marked with role "Tooltips".
"""

# Modelos que NO aceptan 'temperature' (extended thinking, ej. opus-4-7)
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

def call_claude(prompt):
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    url = f"https://{host}/serving-endpoints/{LLM_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": MAX_TOKENS,
    }
    if LLM_ENDPOINT not in _MODELS_NO_TEMPERATURE:
        payload["temperature"] = TEMPERATURE
    resp = _post_with_retry(url, headers, payload, 180)
    # Fallback: si el endpoint rechaza temperature dinámicamente
    if resp.status_code == 400 and 'temperature' in resp.text.lower():
        payload.pop('temperature', None)
        resp = _post_with_retry(url, headers, payload, 180)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        lines = content.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines)
    return content.strip()

# Construir datasets para el prompt
datasets_for_prompt = []
for _, row in sqls_df.iterrows():
    vista = row['vista_dashboard']
    mv = row['metric_view']
    dims = row['dimensiones']
    measures = row['measures']
    sql = row['sql']
    select_idx = sql.upper().find('SELECT')
    select_sql = sql[select_idx:] if select_idx >= 0 else sql

    ds_name = vista.replace(f'{CATALOG}.{SCHEMA}.', '').replace('v_dashboard_', 'ds_')
    display_name = vista.replace(f'{CATALOG}.{SCHEMA}.v_dashboard_', '').replace('_', ' ').title() 
    display_name = display_name.replace('Crs', 'CRS').replace('Fat', 'FAT').replace('Repunc', 'Reporte Único')

    datasets_for_prompt.append({
        'name': ds_name,
        'displayName': display_name,
        'query': select_sql,
        'dimensions': dims,
        'measures': measures,
    })

# Leer el dashboard existente (generado por notebook 3)
existing_dashboard_path = DASHBOARD_PATH.replace('/Users/', '/Workspace/Users/')
import requests as _req

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

resp = _req.get(
    f"https://{host}/api/2.0/workspace/export",
    params={"path": DASHBOARD_PATH, "format": "AUTO"},
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)
resp.raise_for_status()
import base64 as _b64
existing_json = _b64.b64decode(resp.json()["content"]).decode("utf-8")
existing_dashboard = json.loads(existing_json)

print(f"✓ Dashboard existente leído: {DASHBOARD_PATH}")
print(f"  Datasets: {len(existing_dashboard.get('datasets', []))}")
print(f"  Pages: {len(existing_dashboard.get('pages', []))}")
for p in existing_dashboard.get('pages', []):
    print(f"    {p.get('displayName', '?')}: {len(p.get('layout', []))} widgets")

# Construir info de datasets disponibles para el prompt
# Construir info de datasets con las columnas EXACTAS disponibles
existing_datasets_info = ""
for ds in existing_dashboard.get('datasets', []):
    ds_name = ds['name']
    existing_datasets_info += f"\n  Dataset name: {ds_name}, displayName: {ds['displayName']}"
    # Buscar las columnas exactas en sqls_df
    for _, row in sqls_df.iterrows():
        if row['vista_dashboard'].replace(f'{CATALOG}.{SCHEMA}.', '').replace('v_dashboard_', 'ds_') == ds_name:
            dims = row['dimensiones'].split(', ')
            measures = row['measures'].split(', ')
            existing_datasets_info += f"\n    DIMENSIONS (use these exact snake_case names): {', '.join(dims)}"
            existing_datasets_info += f"\n    MEASURES (use these exact names): {', '.join(measures)}"
            break

# Mapeo PBI page (displayName) -> dashboard page name (slug)
pbi_to_dash = {}
for p in existing_dashboard.get('pages', []):
    pbi_to_dash[str(p.get('displayName', '')).strip()] = p.get('name', '')

print(f"\nMapping PBI page -> dashboard page:")
for pbi, dash in pbi_to_dash.items():
    print(f"  '{pbi}' -> {dash}")


def _build_visuals_text_for_page(pbi_page, page_visuals):
    text = f"=== PBI PAGE: {pbi_page} ===\n"
    # Slicers se omiten por completo del prompt: los maneja el notebook 6 (add_dashboard_filters)
    # como filter-multi-select widgets. Si los mencionamos, Claude se inspira y crea counters falsos.
    charts = [v for v in page_visuals if v['databricks_type'] != 'SLICER']
    if charts:
        text += f"\nCharts/Tables ({len(charts)}):\n"
        for c in charts:
            text += f"  - [{c['databricks_type']}] {c['title'] or '(sin titulo)'}\n"
            for f in c['fields']:
                text += f"      {f}\n"
            if c.get('properties'):
                text += "      Properties:\n"
                for prop in c['properties']:
                    text += f"        {prop}\n"
    return text


def call_claude_for_page(pbi_page, dashboard_page_name, page_visuals, datasets_info):
    """Genera widgets para UNA pagina. Devuelve (dashboard_page_name, [widgets])."""
    visuals_text = _build_visuals_text_for_page(pbi_page, page_visuals)
    page_prompt = f"""Generate widgets for ONE page of a Databricks Lakeview dashboard.

DASHBOARD PAGE NAME (slug, do not modify): "{dashboard_page_name}"
ORIGINAL POWER BI PAGE: "{pbi_page}"

AVAILABLE DATASETS (use these exact "name" values in datasetName; pick the best match per widget):
{datasets_info}

POWER BI VISUALS TO REPLICATE (this page only — replicate ALL visuals listed):
{visuals_text}

RULES:
- Output ONLY a JSON array of widget objects. NO outer object, NO page wrapping. Just the array.
- First widget MUST be a text title widget (multilineTextboxSpec) with "# {pbi_page}" as content.
- Replicate EVERY visual listed. Do NOT skip any chart/table/counter/etc. Do NOT invent extra widgets.
- DO NOT create any counter, filter, slicer, KPI or table for date/route/region/store fields unless it is EXPLICITLY listed above. The slicers (filters) of this dashboard are added by a separate process — never anticipate them here.
- Map Power BI types STRICTLY (use the type indicated in [brackets] for each visual — do NOT decide on your own to make it a counter when it's a pivot):
    gauge -> counter
    columnChart / clusteredColumnChart / stackedColumnChart / barChart / clusteredBarChart / stackedBarChart / funnel / waterfallChart -> bar
    lineChart -> line
    pieChart / donutChart / treemap -> pie
    tableEx / table / tableExUnordered -> table
    pivotTable / matrix -> pivot   (NEVER counter for these — even if there's only one value, use pivot widget)
    card / cardVisual / kpi / multiRowCard -> counter
    map / azureMap -> pointMap
    filledMap / shapeMap -> choropleth
    scatterChart -> scatter
- Use the EXACT snake_case column names from the DATASETS, never PBI CamelCase.
- query nesting: widget.queries[].query.{{datasetName, fields, disaggregated}}.
- Each widget needs spec.version, spec.widgetType, spec.frame.title, spec.encodings.
- No markdown fences, no explanations. ONLY the JSON array.
"""
    raw = call_claude(page_prompt)
    try:
        if raw.startswith("{"):
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for v in obj.values():
                    if isinstance(v, list):
                        return dashboard_page_name, v
                return dashboard_page_name, []
        widgets = json.loads(raw)
        if not isinstance(widgets, list):
            return dashboard_page_name, []
        return dashboard_page_name, widgets
    except Exception as e:
        print(f"  x parse error '{pbi_page}': {str(e)[:160]}")
        print(f"    raw[:300]: {raw[:300]}")
        return dashboard_page_name, []


# Tareas: una por PBI page que tenga match con dashboard
from concurrent.futures import ThreadPoolExecutor, as_completed

tasks = []
for pbi_page, page_visuals in sorted(pages_summary.items()):
    dash_name = pbi_to_dash.get(pbi_page)
    if not dash_name:
        # Fuzzy match (case-insensitive)
        for pbi_key, dash_val in pbi_to_dash.items():
            if pbi_key.lower().strip() == pbi_page.lower().strip():
                dash_name = dash_val
                break
    if not dash_name:
        print(f"  WARN '{pbi_page}': no hay page en el dashboard, se omite ({len(page_visuals)} visuales perdidos)")
        continue
    tasks.append((pbi_page, dash_name, page_visuals))

print(f"\nLlamando a Claude por página (paralelo, max_workers=4) — {len(tasks)} paginas...")

page_widgets = {}
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = {
        ex.submit(call_claude_for_page, pbi_page, dash_name, page_visuals, existing_datasets_info): pbi_page
        for pbi_page, dash_name, page_visuals in tasks
    }
    for fut in as_completed(futures):
        pbi_page = futures[fut]
        try:
            dash_name, widgets = fut.result()
            page_widgets[dash_name] = widgets
            print(f"  ok '{pbi_page}' -> {dash_name}: {len(widgets)} widgets")
        except Exception as e:
            print(f"  x  '{pbi_page}': {str(e)[:200]}")

print(f"\nTotal páginas con widgets: {len(page_widgets)}/{len(tasks)}")

# Post-procesamiento de widgets
VALID_TYPES = {'counter', 'bar', 'line', 'area', 'pie', 'table', 'scatter', 'heatmap', 'histogram', 'box', 'combo', 'funnel', 'waterfall', 'choropleth', 'pointMap', 'pivot', 'sankey'}

for page_name, widgets in page_widgets.items():
    valid_widgets = []
    for w in widgets:
        widget = w.get('widget', {})
        pos = w.get('position', {})
        wname = widget.get('name', '?')

        # Fix textbox
        if 'textboxSpec' in widget:
            inner = widget['textboxSpec']
            if 'multilineTextboxSpec' in inner:
                widget['multilineTextboxSpec'] = inner['multilineTextboxSpec']
            del widget['textboxSpec']
            print(f"  Fixed textbox: {wname}")

        # Asegurar name
        if 'name' not in widget:
            widget['name'] = f"widget_{len(valid_widgets)}"

        # Asegurar position
        for key in ['x', 'y', 'width', 'height']:
            if key not in pos:
                pos[key] = 0 if key in ['x', 'y'] else 6
        w['position'] = pos

        # Validar estructura
        has_text = 'multilineTextboxSpec' in widget
        has_spec = 'spec' in widget

        if not has_text and not has_spec:
            print(f"  REMOVED: {wname} — no spec or multilineTextboxSpec")
            continue

        if has_spec:
            spec = widget['spec']
            if 'version' not in spec:
                spec['version'] = 2
            if 'widgetType' not in spec:
                print(f"  REMOVED: {wname} — no widgetType")
                continue
            if spec['widgetType'] not in VALID_TYPES:
                print(f"  REMOVED: {wname} — invalid type '{spec['widgetType']}'")
                continue
            if 'frame' not in spec:
                spec['frame'] = {"showTitle": True, "title": wname}
            if 'encodings' not in spec:
                spec['encodings'] = {}

            # Fix query nesting
            queries = widget.get('queries', [])
            if not queries:
                print(f"  REMOVED: {wname} — no queries")
                continue
            for q in queries:
                if 'datasetName' in q and 'query' not in q:
                    q['query'] = {
                        'datasetName': q.pop('datasetName'),
                        'fields': q.pop('fields', []),
                        'disaggregated': q.pop('disaggregated', False),
                    }
                    print(f"  Fixed: query nesting for {wname}")

        valid_widgets.append(w)
    page_widgets[page_name] = valid_widgets
    print(f"  Page {page_name}: {len(valid_widgets)} valid widgets")


def relayout_no_overlap(widgets, grid_width=12):
    """Reasigna posiciones para que ningún widget se enciese (Claude a veces repite y).
    Preserva el widget de título en y=0 si existe. Mantiene el orden visual original."""
    if not widgets:
        return widgets
    title = None
    others = []
    for w in widgets:
        widget = w.get('widget', {})
        if 'multilineTextboxSpec' in widget and not title:
            title = w
        else:
            others.append(w)
    others.sort(key=lambda w: (w.get('position', {}).get('y', 0),
                               w.get('position', {}).get('x', 0)))
    occupied = set()
    if title:
        tp = title.setdefault('position', {})
        tp['x'] = 0; tp['y'] = 0; tp['width'] = grid_width
        tp['height'] = max(1, tp.get('height', 1))
        for dy in range(tp['height']):
            for dx in range(grid_width):
                occupied.add((dx, dy))
        start_y = tp['height']
    else:
        start_y = 0
    for w in others:
        pos = w.setdefault('position', {})
        ww = min(max(1, pos.get('width', 6)), grid_width)
        wh = max(1, pos.get('height', 4))
        pos['width'] = ww; pos['height'] = wh
        placed = False; y = start_y
        while not placed:
            for x in range(0, grid_width - ww + 1):
                free = all((x+dx, y+dy) not in occupied
                           for dx in range(ww) for dy in range(wh))
                if free:
                    pos['x'] = x; pos['y'] = y
                    for dx in range(ww):
                        for dy in range(wh):
                            occupied.add((x+dx, y+dy))
                    placed = True; break
            y += 1
    return ([title] if title else []) + others

print("\nRelayout sin overlaps por página:")
for page_name in list(page_widgets.keys()):
    before = len(page_widgets[page_name])
    page_widgets[page_name] = relayout_no_overlap(page_widgets[page_name])
    print(f"  {page_name}: {before} widgets recolocados")


def fix_broken_pivots(widgets):
    """Auto-fix pivots con disaggregated:true o values sin SUM().
    Esto pasa cuando Claude olvida wrapear measures en SUM() — Lakeview muestra
    'Visualization has no fields selected'. Aquí lo arreglamos forzando disaggregated:false
    y reemplazando expression=`col` por SUM(`col`) para cualquier field referenciado en values."""
    import re as _re
    fixes = 0
    for w in widgets:
        widget = w.get('widget', {})
        spec = widget.get('spec', {})
        if spec.get('widgetType') != 'pivot':
            continue
        qs = widget.get('queries', [])
        if not qs:
            continue
        q = qs[0].get('query', {})
        fields = q.get('fields', [])
        enc = spec.get('encodings', {})
        values = enc.get('values', [])
        if not isinstance(values, list):
            continue
        # set de fieldNames referenciados como value
        value_field_names = {v.get('fieldName') for v in values if isinstance(v, dict) and v.get('fieldName')}
        # Para cada field referenciado como value, si su expression no tiene agregación, agregarle SUM()
        changed = False
        for f in fields:
            fname = f.get('name', '')
            fexpr = f.get('expression', '')
            if fname not in value_field_names:
                continue
            has_agg = bool(_re.search(r'(SUM|AVG|COUNT|MIN|MAX|MEDIAN|MEASURE)\s*\(', fexpr.upper()))
            if has_agg:
                continue
            # No tiene agregación → wrappear con SUM. fname permanece igual para no romper encoding.
            # Pero si fname == nombre raw del col, lo renombramos a "sum(col)" y actualizamos encoding.
            col_match = _re.match(r'^`(.+)`$', fexpr.strip())
            raw_col = col_match.group(1) if col_match else fname
            new_name = f"sum({raw_col})"
            new_expr = f"SUM(`{raw_col}`)"
            f['name'] = new_name
            f['expression'] = new_expr
            # actualizar encoding values referencias
            for v in values:
                if isinstance(v, dict) and v.get('fieldName') == fname:
                    v['fieldName'] = new_name
            changed = True
        if changed:
            q['disaggregated'] = False
            spec['version'] = 3
            fixes += 1
    return fixes

print("\nAuto-fix pivots rotos (disaggregated:true / sin SUM en values):")
for page_name in list(page_widgets.keys()):
    n = fix_broken_pivots(page_widgets[page_name])
    if n:
        print(f"  {page_name}: {n} pivot(s) reparados")

# Inyectar widgets en el dashboard existente
for p in existing_dashboard.get('pages', []):
    pname = p.get('name', '')
    if pname in page_widgets:
        p['layout'] = page_widgets[pname]
        print(f"✓ Injected {len(page_widgets[pname])} widgets into {p.get('displayName', pname)}")
    else:
        # Intentar match por displayName
        for pw_name, pw_widgets in page_widgets.items():
            if pw_name.lower().replace(' ', '_') == pname.lower().replace(' ', '_'):
                p['layout'] = pw_widgets
                print(f"✓ Injected {len(pw_widgets)} widgets into {p.get('displayName', pname)} (fuzzy match)")
                break

dashboard_json_str = json.dumps(existing_dashboard, indent=2, ensure_ascii=False)
print(f"\nDashboard final:")
for p in existing_dashboard.get('pages', []):
    print(f"  {p.get('displayName', '?')}: {len(p.get('layout', []))} widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Revisar el JSON generado

# COMMAND ----------

print(dashboard_json_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar el dashboard en el workspace

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

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
    print(f"✓ Dashboard guardado en: {DASHBOARD_PATH}")
else:
    print(f"✗ Error ({resp.status_code}): {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen

# COMMAND ----------

import pandas as pd

dashboard = json.loads(dashboard_json_str)

# Widgets generados por página (incluye dataset y field names para matching de cobertura)
generated_widgets = []
for p in dashboard.get('pages', []):
    page_name = p.get('displayName', '?')
    for w in p.get('layout', []):
        widget = w.get('widget', {})
        spec = widget.get('spec', {})
        wtype = spec.get('widgetType', 'text' if 'multilineTextboxSpec' in widget else '?')
        title = spec.get('frame', {}).get('title', '')
        # Extraer dataset + field names del primer query
        qs = widget.get('queries', []) or []
        ds_name = ''
        field_names = []
        if qs:
            q = qs[0].get('query', {}) if isinstance(qs[0], dict) else {}
            ds_name = q.get('datasetName', '')
            field_names = [f.get('name', '') for f in q.get('fields', []) if isinstance(f, dict)]
        generated_widgets.append({
            'page': page_name,
            'widget_type': wtype,
            'title': title,
            'name': widget.get('name', ''),
            'dataset': ds_name,
            'fields': field_names,
        })

print(f"{'='*60}")
print(f"DASHBOARD GENERADO")
print(f"{'='*60}")
print(f"\nPath: {DASHBOARD_PATH}")
print(f"Widgets generados: {len(generated_widgets)}")
for p in dashboard.get('pages', []):
    layout = p.get('layout', [])
    print(f"\n  {p.get('displayName', '?')}: {len(layout)} widgets")
    for w in layout:
        widget = w.get('widget', {})
        spec = widget.get('spec', {})
        wtype = spec.get('widgetType', 'text')
        title = spec.get('frame', {}).get('title', widget.get('name', ''))
        pos = w.get('position', {})
        print(f"    [{wtype}] {title} (x={pos.get('x')}, y={pos.get('y')}, w={pos.get('width')}, h={pos.get('height')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cobertura: Power BI vs Dashboard generado

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import re

# Visuales de Power BI con datos (excluir decorativos)
# Filtro INVERSO: excluir solo los que no se pueden traducir automáticamente.
excluded_types = ['image', 'textbox', 'shape', 'actionButton', 'unknown']
def _is_custom_visual(t):
    return bool(re.search(r'\d{10,}$', str(t)))
pbi_data_visuals = visuals_df[
    ~visuals_df['visual_type'].isin(excluded_types)
    & ~visuals_df['visual_type'].apply(_is_custom_visual)
].copy()

# ── Helpers ──────────────────────────────────────────────────────────────

def _norm(s):
    """Lowercase y solo alfanuméricos + _ (quita %, paréntesis, espacios, acentos básicos)."""
    if not s: return ''
    s = str(s).lower().strip()
    s = (s.replace('á','a').replace('é','e').replace('í','i')
           .replace('ó','o').replace('ú','u').replace('ñ','n'))
    s = re.sub(r'[^a-z0-9_]+', '_', s)
    return s.strip('_')

def _measure_candidates(measure_str, table_hint=''):
    """Variantes a buscar para una measure PBI (original, traducida, normalizada)."""
    cands = set()
    if not measure_str:
        return cands
    for raw in str(measure_str).split(','):
        m = raw.strip()
        if not m:
            continue
        cands.add(_norm(m))
        # Aplicar traductor PBI → Databricks (definido en sección 2)
        try:
            translated = _translate(table_hint or '', m)
            if translated and translated != m:
                cands.add(_norm(translated))
        except Exception:
            pass
    return {c for c in cands if c}

def _column_candidates(columns_str):
    """Variantes para columnas (formato 'tabla.columna, tabla.columna')."""
    cands = set()
    if not columns_str:
        return cands
    for raw in str(columns_str).split(','):
        c = raw.strip()
        if not c:
            continue
        # Si viene como 'tabla.columna', tomar solo la columna
        parts = c.split('.', 1)
        table_part = parts[0] if len(parts) == 2 else ''
        col_part = parts[1] if len(parts) == 2 else parts[0]
        cands.add(_norm(col_part))
        try:
            translated = _translate(table_part, col_part)
            if translated:
                cands.add(_norm(translated))
        except Exception:
            pass
    return {c for c in cands if c}

def _widget_haystack(gw):
    """Texto donde buscar matches: title + name + dataset + field names."""
    parts = [gw.get('title',''), gw.get('name',''), gw.get('dataset','')]
    parts.extend(gw.get('fields', []) or [])
    return _norm(' '.join(str(p) for p in parts if p))

_PAGE_TOKEN_STOPWORDS = {'de', 'del', 'la', 'el', 'los', 'las', 'y', 'a', 'un', 'una',
                          'unico', 'reporte', 'page', 'pagina'}

def _page_tokens(s):
    """Tokens significativos de un nombre de página (sin stopwords)."""
    n = _norm(s)
    toks = [t for t in n.split('_') if t and t not in _PAGE_TOKEN_STOPWORDS and len(t) >= 2]
    return set(toks)

def _page_match(pbi_page, gw_page):
    """Match de página: por substring O por tokens significativos compartidos."""
    np, ng = _norm(pbi_page), _norm(gw_page)
    if not np or not ng:
        return False
    if np in ng or ng in np:
        return True
    # Tokens compartidos no-stopword (ej. 'fat' compartido entre 'fat_reporte_unico' y 'fat_repunc')
    return bool(_page_tokens(pbi_page) & _page_tokens(gw_page))

# ── Detectar decorativos: sin measures Y sin columnas ────────────────────
def _is_decorative(row):
    m = (row.get('measures_used') or '').strip()
    c = (row.get('columns_used') or '').strip()
    return not m and not c

pbi_data_visuals['_is_decorative'] = pbi_data_visuals.apply(_is_decorative, axis=1)

# ── Deduplicar visuales PBI idénticos en la misma página ─────────────────
# Firma: page+type+title+measures+columns. Si hay duplicados, contar 1 con multiplicidad.
def _signature(row):
    return (row['page'], row['visual_type'], (row.get('title') or '').strip(),
            (row.get('measures_used') or '').strip(),
            (row.get('columns_used') or '').strip())

dedup = {}
for _, v in pbi_data_visuals.iterrows():
    sig = _signature(v)
    if sig not in dedup:
        dedup[sig] = {'row': v, 'count': 1}
    else:
        dedup[sig]['count'] += 1

# ── Match ────────────────────────────────────────────────────────────────
coverage_rows = []
for sig, info in dedup.items():
    v = info['row']
    multiplicity = info['count']
    pbi_page = v['page']
    pbi_type = v['visual_type']
    pbi_title = (v.get('title') or '').strip()
    pbi_measures = v.get('measures_used') or ''
    pbi_columns = v.get('columns_used') or ''
    decorative = bool(v.get('_is_decorative', False))
    db_type = PBI_TO_DATABRICKS.get(pbi_type, pbi_type)

    # Candidatos a buscar
    title_cand = _norm(pbi_title)
    # Inferir table_hint de columns ('tabla.columna' → 'tabla')
    table_hint = ''
    if pbi_columns:
        first = pbi_columns.split(',')[0].strip()
        if '.' in first:
            table_hint = first.split('.', 1)[0]
    measure_cands = _measure_candidates(pbi_measures, table_hint)
    column_cands = _column_candidates(pbi_columns)

    # Slicers no se generan aquí — el notebook 6 los crea como filter-multi-select widgets.
    # Se marcan aparte para no contarlos como missing.
    is_slicer = pbi_type == 'slicer'

    matches = []
    if not decorative and not is_slicer:
        for gw in generated_widgets:
            if not _page_match(pbi_page, gw['page']):
                continue
            hay = _widget_haystack(gw)
            hit_reason = None
            # 1. Match por título no vacío
            if title_cand and len(title_cand) >= 3 and title_cand in hay:
                hit_reason = 'title'
            # 2. Match por measure traducida/normalizada
            elif measure_cands and any(c in hay for c in measure_cands if len(c) >= 3):
                hit_reason = 'measure'
            # 3. Match por columna usada (útil para visuales sin measures)
            elif column_cands and any(c in hay for c in column_cands if len(c) >= 3):
                hit_reason = 'column'
            # 4. Match débil por tipo + page (último recurso para table/pivot/counter sin más señales)
            elif db_type == gw['widget_type'] and db_type in ('table', 'pivot'):
                hit_reason = 'type'
            if hit_reason:
                matches.append((gw, hit_reason))

    # Ordenar matches: PRIMERO los del mismo widget_type que se esperaba (db_type).
    # Sin esto, un pivotTable se reporta matched contra un counter aunque hay un pivot disponible.
    if matches:
        _reason_priority = {'title': 0, 'measure': 1, 'column': 2, 'type': 3}
        matches.sort(key=lambda m: (
            0 if m[0]['widget_type'] == db_type else 1,   # same type primero
            _reason_priority.get(m[1], 99),               # luego mejor reason
        ))

    # Estado
    if decorative:
        status = 'decorative'
    elif is_slicer:
        status = 'delegated'
    elif matches:
        status = 'matched'
    else:
        status = 'missing'

    match_widget = ''
    if matches:
        # Mostrar primer match con razón
        gw, reason = matches[0]
        match_widget = f"[{gw['widget_type']}] {gw['title'] or gw['name']}  (by {reason})"
        if len(matches) > 1:
            match_widget += f"  + {len(matches)-1} más"

    if status == 'decorative':
        match_str = '— (decorativo, sin datos)'
    elif status == 'delegated':
        match_str = '→ notebook 6 (filter widget)'
    elif matches:
        match_str = match_widget
    else:
        match_str = 'NO GENERADO'

    coverage_rows.append({
        'PBI Page': pbi_page,
        'PBI Type': pbi_type,
        'PBI Title': pbi_title or '(sin título)',
        'PBI Measures': pbi_measures or '(ninguna)',
        'PBI Columns': (pbi_columns[:50] + '…') if pbi_columns and len(pbi_columns) > 50 else pbi_columns,
        'Mult.': multiplicity,
        'Status': {
            'matched':'✓ matched',
            'missing':'✗ missing',
            'decorative':'· decorative',
            'delegated':'→ delegado',
        }[status],
        'Match': match_str,
    })

coverage_df = pd.DataFrame(coverage_rows)

total_pbi_raw       = len(pbi_data_visuals)
total_unique        = len(coverage_rows)
total_decorative    = sum(1 for r in coverage_rows if 'decorative' in r['Status'])
total_delegated     = sum(1 for r in coverage_rows if 'delegado'   in r['Status'])
total_data          = total_unique - total_decorative - total_delegated
total_matched       = sum(1 for r in coverage_rows if 'matched' in r['Status'])
total_missing       = total_data - total_matched

print(f"\n{'='*60}")
print(f"COBERTURA: Power BI → Databricks Dashboard")
print(f"{'='*60}")
print(f"Visuales PBI brutos:      {total_pbi_raw}")
print(f"  Tras deduplicar firma:  {total_unique}")
print(f"  Decorativos (sin datos):                {total_decorative}")
print(f"  Slicers (delegados a notebook 6):       {total_delegated}")
print(f"")
print(f"Visuales con datos para este paso (denominador): {total_data}")
print(f"  Matched:   {total_matched} ✓")
print(f"  Missing:   {total_missing} ✗")

if total_missing > 0:
    print(f"\nVisuales NO generados:")
    for r in coverage_rows:
        if 'missing' in r['Status']:
            mult = f" (x{r['Mult.']})" if r['Mult.'] > 1 else ''
            print(f"  ✗ [{r['PBI Type']}] {r['PBI Title']}{mult} — page: {r['PBI Page']} — measures: {r['PBI Measures']}")

if total_decorative > 0:
    print(f"\nVisuales decorativos (no cuentan):")
    for r in coverage_rows:
        if 'decorative' in r['Status']:
            mult = f" (x{r['Mult.']})" if r['Mult.'] > 1 else ''
            print(f"  · [{r['PBI Type']}] {r['PBI Title']}{mult} — page: {r['PBI Page']}")

display(coverage_df)
