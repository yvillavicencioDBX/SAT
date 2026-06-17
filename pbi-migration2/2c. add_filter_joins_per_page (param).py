# Databricks notebook source
# MAGIC %md
# MAGIC # 2b. Crear views por hoja con joins de filtros
# MAGIC
# MAGIC Standalone — opera sobre las `v_dashboard_<fact>` ya creadas por el paso 2.
# MAGIC Para cada hoja del dashboard:
# MAGIC 1. Detecta la fact principal (la tabla PBI más usada por las measures de los visuales)
# MAGIC 2. Identifica los filtros de la hoja desde `pbi_page_filters`
# MAGIC 3. Crea `v_dashboard_page_<hoja>` que selecciona desde la view base + JOIN a dims si la columna no estaba expuesta + renombre de columnas prefijadas a nombres simples
# MAGIC
# MAGIC Salida: tabla `dashboard_page_views` con `(page, view, base_view, filters_exposed, extra_joins, sql)`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

import re
import unicodedata
import pandas as pd

dbutils.widgets.text("catalog", "migracion_pbix", "Catálogo destino")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("schema", "default", "Schema destino")
dbutils.widgets.text("data_locations", "", "Ubicaciones de datos (lista catalog.schema separada por coma; vacío=usar destino)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG " + CATALOG)
spark.sql(f"USE SCHEMA " + SCHEMA)

RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
def _t(name):
    """Sufija nombres de tabla con run_id."""
    return f"{name}{SUFFIX}"
DATA_LOCATIONS_RAW = dbutils.widgets.get("data_locations").strip()

# Lista (catalog, schema) donde buscar tablas físicas (dim tables para joins).
DATA_LOCATIONS = []
if DATA_LOCATIONS_RAW:
    for loc in DATA_LOCATIONS_RAW.split(','):
        loc = loc.strip()
        if not loc or '.' not in loc:
            continue
        c, s = loc.split('.', 1)
        DATA_LOCATIONS.append((c.strip(), s.strip()))
if not DATA_LOCATIONS:
    DATA_LOCATIONS = [(CATALOG, SCHEMA)]

print(f"Catálogo destino: {CATALOG}.{SCHEMA}")
print("Data locations:")
for c, s in DATA_LOCATIONS:
    print(f"  - {c}.{s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers

# COMMAND ----------

def _snake(s):
    """'F Sales margin' -> 'f_sales_margin', 'Año' -> 'anio' (ñ -> ni)."""
    if s is None:
        return ''
    pre = str(s).replace('ñ', 'ni').replace('Ñ', 'NI')
    nfd = unicodedata.normalize('NFD', pre)
    a = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-zA-Z0-9_]+', '_', a).strip('_').lower() or 'col'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inventario: views base ya creadas + dim tables para joins

# COMMAND ----------

# Views base existentes en el schema destino
base_views = {}  # {fact_uc_name: full_qualified_view_name}
base_view_cols = {}  # {fact_uc_name: [columns]}

vrows = spark.sql(f"SHOW VIEWS IN {CATALOG}.{SCHEMA} LIKE 'v_dashboard_*'").collect()
for r in vrows:
    vname = r.viewName
    if vname.startswith('v_dashboard_page_'):
        continue  # vistas por hoja existentes (las vamos a recrear)
    fact_name = vname.replace('v_dashboard_', '')
    full = f"{CATALOG}.{SCHEMA}.{vname}"
    try:
        cols = spark.sql(f"DESCRIBE TABLE {full}").collect()
        col_names = [c.col_name for c in cols if c.col_name and not c.col_name.startswith('#')]
        base_views[fact_name] = full
        base_view_cols[fact_name] = col_names
    except Exception as e:
        print(f"  ⚠ {full}: {str(e)[:120]}")

print(f"\nViews base encontradas: {len(base_views)}")
for f, full in base_views.items():
    print(f"  {full}: {len(base_view_cols[f])} columnas")

# Dim tables disponibles para JOIN (en cualquier DATA_LOCATION)
dim_tables = {}     # {table_name: [columns]}
dim_to_fqn = {}     # {table_name: 'catalog.schema.table'}
for c_loc, s_loc in DATA_LOCATIONS:
    try:
        rows = spark.sql(f"SHOW TABLES IN {c_loc}.{s_loc}").collect()
    except Exception as e:
        print(f"  ⚠ No puedo listar {c_loc}.{s_loc}: {str(e)[:120]}")
        continue
    for r in rows:
        tname = r.tableName
        if tname.startswith(('pbi_', 'mv_', 'v_dashboard_', 'lod_')):
            continue
        if tname in dim_tables:
            continue  # primera location gana
        try:
            cols = spark.sql(f"DESCRIBE TABLE {c_loc}.{s_loc}.{tname}").collect()
            dim_tables[tname] = [c.col_name for c in cols if c.col_name and not c.col_name.startswith('#')]
            dim_to_fqn[tname] = f"{c_loc}.{s_loc}.{tname}"
        except Exception:
            pass

print(f"\nTablas físicas disponibles para JOIN: {len(dim_tables)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leer metadatos PBI: visuales y filtros de página

# COMMAND ----------

try:
    visual_fields_df = spark.sql(
        f"SELECT page, table, field_type, measure_name FROM {CATALOG}.{SCHEMA}.{_t('pbi_visual_fields')}"
    ).toPandas()
    print(f"pbi_visual_fields: {len(visual_fields_df)} filas")
except Exception as e:
    print(f"⚠ No se puede leer pbi_visual_fields: {e}")
    visual_fields_df = pd.DataFrame()

try:
    pbi_slicers = spark.sql(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.{_t('pbi_page_filters')}"
    ).toPandas()
    print(f"pbi_page_filters: {len(pbi_slicers)} filas")
except Exception as e:
    print(f"⚠ No se puede leer pbi_page_filters: {e}")
    pbi_slicers = pd.DataFrame()

if pbi_slicers.empty:
    print("No hay filtros de página — no se generan views por hoja.")
    dbutils.notebook.exit('{"page_views_built": 0}')

unique_pages = sorted(set(pbi_slicers['Pagina'].dropna().unique()))
print(f"\nPáginas con filtros: {len(unique_pages)}")
for p in unique_pages:
    print(f"  - {p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generar `v_dashboard_page_<hoja>` por cada hoja

# COMMAND ----------

page_views_built = []

for page in unique_pages:
    page_snake = _snake(page)
    page_view_name = f"v_dashboard_page_{page_snake}"
    page_view_full = f"{CATALOG}.{SCHEMA}.{page_view_name}"

    print(f"\n{'='*60}\n{page_view_name} — hoja: {page!r}\n{'='*60}")

    # 1. Detectar fact principal: tabla PBI más frecuente entre las measures de los visuales de la hoja
    main_table_pbi = None
    if not visual_fields_df.empty:
        page_fields = visual_fields_df[
            (visual_fields_df['page'] == page)
            & (visual_fields_df['field_type'] == 'Measure')
        ]
        if not page_fields.empty:
            counts = page_fields['table'].dropna().value_counts()
            if len(counts) > 0:
                main_table_pbi = counts.index[0]

    if not main_table_pbi:
        print(f"  ⚠ no se detectó fact principal; salteando")
        continue

    main_uc = _snake(main_table_pbi)

    # 2. Encontrar la view base correspondiente (fuzzy match)
    base_match = None
    if main_uc in base_views:
        base_match = main_uc
    else:
        for fname in base_views:
            if fname.replace('_', '') == main_uc.replace('_', ''):
                base_match = fname
                break

    if not base_match:
        print(f"  ⚠ no encontré view base v_dashboard_{main_uc}; salteando")
        continue

    base_full = base_views[base_match]
    base_cols = base_view_cols[base_match]
    base_col_lower = {c.lower(): c for c in base_cols}
    print(f"  Base view: {base_full} ({len(base_cols)} cols)")

    # 3. Procesar cada filtro de la hoja
    page_filters = pbi_slicers[pbi_slicers['Pagina'] == page]

    rename_clauses = []   # base.<src> AS <alias>
    extra_joins = []      # LEFT JOIN <dim_fqn> alias ON ...
    join_aliases = {}     # {dim_table: alias}
    extra_select = []     # alias.<col> AS <simple_name>
    seen_aliases = set()

    for _, f in page_filters.iterrows():
        pbi_table = str(f['Tabla'])
        pbi_col = str(f['Columna'])
        if not pbi_col or pbi_col == 'nan':
            continue

        col_simple = _snake(pbi_col)
        prefix = _snake(pbi_table)
        col_prefixed = f"{prefix}_{col_simple}"

        if col_simple in seen_aliases:
            continue

        # Caso 2 (chequeado PRIMERO para evitar duplicado): ya existe con nombre simple en la base.
        # Si está, `base.*` ya la trae — NO renombrar ni JOIN, solo marcarla como expuesta.
        if col_simple.lower() in base_col_lower:
            seen_aliases.add(col_simple)
            continue

        # Caso 1: existe con prefijo (ej. dim_estaciones_estatus) — renombrar a alias simple.
        # Solo aplicar si col_simple NO existe ya en la base (caso 2 ya descartado arriba).
        if col_prefixed.lower() in base_col_lower:
            real = base_col_lower[col_prefixed.lower()]
            rename_clauses.append(f"base.`{real}` AS `{col_simple}`")
            seen_aliases.add(col_simple)
            continue

        # Caso 3: no está en la view base — hacer LEFT JOIN a la dim
        # Buscar match en dim_tables (fuzzy con prefix)
        dim_match = None
        for t in dim_tables:
            if t == prefix or t.replace('_', '') == prefix.replace('_', ''):
                dim_match = t
                break

        if not dim_match:
            print(f"    ⚠ filtro '{pbi_table}.{pbi_col}' — no encontré dim '{prefix}' en data_locations")
            continue

        # Buscar la columna específica en la dim
        dim_col_match = None
        for c in dim_tables[dim_match]:
            if _snake(c) == col_simple:
                dim_col_match = c
                break
        if not dim_col_match:
            print(f"    ⚠ filtro '{pbi_table}.{pbi_col}' — no encontré columna en {dim_match}")
            continue

        # Construir JOIN si la dim no se ha joineado todavía
        if dim_match not in join_aliases:
            # Buscar par de columnas comunes para el JOIN entre la view base y la dim
            join_on = None
            for bc in base_cols:
                bc_tail = _snake(bc).split('_')[-1]
                for dc in dim_tables[dim_match]:
                    if _snake(dc) == bc_tail:
                        join_on = (bc, dc)
                        break
                if join_on:
                    break
            if not join_on:
                print(f"    ⚠ no encontré clave de join entre {base_full} y {dim_to_fqn[dim_match]}")
                continue
            alias = f"j_{dim_match}"
            extra_joins.append(
                f"LEFT JOIN {dim_to_fqn[dim_match]} {alias} "
                f"ON base.`{join_on[0]}` = {alias}.`{join_on[1]}`"
            )
            join_aliases[dim_match] = alias
            print(f"    + JOIN {dim_to_fqn[dim_match]} ON base.{join_on[0]} = {alias}.{join_on[1]}")

        alias = join_aliases[dim_match]
        extra_select.append(f"{alias}.`{dim_col_match}` AS `{col_simple}`")
        seen_aliases.add(col_simple)

    # 4. Construir CREATE VIEW
    select_lines = ["base.*"]
    select_lines.extend(rename_clauses)
    select_lines.extend(extra_select)
    select_clause = ",\n  ".join(select_lines)
    join_clause = "\n".join(extra_joins) if extra_joins else ""

    sql = f"""CREATE OR REPLACE VIEW {page_view_full} AS
SELECT
  {select_clause}
FROM {base_full} base
{join_clause}"""

    print(f"  filtros expuestos: {len(seen_aliases)} | joins extra: {len(extra_joins)}")
    try:
        spark.sql(sql)
        print(f"  ✓ {page_view_full}")
        page_views_built.append({
            'page': page,
            'view': page_view_full,
            'base_view': base_full,
            'filters_exposed': len(seen_aliases),
            'extra_joins': len(extra_joins),
            'sql': sql,
        })
    except Exception as e:
        print(f"  ✗ {str(e)[:400]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar tabla de views por hoja

# COMMAND ----------

if page_views_built:
    pv_df = pd.DataFrame(page_views_built)
    spark.createDataFrame(pv_df.astype(str)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{_t('dashboard_page_views')}")
    print(f"\n✓ {CATALOG}.{SCHEMA}.{_t('dashboard_page_views')} — {len(page_views_built)} views por hoja")
    display(pv_df)
else:
    print("\n⚠ No se construyeron views por hoja")

import json
dbutils.notebook.exit(json.dumps({
    "page_views_built": len(page_views_built),
}))
