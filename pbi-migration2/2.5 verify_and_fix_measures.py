# Databricks notebook source
# MAGIC %md
# MAGIC # 2.5 Verificar y arreglar measures con runtime errors
# MAGIC
# MAGIC Toma cada `mv_*_<run_id>` ya creada, ejecuta cada measure contra el warehouse
# MAGIC (full scan, sin LIMIT) y detecta errores como `CAST_OVERFLOW`, `CAST_INVALID_INPUT`,
# MAGIC `DIVIDE_BY_ZERO`, etc. Para cada measure que falle:
# MAGIC
# MAGIC 1. Lee el YAML actual de la MV
# MAGIC 2. Manda a Claude la measure + el error + el YAML
# MAGIC 3. Aplica el fix (CREATE OR REPLACE VIEW con la expresión corregida)
# MAGIC 4. Re-testea
# MAGIC
# MAGIC Es idempotente — puedes correrlo varias veces.

# COMMAND ----------

# DBTITLE 1,Parámetros
import json, re, requests, time

try:
    _CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except Exception:
    _CURRENT_USER = ""

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("run_id", "", "Sufijo identificador de corrida (vacío = sin sufijo)")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4", "Endpoint LLM")
dbutils.widgets.text("max_fix_attempts", "3", "Reintentos máximos por measure")
dbutils.widgets.text("only_mv", "", "(opcional) Solo procesar esta MV (sufijo no incluido), vacío = todas")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
RUN_ID = dbutils.widgets.get("run_id").strip()
SUFFIX = f"_{RUN_ID}" if RUN_ID else ""
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint").strip() or "databricks-claude-sonnet-4"
MAX_FIX_ATTEMPTS = int(dbutils.widgets.get("max_fix_attempts") or "3")
ONLY_MV = dbutils.widgets.get("only_mv").strip()

print(f"Catálogo:  {CATALOG}.{SCHEMA}")
print(f"Run ID:    {RUN_ID or '(sin sufijo)'}")
print(f"LLM:       {LLM_ENDPOINT}")
print(f"Reintentos:{MAX_FIX_ATTEMPTS}")
if ONLY_MV:
    print(f"Solo MV:   {ONLY_MV}")

# COMMAND ----------

# DBTITLE 1,Cliente LLM con retry
_MODELS_NO_TEMPERATURE = {'databricks-claude-opus-4-7'}
_RETRY_STATUS = {502, 503, 504, 429}
_MAX_RETRIES = 5

def _post_with_retry(url, headers, payload, timeout=120):
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

def call_claude(prompt, system_prompt="You fix Databricks Metric View measure SQL.", max_tokens=2000):
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

# DBTITLE 1,Ejecutor de queries via SQL Warehouse
def _get_warehouse_id():
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    r = requests.get(f"https://{host}/api/2.0/sql/warehouses",
                     headers={"Authorization": f"Bearer {token}"}, timeout=30)
    whs = r.json().get('warehouses', [])
    for w in whs:
        if 'serverless' in w.get('name','').lower() or w.get('warehouse_type') == 'PRO':
            return w['id']
    return whs[0]['id'] if whs else None

WAREHOUSE_ID = _get_warehouse_id()
print(f"Warehouse: {WAREHOUSE_ID}")

def run_sql(sql, timeout_seconds=180):
    """Ejecuta SQL y devuelve (ok, error_msg_o_None)."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    r = requests.post(f"https://{host}/api/2.0/sql/statements",
        json={"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "0s"},
        headers={"Authorization": f"Bearer {token}"}, timeout=30)
    stmt = r.json()
    stmt_id = stmt.get('statement_id', '')
    state = stmt.get('status', {}).get('state', '')
    start = time.time()
    while state in ('PENDING', 'RUNNING', '') and (time.time() - start) < timeout_seconds:
        time.sleep(2)
        poll = requests.get(f"https://{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"}, timeout=30).json()
        state = poll.get('status', {}).get('state', '')
        stmt = poll
    if state == 'SUCCEEDED':
        return True, None
    return False, stmt.get('status', {}).get('error', {}).get('message', f'state={state}')

# COMMAND ----------

# DBTITLE 1,Descubrir MVs y sus measures
def get_mv_yaml(view_name):
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    r = requests.get(f"https://{host}/api/2.1/unity-catalog/tables/{view_name}",
                     headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json().get("view_definition", "")

def parse_measures_from_yaml(yaml_text):
    """Devuelve lista de dicts {name, expr, raw_block} de cada measure en el YAML."""
    measures = []
    in_meas = False
    cur = None
    block_lines = []
    for line in yaml_text.split('\n'):
        stripped = line.strip()
        if stripped == 'measures:' or stripped.startswith('measures:'):
            in_meas = True
            continue
        if in_meas:
            if stripped.startswith('- name:'):
                if cur:
                    cur['raw_block'] = '\n'.join(block_lines)
                    measures.append(cur)
                cur = {'name': stripped.replace('- name:', '').strip().strip("'\""), 'expr': '', 'raw_block': ''}
                block_lines = [line]
            elif cur and stripped.startswith('expr:'):
                cur['expr'] = stripped.replace('expr:', '').strip().strip("'\"")
                block_lines.append(line)
            elif stripped and not line.startswith(' ') and not line.startswith('\t') and ':' in stripped:
                # Otra sección — terminar measures
                if cur:
                    cur['raw_block'] = '\n'.join(block_lines)
                    measures.append(cur)
                    cur = None
                in_meas = False
            elif cur:
                block_lines.append(line)
    if cur:
        cur['raw_block'] = '\n'.join(block_lines)
        measures.append(cur)
    return [m for m in measures if m.get('name') and m['name'] != '__row_count']

# Listar MVs de esta corrida
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
mvs = []
for r in tables:
    t = r.tableName
    if not t.startswith("mv_"):
        continue
    if SUFFIX and not t.endswith(SUFFIX):
        continue
    if ONLY_MV:
        if ONLY_MV not in t:
            continue
    mvs.append(t)

print(f"MVs a verificar: {len(mvs)}")
for m in mvs: print(f"  {m}")

# COMMAND ----------

# DBTITLE 1,Verificar y arreglar measures por MV
all_results = []

for mv_name in mvs:
    view_fqn = f"{CATALOG}.{SCHEMA}.{mv_name}"
    print(f"\n{'='*70}")
    print(f"{view_fqn}")
    print('='*70)

    try:
        yaml_text = get_mv_yaml(view_fqn)
    except Exception as e:
        print(f"  ✗ no pude leer YAML: {str(e)[:200]}")
        continue

    measures = parse_measures_from_yaml(yaml_text)
    print(f"  {len(measures)} measures a probar")

    failing = []
    for m in measures:
        mname = m['name']
        sql = f"SELECT MEASURE(`{mname}`) FROM {view_fqn}"
        ok, err = run_sql(sql)
        if ok:
            print(f"  ✓ {mname}")
        else:
            print(f"  ✗ {mname}: {(err or '')[:140]}")
            failing.append({'name': mname, 'expr': m['expr'], 'raw_block': m['raw_block'], 'error': err})

    if not failing:
        print(f"  → Todas pasan, nada que arreglar")
        all_results.append({'mv': mv_name, 'total': len(measures), 'failing_initial': 0, 'fixed': 0, 'still_failing': 0})
        continue

    # Para cada measure que falla, pedir fix a Claude e intentar
    fixed_count = 0
    still_failing = []

    for f in failing:
        mname = f['name']
        print(f"\n  -> Arreglando '{mname}'…")

        # Buscar todos los CASTs y SUMs sospechosos en la expresión
        current_expr = f['expr']

        for attempt in range(MAX_FIX_ATTEMPTS):
            fix_prompt = f"""Fix this SQL expression for a Databricks Metric View measure that fails with a runtime error.

Measure name: {mname}
Current expression: {current_expr}

Error when running `SELECT MEASURE({mname}) FROM {view_fqn}`:
{f['error']}

Full YAML of the metric view (for context — see dimensions, source, other measures):
```yaml
{yaml_text[:5000]}
```

RULES for the fix:
- CAST_OVERFLOW / CAST_INVALID_INPUT → replace CAST(...) with TRY_CAST(...). Returns NULL on bad data instead of failing.
  Example: CAST(col AS BIGINT) → TRY_CAST(col AS BIGINT). CAST(col AS INTEGER) → TRY_CAST(col AS INTEGER).
- DIVIDE_BY_ZERO → wrap denominator in NULLIF(x, 0).
- UNRESOLVED_COLUMN → check the YAML above for the actual dimension names (preserve case, no lowercase). The error message often suggests the right name.
- DATATYPE_MISMATCH → cast operands explicitly with TRY_CAST. For dates from BIGINT, use DATE_FROM_UNIX_DATE(col).
- AGGREGATE inside FILTER → move the aggregate to a scalar subquery (SELECT MAX(...) FROM source_fqn) and reference it.
- Window functions are NOT allowed in measure expr; use ANY_VALUE(dim_name) over a pre-computed Fixed LOD dimension.
- Preserve the same semantic intent — return the same kind of number, not just a hack.

Return ONLY a JSON object: {{"sql_expr": "fixed expression here"}}
OR if truly unrecoverable: {{"sql_expr": null, "reason": "explanation"}}
No markdown fences."""

            try:
                resp = call_claude(fix_prompt, max_tokens=2000)
                # Parse JSON
                _txt = resp.strip()
                if _txt.startswith("```"):
                    _txt = _txt.split('\n', 1)[1] if '\n' in _txt else _txt
                    if _txt.endswith("```"):
                        _txt = _txt.rsplit('\n', 1)[0]
                m = re.search(r'\{.*\}', _txt, re.DOTALL)
                if m:
                    _txt = m.group()
                fix_json = json.loads(_txt)
                new_expr = fix_json.get('sql_expr')
            except Exception as e:
                print(f"    [attempt {attempt+1}] parse error: {str(e)[:120]}")
                continue

            if not new_expr:
                reason = fix_json.get('reason', 'sin razón')
                print(f"    [attempt {attempt+1}] Claude dice unrecoverable: {reason[:120]}")
                break

            # Aplicar el fix: reemplazar el expr en el YAML
            # Buscar el bloque de la measure y reemplazar la línea expr:
            new_yaml = yaml_text
            old_block = f['raw_block']
            new_expr_line_replaced = re.sub(
                r'^(\s*expr:\s*).*$',
                lambda mm: f'{mm.group(1)}"{new_expr}"' if any(c in new_expr for c in ':#&*[]{},?!|>\'"`') else f'{mm.group(1)}{new_expr}',
                old_block,
                count=1, flags=re.MULTILINE
            )
            new_yaml = new_yaml.replace(old_block, new_expr_line_replaced)

            # CREATE OR REPLACE VIEW con el YAML corregido
            create_sql = f"""CREATE OR REPLACE VIEW {view_fqn}
WITH METRICS
LANGUAGE YAML
AS $$
{new_yaml}
$$"""
            ok_create, err_create = run_sql(create_sql)
            if not ok_create:
                print(f"    [attempt {attempt+1}] CREATE falló: {(err_create or '')[:140]}")
                continue

            # Re-testear la measure
            test_sql = f"SELECT MEASURE(`{mname}`) FROM {view_fqn}"
            ok_test, err_test = run_sql(test_sql)
            if ok_test:
                print(f"    ✓ FIXED en attempt {attempt+1}: {new_expr[:80]}")
                yaml_text = new_yaml  # persistir para siguientes measures
                fixed_count += 1
                break
            else:
                print(f"    [attempt {attempt+1}] sigue fallando: {(err_test or '')[:140]}")
                f['error'] = err_test
                current_expr = new_expr
        else:
            still_failing.append({'name': mname, 'last_error': f['error']})

    all_results.append({
        'mv': mv_name,
        'total': len(measures),
        'failing_initial': len(failing),
        'fixed': fixed_count,
        'still_failing': len(failing) - fixed_count,
        'unresolved': [s['name'] for s in still_failing],
    })

# COMMAND ----------

# DBTITLE 1,Resumen
print(f"\n{'='*70}")
print("RESUMEN VERIFICADOR")
print('='*70)
print(f"{'MV':<60} {'OK':>5} {'Fallos':>7} {'Fixed':>6} {'Pend':>5}")
for r in all_results:
    print(f"{r['mv']:<60} {r['total'] - r['failing_initial']:>5} {r['failing_initial']:>7} {r['fixed']:>6} {r['still_failing']:>5}")

total_total = sum(r['total'] for r in all_results)
total_fixed = sum(r['fixed'] for r in all_results)
total_pend  = sum(r['still_failing'] for r in all_results)
print(f"\nTOTAL — measures: {total_total}, arregladas: {total_fixed}, pendientes: {total_pend}")

if total_pend > 0:
    print(f"\nMeasures que quedaron sin arreglar:")
    for r in all_results:
        for u in r.get('unresolved', []):
            print(f"  {r['mv']} → {u}")

