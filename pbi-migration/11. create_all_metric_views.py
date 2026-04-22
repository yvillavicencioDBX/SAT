# Databricks notebook source
# MAGIC %md
# MAGIC # Crear Metric Views — FATCA/CRS
# MAGIC
# MAGIC Este notebook crea todas las Metric Views necesarias para el dashboard.
# MAGIC
# MAGIC **Instrucciones:**
# MAGIC 1. Ajustar `catalog` y `schema` a los valores de su workspace
# MAGIC 2. Verificar que las tablas fuente existan (`crs__repunc`, `crs_sabana`, `fat_repunc`, `fat_sabana`)
# MAGIC 3. Correr todas las celdas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

dbutils.widgets.text("catalog", "sat_reportes", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Destino: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar tablas fuente

# COMMAND ----------

required_tables = ["crs__repunc", "crs_sabana", "fat_repunc", "fat_sabana"]

for t in required_tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as n FROM {CATALOG}.{SCHEMA}.{t}").collect()[0].n
        print(f"  ✓ {t}: {count} filas")
    except Exception as e:
        print(f"  ✗ {t}: NO EXISTE — {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear Metric Views

# COMMAND ----------

METRIC_VIEWS = {

"mv_crs__repunc": """version: 1.1

source: {catalog}.{schema}.crs__repunc

dimensions:
  - name: crs_version
    expr: CRS_Version
  - name: transmitting_country
    expr: TransmittingCountry
  - name: receiving_country
    expr: ReceivingCountry
  - name: message_ref_id
    expr: MessageRefId
  - name: message_type_indic
    expr: MessageTypeIndic
  - name: reporting_period
    expr: ReportingPeriod
  - name: anio_fiscal
    expr: Anio_Fiscal
  - name: timestamp
    expr: Timestamp
  - name: timestamp_month
    expr: "DATE_TRUNC('MONTH', CAST(Timestamp AS TIMESTAMP))"
  - name: status
    expr: Status
  - name: in_reporting_fi
    expr: IN_ReportingFI
  - name: name_reporting_fi
    expr: Name_ReportingFI
  - name: address_free_reporting_fi
    expr: AddressFree_ReportingFI
  - name: address_fix_reporting_fi
    expr: AddressFix_ReportingFI
  - name: address_free_fix_reporting_fi
    expr: AddressFreeFix_ReportingFI
  - name: doc_type_indic_reporting_fi
    expr: DocTypeIndic_ReportingFI
  - name: doc_ref_id_reporting_fi
    expr: DocRefId_ReportingFI
  - name: corr_doc_ref_id_reporting_fi
    expr: CorrDocRefId_ReportingFI
  - name: in_sponsor
    expr: IN_Sponsor
  - name: name_sponsor
    expr: Name_Sponsor
  - name: address_free_sponsor
    expr: AddressFree_Sponsor
  - name: address_fix_sponsor
    expr: AddressFix_Sponsor
  - name: address_free_fix_sponsor
    expr: AddressFreeFix_Sponsor
  - name: doc_type_indic_sponsor
    expr: DocTypeIndic_Sponsor
  - name: doc_ref_id_sponsor
    expr: DocRefId_Sponsor
  - name: corr_doc_ref_id_sponsor
    expr: CorrDocRefId_Sponsor
  - name: doc_type_indic_account_report
    expr: DocTypeIndic_AccountReport
  - name: doc_ref_id_account_report
    expr: DocRefId_AccountReport
  - name: corr_doc_ref_id_account_report
    expr: CorrDocRefId_AccountReport
  - name: account_number_account_report
    expr: AccountNumber_AccountReport
  - name: account_number_type
    expr: AccountNumberType
  - name: undocumented_account
    expr: UndocumentedAccount
  - name: closed_account
    expr: ClosedAccount
  - name: dormant_account
    expr: DormantAccount
  - name: tin_individual
    expr: TIN_Individual
  - name: name_individual
    expr: Name_Individual
  - name: address_free_individual
    expr: AddressFree_Individual
  - name: address_fix_individual
    expr: AddressFix_Individual
  - name: address_free_fix_individual
    expr: AddressFreeFix_Individual
  - name: birth_date_individual
    expr: BirthDate_Individual
  - name: city_birth_info_individual
    expr: City_BirthInfo_Individual
  - name: country_code_birth_info_individual
    expr: CountryCode_BirthInfo_Individual
  - name: in_organisation
    expr: IN_Organisation
  - name: name_organisation
    expr: Name_Organisation
  - name: address_free_organisation
    expr: AddressFree_Organisation
  - name: address_fix_organisation
    expr: AddressFix_Organisation
  - name: address_free_fix_organisation
    expr: AddressFreeFix_Organisation
  - name: tin_controlling_person
    expr: TIN_ControllingPerson
  - name: name_controlling_person
    expr: Name_ControllingPerson
  - name: address_free_controlling_person
    expr: AddressFree_ControllingPerson
  - name: address_fix_controlling_person
    expr: AddressFix_ControllingPerson
  - name: address_free_fix_controlling_person
    expr: AddressFreeFix_ControllingPerson
  - name: birth_date_controlling_person
    expr: BirthDate_ControllingPerson
  - name: city_birth_info_controlling_person
    expr: City_BirthInfo_ControllingPerson
  - name: country_code_birth_info_controlling_person
    expr: CountryCode_BirthInfo_ControllingPerson
  - name: account_balance
    expr: AccountBalance
  - name: curr_code_account_balance
    expr: CurrCode_AccountBalance
  - name: payment_type_crs501
    expr: PaymentType_CRS501
  - name: curr_code_crs501
    expr: CurrCode_CRS501
  - name: payment_type_crs502
    expr: PaymentType_CRS502
  - name: curr_code_crs502
    expr: CurrCode_CRS502
  - name: payment_type_crs503
    expr: PaymentType_CRS503
  - name: curr_code_crs503
    expr: CurrCode_CRS503
  - name: payment_type_crs504
    expr: PaymentType_CRS504
  - name: curr_code_crs504
    expr: CurrCode_CRS504
  - name: tipo_de_cambio_usd
    expr: TipoDeCambioUSD
  - name: account_balance_usd
    expr: AccountBalanceUSD
  - name: tipo_de_cambio_mxn
    expr: TipoDeCambioMXN
  - name: account_balance_mxn
    expr: AccountBalanceMXN
  - name: rfc_localizado
    expr: Rfc_localizado
  - name: marca_rfc
    expr: Marca_rfc
  - name: nombre_higienizado
    expr: Nombre_higienizado
  - name: domicilio_higienizado
    expr: Domicilio_higienizado
  - name: id_ejecucion
    expr: IdEjecucion
  - name: operacion
    expr: Operacion
  - name: name
    expr: Name

measures:
  - name: filter_menu_crs
    expr: CASE WHEN COUNT(DISTINCT name) = 1 THEN CASE WHEN ANY_VALUE(name) = 'Individual' THEN 'Individual CRS' WHEN ANY_VALUE(name) = 'Organization' THEN 'Organization CRS' ELSE 'Menu' END ELSE 'CRS_Reporte Único' END
    display_name: Filter Menu CRS
  - name: registros_crs_reporte_unc
    expr: COUNT(1)
    display_name: Registros CRS ReporteUnc
    format:
      type: number
  - name: crs_registros_global_rep_unc
    expr: COUNT(1)
    window:
      - order: crs_version
        semiadditive: last
        range: all
    display_name: CRS Registros Global RepUnc
    format:
      type: number
""",

"mv_crs_sabana": """version: 1.1

source: {catalog}.{schema}.crs_sabana

dimensions:
  - name: accountnumber_accountreport
    expr: AccountNumber_AccountReport
  - name: acctholdertype_accountholder
    expr: AcctHolderType_AccountHolder
  - name: anio_fiscal
    expr: Anio_Fiscal
  - name: birthdate_controllingperson
    expr: BirthDate_ControllingPerson
  - name: birthdate_individual
    expr: BirthDate_Individual
  - name: contact
    expr: Contact
  - name: messagerefid
    expr: MessageRefId
  - name: messagetype
    expr: MessageType
  - name: paymenttype_payment
    expr: PaymentType_Payment
  - name: receivingcountry
    expr: ReceivingCountry
  - name: reportingperiod
    expr: ReportingPeriod
  - name: timestamp
    expr: Timestamp
  - name: timestamp_month
    expr: "DATE_TRUNC('MONTH', Timestamp)"
  - name: transmittingcountry
    expr: TransmittingCountry
  - name: warning
    expr: Warning
  - name: fechainsercion
    expr: fechaInsercion
  - name: fechainsercion_month
    expr: "DATE_TRUNC('MONTH', fechaInsercion)"
  - name: idejecucion
    expr: idejecucion
  - name: nametype_individual
    expr: nameType_Individual
  - name: nametype_organisation
    expr: nameType_Organisation
  - name: p_fechainsercion
    expr: p_fechaInsercion
  - name: p_fechainsercion_month
    expr: "DATE_TRUNC('MONTH', p_fechaInsercion)"
  - name: version
    expr: version

measures:
  - name: registros_crs_sabana
    expr: COUNT(1)
    display_name: Registros CRS Sabana
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: crs_registros_global_sabana
    expr: COUNT(1)
    window:
      - order: accountnumber_accountreport
        semiadditive: last
        range: all
    display_name: CRS Registros Global Sabana
    format:
      type: number
""",

"mv_fat_repunc": """version: 1.1

source: {catalog}.{schema}.fat_repunc

dimensions:
  - name: account_closed
    expr: AccountClosed
  - name: account_number
    expr: AccountNumber
  - name: account_number_type
    expr: AccountNumberType
  - name: address_fix_individual
    expr: AddressFix_Individual
  - name: address_fix_intermediary
    expr: AddressFix_Intermediary
  - name: address_fix_organisation
    expr: AddressFix_Organisation
  - name: address_fix_reporting_fi
    expr: AddressFix_ReportingFI
  - name: address_fix_sponsor
    expr: AddressFix_Sponsor
  - name: address_fix_substantial_owner_individual
    expr: AddressFix_SubstantialOwner_Individual
  - name: address_fix_substantial_owner_organisation
    expr: AddressFix_SubstantialOwner_Organisation
  - name: address_free_fix_individual
    expr: AddressFreeFix_Individual
  - name: address_free_fix_intermediary
    expr: AddressFreeFix_Intermediary
  - name: address_free_fix_organisation
    expr: AddressFreeFix_Organisation
  - name: address_free_fix_reporting_fi
    expr: AddressFreeFix_ReportingFI
  - name: address_free_fix_sponsor
    expr: AddressFreeFix_Sponsor
  - name: address_free_fix_substantial_owner_individual
    expr: AddressFreeFix_SubstantialOwner_Individual
  - name: address_free_fix_substantial_owner_organisation
    expr: AddressFreeFix_SubstantialOwner_Organisation
  - name: address_free_individual
    expr: AddressFree_Individual
  - name: address_free_intermediary
    expr: AddressFree_Intermediary
  - name: address_free_organisation
    expr: AddressFree_Organisation
  - name: address_free_reporting_fi
    expr: AddressFree_ReportingFI
  - name: address_free_sponsor
    expr: AddressFree_Sponsor
  - name: address_free_substantial_owner_individual
    expr: AddressFree_SubstantialOwner_Individual
  - name: address_free_substantial_owner_organisation
    expr: AddressFree_SubstantialOwner_Organisation
  - name: birth_date_individual
    expr: BirthDate_Individual
  - name: birth_date_substantial_owner_individual
    expr: BirthDate_SubstantialOwner_Individual
  - name: city_birth_date_individual
    expr: City_BirthDate_Individual
  - name: corr_doc_ref_id_account_report
    expr: CorrDocRefId_AccountReport
  - name: corr_doc_ref_id_intermediary
    expr: CorrDocRefId_Intermediary
  - name: corr_doc_ref_id_nil_report
    expr: CorrDocRefId_NilReport
  - name: corr_doc_ref_id_reporting_fi
    expr: CorrDocRefId_ReportingFI
  - name: corr_doc_ref_id_sponsor
    expr: CorrDocRefId_Sponsor
  - name: corr_message_ref_id
    expr: CorrMessageRefId
  - name: corr_message_ref_id_account_report
    expr: CorrMessageRefId_AccountReport
  - name: corr_message_ref_id_intermediary
    expr: CorrMessageRefId_Intermediary
  - name: corr_message_ref_id_nil_report
    expr: CorrMessageRefId_NilReport
  - name: corr_message_ref_id_reporting_fi
    expr: CorrMessageRefId_ReportingFI
  - name: corr_message_ref_id_sponsor
    expr: CorrMessageRefId_Sponsor
  - name: country_code_country_info_individual
    expr: CountryCode_CountryInfo_Individual
  - name: curr_code_account_balance
    expr: CurrCode_AccountBalance
  - name: curr_code_fatca501
    expr: CurrCode_FATCA501
  - name: curr_code_fatca502
    expr: CurrCode_FATCA502
  - name: curr_code_fatca503
    expr: CurrCode_FATCA503
  - name: curr_code_fatca504
    expr: CurrCode_FATCA504
  - name: marca_rfc
    expr: Marca_rfc
  - name: message_ref_id
    expr: MessageRefId
  - name: name
    expr: Name
  - name: name_individual
    expr: Name_Individual
  - name: name_organisation
    expr: Name_Organisation
  - name: receiving_country
    expr: ReceivingCountry
  - name: reporting_period
    expr: ReportingPeriod
  - name: rfc_localizado
    expr: Rfc_localizado
  - name: tin_individual
    expr: TIN_Individual
  - name: tin_organisation
    expr: TIN_Organisation
  - name: timestamp
    expr: Timestamp
  - name: timestamp_month
    expr: "DATE_TRUNC('MONTH', CAST(Timestamp AS TIMESTAMP))"
  - name: transmitting_country
    expr: TransmittingCountry
  - name: id_ejecucion
    expr: idEjecucion

measures:
  - name: registros_rep_unico
    expr: COUNT(idEjecucion)
    display_name: Registros Rep Único
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: filter_menu_fatca
    expr: CASE WHEN COUNT(DISTINCT Name) = 1 THEN CASE WHEN ANY_VALUE(Name) = 'Individual' THEN 'Individual FATCA' WHEN ANY_VALUE(Name) = 'Organization' THEN 'Organization FATCA' ELSE 'Menu' END ELSE 'Fat_Reporte Único' END
    display_name: Filter Menu FATCA
  - name: fat_registros_global_rep_unc
    expr: COUNT(1)
    window:
      - order: id_ejecucion
        semiadditive: last
        range: all
    display_name: FAT Registros Global RepUnc
    format:
      type: number
""",

"mv_fat_sabana": """version: 1.1

source: {catalog}.{schema}.fat_sabana

dimensions:
  - name: accountclosed
    expr: AccountClosed
  - name: accountnumber
    expr: AccountNumber
  - name: accountnumbertype
    expr: AccountNumberType
  - name: acctholdertype
    expr: AcctHolderType
  - name: birthdate_individual
    expr: BirthDate_Individual
  - name: birthdate_substantialowner_individual
    expr: BirthDate_SubstantialOwner_individual
  - name: contact
    expr: Contact
  - name: corrmessagerefid
    expr: CorrMessageRefId
  - name: messagerefid
    expr: MessageRefId
  - name: messagetype
    expr: MessageType
  - name: payment
    expr: Payment
  - name: paymenttype
    expr: PaymentType
  - name: receivingcountry
    expr: ReceivingCountry
  - name: reportingperiod
    expr: ReportingPeriod
  - name: timestamp
    expr: Timestamp
  - name: timestamp_month
    expr: "DATE_TRUNC('MONTH', Timestamp)"
  - name: transmittingcountry
    expr: TransmittingCountry
  - name: warning
    expr: Warning
  - name: fechainsercion
    expr: fechaInsercion
  - name: fechainsercion_month
    expr: "DATE_TRUNC('MONTH', fechaInsercion)"
  - name: idejecucion
    expr: idejecucion
  - name: nametype_individual
    expr: nameType_Individual
  - name: nametype_organisation
    expr: nameType_Organisation
  - name: p_fechainsercion
    expr: p_fechaInsercion
  - name: p_fechainsercion_month
    expr: "DATE_TRUNC('MONTH', p_fechaInsercion)"
  - name: version
    expr: version

measures:
  - name: registros_sabana
    expr: COUNT(idejecucion)
    display_name: Registros Sabana
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: fat_registros_global_sabana
    expr: COUNT(1)
    window:
      - order: accountnumber
        semiadditive: last
        range: all
    display_name: FAT Registros Global Sabana
    format:
      type: number
""",

}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear cada Metric View

# COMMAND ----------

results = []

for mv_name, yaml_template in METRIC_VIEWS.items():
    full_name = f"{CATALOG}.{SCHEMA}.{mv_name}"
    yaml_text = yaml_template.format(catalog=CATALOG, schema=SCHEMA)

    print(f"\n--- {full_name} ---")
    try:
        spark.sql(f"""
            CREATE OR REPLACE VIEW {full_name}
            WITH METRICS
            LANGUAGE YAML
            AS $$
{yaml_text}
$$""")
        # Verificar
        cols = spark.sql(f"DESCRIBE {full_name}").collect()
        dims = [c.col_name for c in cols if not c.col_name.startswith('#') and 'measure' not in (c.data_type or '')]
        measures = [c.col_name for c in cols if 'measure' in (c.data_type or '')]
        print(f"  ✓ Creada: {len(dims)} dims, {len(measures)} measures")
        results.append({"view": full_name, "status": "OK", "dims": len(dims), "measures": len(measures)})
    except Exception as e:
        print(f"  ✗ Error: {str(e)[:200]}")
        results.append({"view": full_name, "status": f"FAIL: {str(e)[:100]}", "dims": 0, "measures": 0})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumen

# COMMAND ----------

import pandas as pd

print(f"{'='*60}")
print(f"METRIC VIEWS CREADAS")
print(f"{'='*60}")
print(f"Catálogo: {CATALOG}.{SCHEMA}")
print()
for r in results:
    icon = "✓" if r["status"] == "OK" else "✗"
    print(f"  {icon} {r['view']}: {r['dims']} dims, {r['measures']} measures")

ok = sum(1 for r in results if r["status"] == "OK")
fail = len(results) - ok
print(f"\nTotal: {ok} OK, {fail} FAIL")

display(pd.DataFrame(results))
