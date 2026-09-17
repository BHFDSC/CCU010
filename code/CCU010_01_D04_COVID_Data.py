# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **Project(s)** CCU010_01 (from CCU018_01 (from CCU002_01 (from CCU013)))
# MAGIC  
# MAGIC **Original Author(s)** Tom Bolton, Chris Tomlinson, Johan Thygesen, Sam Hollings, Jenny Cooper, Samantha Ip, John Nolan, Elena Raffetti
# MAGIC  
# MAGIC **CCU010 Project Analyst** Sharmin Shabnam
# MAGIC  
# MAGIC **Date last updated** 2021-06-22

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "Helper_functions/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

#Datasets
# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu010_01'


# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = ''
dbc = f'{db}_collab'


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

# in tables (available post table_freeze)
path_deaths        = f'{dbc}.{proj}_freeze_deaths_20220930'
path_gdppr         = f'{dbc}.{proj}_freeze_gdppr_20220930'
path_hes_apc       = f'{dbc}.{proj}_freeze_hes_apc_20220930'
path_hes_op        = f'{dbc}.{proj}_freeze_hes_op_20220930'
path_hes_ae        = f'{dbc}.{proj}_freeze_hes_ae_20220930'
path_hes_cc        = f'{dbc}.{proj}_freeze_hes_cc_20220930'
path_chess         = f'{dbc}.{proj}_freeze_chess_20220930'
path_sgss          = f'{dbc}.{proj}_freeze_sgss_20220930'
path_sus           = f'{dbc}.{proj}_freeze_sus_20220930'
path_vacc          = f'{dbc}.{proj}_freeze_vaccine_status_20220930'
path_pmeds         = f'{dbc}.{proj}_freeze_primary_care_meds_20220930'

path_codelist      = f'{dbc}.{proj}_codelists'

start_date = '2020-01-31'
end_date = '2022-10-31' 

save_data = True

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist = spark.table(path_codelist) 
gdppr    = spark.table(path_gdppr)
hes_apc  = spark.table(path_hes_apc)
hes_cc   = spark.table(path_hes_cc)
sus      = spark.table(path_sus)
sgss     = spark.table(path_sgss)
chess    = spark.table(path_chess)
deaths   = spark.table(path_deaths)

# COMMAND ----------

def save_dataset(name, dataset):
  proj = 'ccu010_01'
  db = ''
  dbc = f'{db}_collab'
  
  outName = f'{proj}{name}'.lower()
  dataset.createOrReplaceGlobalTempView(outName)
  drop_table(outName, if_exists=True)
  create_table(outName, select_sql_script=f"SELECT * FROM global_temp.{outName}")
  
def read_dataset(name):
  spark.sql('CLEAR CACHE')
  path_tmp_covid = f'{dbc}.{proj}{name}'
  spark.sql(F"""REFRESH TABLE {path_tmp_covid}""")
  dataset = spark.table(path_tmp_covid)
  return dataset

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 GDPPR SGSS CHESS

# COMMAND ----------

# ==========================================
#              GDPPR
# ==========================================
# gdppr
_gdppr = (gdppr
  .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))
  .dropDuplicates()
         )

# ==========================================
#              SGSS
# ==========================================
# sgss
_sgss = (sgss
  .select(['PERSON_ID_DEID', 'Reporting_Lab_ID', 'Specimen_Date'])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .withColumnRenamed('Specimen_Date', 'DATE')
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))
  .dropDuplicates()
        )

# ==========================================
#              CHESS
# ==========================================
# chess
_chess = (chess
          .select(['PERSON_ID_DEID', 'Typeofspecimen', 'Covid19', 'AdmittedToICU', 'Highflownasaloxygen', 
                   'NoninvasiveMechanicalventilation', 'Invasivemechanicalventilation', 'RespiratorySupportECMO', 
                   'DateAdmittedICU', 'HospitalAdmissionDate', 'InfectionSwabDate', 'FinalOutcomeDate'])
          .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
          .withColumnRenamed('HospitalAdmissionDate', 'DATE')
          .withColumnRenamed('FinalOutcomeDate', 'ENDDATE')
          .where(f.col('Covid19') == 'Yes')
          .where(f.col('PERSON_ID').isNotNull())
          .where(
            ((f.col('DATE') >= start_date) | (f.col('DATE').isNull()))
            & ((f.col('DATE') <= end_date) | (f.col('DATE').isNull()))
          )
          .where(
            ((f.col('DateAdmittedICU') >= start_date) | (f.col('DateAdmittedICU').isNull()))
            & ((f.col('DateAdmittedICU') <= end_date) | (f.col('DateAdmittedICU').isNull()))
          )
          .distinct()
          .withColumn('DATE', f.when(f.col('DATE') > f.col('ENDDATE'),
                                f.col('InfectionSwabDate')).otherwise(f.col('DATE')))
          .where(f.col('DATE') <= f.col('ENDDATE'))
         )

# COMMAND ----------

# MAGIC %md ## 2.2 HES APC CC SUS

# COMMAND ----------

# ==========================================
#              HES APC
# ==========================================
window = Window.partitionBy(['PERSON_ID_DEID', 'ADMIDATE' ])\
               .orderBy('DISDATE')\
               .rowsBetween(0,f.sys.maxsize)
_hes_apc = (hes_apc
            .select(['PERSON_ID_DEID', 'ADMIDATE','DISDATE', 
                      'DIAG_4_CONCAT', 'DIAG_4_01', 'OPERTN_4_CONCAT', 'SUSRECID',
                      'EPISTART', 'EPIEND'])
            .withColumn('DISDATE',
                         f.when(f.col('DISDATE')<='1900-01-01',
                                f.lit(None)).otherwise(f.col('DISDATE')))
            .orderBy('PERSON_ID_DEID','ADMIDATE', 'DISDATE', ascending = True)
            .withColumn('DISDATE',
                               f.first(f.col('DISDATE'), ignorenulls=True).over(window))
           )

_hes_apc = (_hes_apc
            .select(['PERSON_ID_DEID', 'ADMIDATE','DISDATE', 
                      'DIAG_4_CONCAT', 'DIAG_4_01', 'OPERTN_4_CONCAT', 'SUSRECID',
                      'EPISTART', 'EPIEND'])
            .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
            .withColumnRenamed('ADMIDATE', 'DATE') 
            .withColumnRenamed('DISDATE', 'ENDDATE')
            .where(f.col('DIAG_4_CONCAT').rlike('U07(1|2)'))
            .withColumn('ENDDATE',
                         f.when((f.col('ENDDATE').isNull()),
                                f.col('EPIEND')).otherwise(f.col('ENDDATE')))
            .withColumn('DATE',
                         f.when(
                           (f.col('DATE') > f.col('ENDDATE')) 
                           | (f.col('DATE').isNull())
                           | (f.col('DATE') > f.col('EPISTART')),
                                f.col('EPISTART')).otherwise(f.col('DATE')))
            .dropDuplicates()
            .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))
            .where(f.col('DATE') <= f.col('ENDDATE'))
           )

# COMMAND ----------

# ==========================================
#              SUS
# ==========================================
_sus = (sus
        .select(['NHS_NUMBER_DEID', 'EPISODE_START_DATE', 'PRIMARY_PROCEDURE_DATE', 'SECONDARY_PROCEDURE_DATE_1',
                 'DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL', 'DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL',
                 'END_DATE_HOSPITAL_PROVIDER_SPELL','START_DATE_HOSPITAL_PROVIDER_SPELL'   
                ]
                + [col for col in sus.columns if re.match('.*(DIAGNOSIS|PROCEDURE)_CODE.*', col)]
               )
        .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
        .withColumnRenamed('START_DATE_HOSPITAL_PROVIDER_SPELL', 'DATE') ##???????????
        .withColumnRenamed('END_DATE_HOSPITAL_PROVIDER_SPELL', 'ENDDATE')
        .withColumn('DIAG_CONCAT',
                    f.concat_ws(',', *[col for col in sus.columns if re.match('.*DIAGNOSIS_CODE.*', col)]))
        .withColumn('PROCEDURE_CONCAT',
                    f.concat_ws(',', *[col for col in sus.columns if re.match('.*PROCEDURE_CODE.*', col)]))
        .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))
        .where(f.col('DIAG_CONCAT').rlike('U07(1|2)'))
        .where(
          ((f.col('ENDDATE') >= start_date) 
           | (f.col('ENDDATE').isNull()))
          & ((f.col('ENDDATE') <= end_date) 
             | (f.col('ENDDATE').isNull()))
        )
        .where((f.col('PERSON_ID').isNotNull()) & (f.col('DATE').isNotNull()))
        .dropDuplicates()
       )

# COMMAND ----------

# MAGIC %md ## 2.2 Death

# COMMAND ----------

# ==========================================
#              DEATHS
# ==========================================
from datetime import date, timedelta

sdate = date(1900,1,1)     # start date
edate = date(2022,10,31)   # end date
dates = pd.date_range(sdate,edate-timedelta(days=1),freq='d').strftime('%Y%m%d').tolist()
_deaths_dates_invalid = (deaths
                         .select(['REG_DATE_OF_DEATH'])
                         .distinct()
                         .filter(~f.col("REG_DATE_OF_DEATH").isin(dates))
                        )
_deaths_dates_invalid = [row.REG_DATE_OF_DEATH for row in _deaths_dates_invalid.select("REG_DATE_OF_DEATH").collect()]
_deaths_dates_invalid

_win = (Window
      .partitionBy('PERSON_ID')
      .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))
           )

_deaths = (deaths
           .select(['DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH', 'REG_DATE',
                    'S_UNDERLYING_COD_ICD10', 'S_COD_CODE_1', 
                    'S_COD_CODE_2', 'S_COD_CODE_3', 'S_COD_CODE_4', 'S_COD_CODE_5', 'S_COD_CODE_6',
                    'S_COD_CODE_7', 'S_COD_CODE_8', 'S_COD_CODE_9', 'S_COD_CODE_10', 'S_COD_CODE_11', 
                    'S_COD_CODE_12', 'S_COD_CODE_13', 'S_COD_CODE_14', 'S_COD_CODE_15'])
           .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')
           .withColumn("REG_DATE_OF_DEATH",
                       f.when(f.col("REG_DATE_OF_DEATH").isin(_deaths_dates_invalid), f.col("REG_DATE"))
                       .otherwise(f.col("REG_DATE_OF_DEATH")))
           .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
           .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))
           .withColumn('_rownum', f.row_number().over(_win))
           .where(
             (f.col('_rownum') == 1)
             & (f.col('PERSON_ID').isNotNull())
             & (f.col('REG_DATE_OF_DEATH').isNotNull())
             & (f.col('REG_DATE_OF_DEATH') > '1900-01-01')
             & (f.col('REG_DATE_OF_DEATH') <= '2022-10-31') 
           )
           .drop('_rownum')
           .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_0')
           .where((f.col('REG_DATE_OF_DEATH') >= start_date) & (f.col('REG_DATE_OF_DEATH') <= end_date))
           .distinct() 
          )
_deaths = (_deaths
           .withColumn('S_COD_CODE_CONCAT',
                       f.concat_ws(',', *[col for col in _deaths.columns if re.match('.*_COD_.*', col)]))
          )

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 COVID positive

# COMMAND ----------

# MAGIC %md ## 3.1 SGSS GDPPR

# COMMAND ----------

# ==========================================
#              SGSS POSITIVE
# ==========================================
# sgss
# note: all records are included as every record is a "positive test"
# -- TODO: wranglers please clarify whether LAB ID 840 is still the best means of identifying pillar 1 vs 2
#   .withColumn('description', f.when(f.col('Reporting_Lab_ID') == '840', 'pillar_2').otherwise('pillar_1'))\
_sgss_pos = (_sgss
             .withColumn('COVID_PHENOTYPE', f.lit('01_covid_positive_test'))
             .withColumn('CODE_TYPE', f.lit('no_code_for_sgss'))
             .withColumn('DESCRIPTION', f.lit('antigen_test_reports'))
             .withColumn('COVID_STATUS', f.lit('confirmed'))
             .withColumn('CODE', f.lit('none'))
             .withColumn('SOURCE', f.lit('sgss'))
             .select('PERSON_ID', 'DATE', 'COVID_PHENOTYPE', 'COVID_STATUS',
                     'DESCRIPTION', 'CODE_TYPE', 'CODE', 'SOURCE')
            )


# ==========================================
#              GDPPR POSITIVE
# ==========================================
# gdppr
# note: need to inspect and identify which are only suspected NOT confirmed!
_codelist_gdppr = (codelist
                   .where(f.col('name') == 'GDPPR_confirmed_COVID')
                   .select(['code', 'term'])
                  )
# tmpt = tab(codelist, 'name', 'terminology', var2_unstyled=1)

_gdppr_pos = (_gdppr
              .select(['PERSON_ID', 'DATE', 'CODE'])
              .join(f.broadcast(_codelist_gdppr), on='code', how='inner')
              .withColumn('COVID_PHENOTYPE', f.lit('01_gp_covid_diagnosis'))
              .withColumnRenamed('term', 'DESCRIPTION')
              .withColumn('COVID_STATUS', f.lit('confirmed'))
              .withColumn('CODE_TYPE', f.lit('SNOMED'))
              .withColumn('SOURCE', f.lit('gdppr'))
              .select('PERSON_ID', 'DATE', 'COVID_PHENOTYPE', 'COVID_STATUS',
                     'DESCRIPTION', 'CODE_TYPE', 'CODE', 'SOURCE')
             )

# COMMAND ----------

# MAGIC %md ## 3.2 HES APC

# COMMAND ----------

# ==========================================
#              HES APC POSITIVE
# ==========================================
_hes_apc_pos = (_hes_apc
                    .where(f.col('DIAG_4_CONCAT').rlike('U07(1|2)'))
                    .withColumn('COVID_PHENOTYPE', f.lit('02_Covid_admission'))
                    .withColumn('CODE',
                      f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'U071')
                      .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'U072')
                    )
                    .withColumn('DESCRIPTION',
                      f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'Confirmed_COVID19')
                      .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'Suspected_COVID19')
                    )
                    .withColumn('COVID_STATUS',
                      f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'confirmed')
                      .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'suspected')
                    )
                    .withColumn('CODE_TYPE', f.lit('ICD10'))
                    .withColumn('SOURCE', f.lit('hes_apc'))
                    .select('PERSON_ID', 'DATE', 'COVID_PHENOTYPE', 'COVID_STATUS',
                     'DESCRIPTION', 'CODE_TYPE', 'CODE', 'SOURCE')
                   )

# COMMAND ----------

# MAGIC %md ## 3.3 SUS

# COMMAND ----------

# ==========================================
#              SUS POSITIVE
# ==========================================
_sus_pos = (_sus
            .where(f.col('DIAG_CONCAT').rlike('U07(1|2)'))\
            .withColumn('COVID_PHENOTYPE', f.lit('02_Covid_admission'))
            .withColumn('CODE',
                        f.when(f.col('DIAG_CONCAT').rlike('U071'), 'U071')
                        .when(f.col('DIAG_CONCAT').rlike('U072'), 'U072')
                       )
            .withColumn('DESCRIPTION',
                        f.when(f.col('DIAG_CONCAT').rlike('U071'), 'Confirmed_COVID19')
                        .when(f.col('DIAG_CONCAT').rlike('U072'), 'Suspected_COVID19')
                       )
            .withColumn('COVID_STATUS',
                        f.when(f.col('DIAG_CONCAT').rlike('U071'), 'confirmed')
                        .when(f.col('DIAG_CONCAT').rlike('U072'), 'suspected')
                       )
            .withColumn('CODE_TYPE', f.lit('ICD10'))
            .withColumn('SOURCE', f.lit('sus'))
            .select('PERSON_ID', 'DATE', 'COVID_PHENOTYPE', 'COVID_STATUS',
                     'DESCRIPTION', 'CODE_TYPE', 'CODE', 'SOURCE')
           )

# COMMAND ----------

# MAGIC %md ## 3.4 CHESS 

# COMMAND ----------

# ==========================================
#              CHESS POSITIVE
# ==========================================
_chess_pos = (_chess
              .select(['PERSON_ID', 'DATE'])
              .withColumn('COVID_PHENOTYPE', f.lit('02_Covid_admission'))
              .withColumn('CODE', f.lit(''))
              .withColumn('DESCRIPTION', f.lit('HospitalAdmissionDate IS NOT null'))
              .withColumn('COVID_STATUS', f.lit('confirmed'))
              .withColumn('CODE_TYPE', f.lit('no_code_for_chess'))
              .withColumn('SOURCE', f.lit('chess'))
             )

# COMMAND ----------

# MAGIC %md ## 3.5 Death

# COMMAND ----------

# ==========================================
#              DEATH POSITIVE
# ==========================================
_deaths_pos = (_deaths
               .where(f.col('S_COD_CODE_CONCAT').rlike('U07(1|2)'))
               .withColumn('COVID_PHENOTYPE', f.lit('03_Fatal_with_covid_diagnosis'))
               .withColumn('CODE',
                           f.when(f.col('S_COD_CODE_CONCAT').rlike('U071'), 'U071')
                           .when(f.col('S_COD_CODE_CONCAT').rlike('U072'), 'U072')
                          )
               .withColumn('DESCRIPTION',
                           f.when(f.col('S_COD_CODE_CONCAT').rlike('U071'), 'Confirmed_COVID19_death')
                           .when(f.col('S_COD_CODE_CONCAT').rlike('U072'), 'Suspected_COVID19_death')
                          )
               .withColumn('COVID_STATUS',
                           f.when(f.col('S_COD_CODE_CONCAT').rlike('U071'), 'confirmed')
                           .when(f.col('S_COD_CODE_CONCAT').rlike('U072'), 'suspected')
                          )
               .withColumn('CODE_TYPE', f.lit('ICD10'))
               .withColumn('SOURCE', f.lit('deaths'))
               .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')
               .select('PERSON_ID', 'DATE', 'COVID_PHENOTYPE', 'COVID_STATUS',
                     'DESCRIPTION', 'CODE_TYPE', 'CODE', 'SOURCE')
                   )

# COMMAND ----------

# MAGIC %md # 4 Combine

# COMMAND ----------

covid_pos_data = (
  _sgss_pos
  .unionByName(_gdppr_pos)
  .unionByName(_sus_pos)
  .unionByName(_deaths_pos)
  .unionByName(_hes_apc_pos)
  .unionByName(_chess_pos)
  .orderBy(f.desc('DATE'))
  )


# COMMAND ----------

# MAGIC %md ## 4.0 Save

# COMMAND ----------

if save_data:
  save_dataset('_tmp_covid_pos', covid_pos_data)

covid_pos_data = read_dataset('_tmp_covid_pos')

# COMMAND ----------

# MAGIC %md ## 4.1 Check

# COMMAND ----------

count_var(covid_pos_data, 'PERSON_ID')
tmpt = tab(covid_pos_data, 'COVID_PHENOTYPE', 'SOURCE', var2_unstyled=1); print()
for var in [
  'COVID_PHENOTYPE',
  'COVID_STATUS',
  'CODE_TYPE',
  'SOURCE'
]:
  covid_pos_data.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# +-------+--------+
# | SOURCE|  
# +-------+--------+
# |    sus|
# |   sgss|
# |hes_apc| 
# |  gdppr|
# | deaths|  
# |  chess|  
# +-------+--------+


# COMMAND ----------

# MAGIC %md # 5 COVID Index date

# COMMAND ----------

# Keep only confirmed COVID-19
covid_pos_data = (covid_pos_data
                 .filter(f.col("COVID_STATUS")=='confirmed')
                )
window_spec = Window.partitionBy(covid_pos_data["PERSON_ID"]).orderBy(f.col("DATE").asc_nulls_last())
covid_pos_data = (covid_pos_data
             .withColumn("rank_col", f.row_number().over(window_spec))
             .filter(f.col("rank_col")==1)
             .drop("rank_col")
             )
#display(covid_pos_data)

# COMMAND ----------

# MAGIC %md ## 5.0 Save

# COMMAND ----------

if save_data:
  save_dataset('_cur_covid_pos', covid_pos_data)

covid_pos_data = read_dataset('_cur_covid_pos')

# COMMAND ----------

count_var(covid_pos_data, 'PERSON_ID')
tab(covid_pos_data, 'COVID_PHENOTYPE', 'SOURCE', var2_unstyled=1); print()
for var in [
  'COVID_STATUS',
  'CODE_TYPE',
  'SOURCE'
]:
  covid_pos_data.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()