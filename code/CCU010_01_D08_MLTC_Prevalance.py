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
from pyspark.sql.window import Window
from pyspark.sql import Row
from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime
import sys

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.sql('CLEAR CACHE')

# COMMAND ----------

# MAGIC %run "Helper_functions/skinny"

# COMMAND ----------

# MAGIC %run "Helper_functions/functions"

# COMMAND ----------

# MAGIC %run "Helper_functions/basic_functions"

# COMMAND ----------

# MAGIC %md # 2 Data

# COMMAND ----------

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

path_skinny        = f'{dbc}.{proj}_final_skinny_assembled'
path_cohort        = f'{dbc}.{proj}_final_cohort_1'

start_date = '1900-01-01'
end_date = '2022-12-31' 
save_data = True

# COMMAND ----------

def drop_table(table_name:str, database_name:str='', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

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
  proj = 'ccu010_01'
  db = ''
  dbc = f'{db}_collab'
  spark.sql('CLEAR CACHE')
  path_tmp_covid = f'{dbc}.{proj}{name}'
  spark.sql(F"""REFRESH TABLE {path_tmp_covid}""")
  dataset = spark.table(path_tmp_covid)
  return dataset

# COMMAND ----------

hes_apc         = spark.table(path_hes_apc)
gdppr           = spark.table(path_gdppr)

# COMMAND ----------

# MAGIC %md # 3 Codelist

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_codelist}""")
codelist = spark.table(path_codelist)
mltc_names = ['Hypertension' ,'Diabetes','Cancer','Asthma','COPD','CKD','Liver_Disease',
              'Dementia','Depression', 'SCHI_BPD']
codelist = codelist.filter(f.col('name').isin(mltc_names)).distinct()
tab(codelist, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

codelist_gdppr = (codelist
                  .filter(f.col('terminology')=='SNOMED') 
                  .withColumnRenamed('code', 'code')
                  .withColumnRenamed('name', 'cond')
                  .select(['cond', 'code'])
                 )
tab(codelist_gdppr, 'cond', var2_unstyled=1); print()

codelist_hes = (codelist
                  .filter(f.col('terminology')=='ICD10') 
                  .withColumnRenamed('code', 'code')
                  .withColumnRenamed('name', 'cond')
                  .select(['cond', 'code'])
                 )
tab(codelist_hes, 'cond', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4 Prepare datasets

# COMMAND ----------

# MAGIC %md ## 4.0 Load Skinny Table

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_cohort}""")
_patient = spark.table(path_cohort)
_patient = _patient.select(['person_id', 'date_covid', 'censor_date_start', 'censor_date_end'])
count_var(_patient, 'person_id')
#display(_patient)

# COMMAND ----------

# MAGIC %md ## 4.1 GDPPR

# COMMAND ----------

# ==========================================
#              GDPPR
# ==========================================
_gdppr_codes = (gdppr
                .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])
                .withColumnRenamed('NHS_NUMBER_DEID', 'person_id')
                .withColumnRenamed('CODE', 'code')
                .withColumnRenamed('DATE', 'date')
                .where(f.col('person_id').isNotNull() & f.col('date').isNotNull())
                .distinct()
                .orderBy(['person_id', 'date'], ascending = True)
                .join(codelist_gdppr, ['code'], 'leftsemi')
                .select(['person_id', 'date', 'code'])
               )

save_dataset('_tmp_gdppr_mltc_snomed_codes', _gdppr_codes)
_gdppr_codes = read_dataset('_tmp_gdppr_mltc_snomed_codes')

_gdppr_codes = (_patient
                 .join(_gdppr_codes, ['person_id'], 'left')
                 .where((f.col('date') >= f.col('censor_date_start')) 
                        & (f.col('date') <= f.col('censor_date_end')))
               )
_gdppr_codes = (_gdppr_codes
                .select(['person_id', 'censor_date_start', 'censor_date_end', 'date', 'code'])
                .join(codelist_gdppr.select(['code', 'cond']), ['code'], 'left')
                .select(['person_id', 'censor_date_start', 'censor_date_end', 'date', 'code', 'cond'])
               )
count_var(_gdppr_codes, 'person_id')

# COMMAND ----------

# MAGIC %md ## 4.2 HES APC

# COMMAND ----------

codelist_hes = codelist_hes.select(['code', 'cond'])
codelist_hes_dict = {row['code']:row['cond'] for row in codelist_hes.collect()}

codelist_hes_dict_keys = list(codelist_hes_dict.keys())
codelist_hes_dict_keys = '|'.join(codelist_hes_dict_keys)

def get_code_list(codelist): 
  code_list = (codelist.select(f.col("code")).toPandas()["code"])
  code_list = list(map(lambda x: str(x), code_list))
  return code_list

codelist_hes_list = get_code_list(codelist_hes)

# COMMAND ----------

_hes_apc = (hes_apc
            .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
            .select(['PERSON_ID', 'EPIKEY', 'EPISTART'] 
                    + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
            .orderBy('PERSON_ID', 'EPIKEY')  
           )
  
_hes_apc_codes = (reshape_wide_to_long_multi(_hes_apc,
                                           i=['PERSON_ID', 'EPIKEY', 'EPISTART'],
                                           j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
               )
_hes_apc_codes = (reshape_wide_to_long_multi(_hes_apc_codes,
                                           i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'],
                                           j='DIAG_DIGITS', stubnames=['DIAG_'])
                .withColumnRenamed('POSITION', 'DIAG_POSITION')
                .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))
                .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))
                .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))
                .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))
                .withColumnRenamed('DIAG_', 'CODE')
                .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))
                .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])
                .filter(f.col('DIAG_DIGITS')==4)
                .withColumnRenamed('EPISTART', 'date')
                .withColumnRenamed('CODE', 'code')
                .select(['PERSON_ID', 'date', 'code'])
                .distinct()
               )
_hes_apc_codes = (_hes_apc_codes
                  .where(f.col('code').rlike(codelist_hes_dict_keys))
)
save_dataset('_tmp_hes_mltc_codes', _hes_apc_codes)
_hes_apc_codes = read_dataset('_tmp_hes_mltc_codes')
count_var(_hes_apc_codes, 'PERSON_ID')

# COMMAND ----------

_hes_apc_codes = (_hes_apc_codes
                  .withColumn('cond', f.regexp_extract('code', codelist_hes_dict_keys, 0))
                  .replace(codelist_hes_dict, subset=['cond'])
                  .select(['PERSON_ID', 'code', 'date', 'cond'])
                  .withColumnRenamed('PERSON_ID','person_id')
                  .distinct()
                  )
count_var(_hes_apc_codes, 'person_id')
  
_hes_apc_codes = (_patient
                 .join(_hes_apc_codes, ['person_id'], 'left')
                 .where((f.col('date') >= f.col('censor_date_start')) 
                        & (f.col('date') <= f.col('censor_date_end')))
                  .orderBy(['person_id', 'date', 'cond'])
                  .select(['person_id', 'censor_date_start', 'censor_date_end', 'date', 'code', 'cond'])
            )
count_var(_hes_apc_codes, 'person_id')

# COMMAND ----------

# MAGIC %md # 5 Find MLTC Codes

# COMMAND ----------

# MAGIC %md ## 5.1 Combine

# COMMAND ----------

_patient_mltc = (
  _hes_apc_codes
  .unionByName(_gdppr_codes)
  .orderBy(['person_id', 'date', 'cond'])
  ) 
  
_win = Window.partitionBy(['person_id', 'cond']).orderBy('date')  
_patient_mltc = (_patient_mltc
    .withColumn('_rownum', f.row_number().over(_win))
    .where(f.col('_rownum') == 1)
    .select('person_id', 'censor_date_start', 'censor_date_end', 'date', 'code', 'cond')
    .orderBy('person_id', 'date', 'cond')
               )

_name_prefix='cond_'
_patient_mltc = (_patient_mltc
                       .withColumn('cond', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('cond'))))
    .groupBy('person_id', 'censor_date_start', 'censor_date_end')
    .pivot('cond')
    .agg(f.first('date'))
    .where(f.col('person_id').isNotNull())
    .orderBy('person_id') 
                      )
save_dataset('_tmp_mltc_cohort', _patient_mltc)
_patient_mltc = read_dataset('_tmp_mltc_cohort').drop('censor_date_start', 'censor_date_end')

# COMMAND ----------

display(_patient_mltc)

# COMMAND ----------

## Prevalent condition is considered only when a condition is first recorded 30days before index COVID date
_patient = spark.table(path_cohort)
_patient = _patient.select(['person_id', 'date_covid'])

_patient_mltc = read_dataset('_tmp_mltc_cohort').drop('censor_date_start', 'censor_date_end')
_patient_mltc = _patient.join(_patient_mltc, on=['person_id'], how='left')
count_var(_patient_mltc, 'person_id')

mltc_names = [x.lower() for x in mltc_names]
print(mltc_names)
for var in mltc_names:
  _patient_mltc = (_patient_mltc
              .withColumnRenamed(f'cond_{var}', f'cond_date_{var}')
              .withColumn(f'cond_date_{var}',
              f.when(f.datediff(f.col(f'cond_date_{var}'),f.col("date_covid")) <=-30,
                     f.col(f'cond_date_{var}')).otherwise(f.lit(None)))
             )
  _patient_mltc = (_patient_mltc
              .withColumn(f'cond_{var}',
                          f.when(f.col(f'cond_date_{var}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
             )
_patient_mltc = _patient_mltc.drop('date_covid')

# COMMAND ----------

display(_patient_mltc)

# COMMAND ----------

cols = ['cond_' + col for col in mltc_names]
colsdate = ['cond_date_' + col for col in mltc_names]
_patient_mltc = (_patient_mltc
            .withColumn("cond_ltc", sum(_patient_mltc[col] for col in cols))
            .withColumn("cond_ltc", f.col("cond_ltc") + 1)
            .withColumn("cond_mltc", f.when(f.col("cond_ltc")==1, f.lit(0)).otherwise(f.lit(1)))
              )
splits = [('1', 1), ('2', 2), ('>=3', float('Inf'))]
bins = reduce(lambda c, i: c.when(f.col('cond_ltc') <= i[1], i[0]), splits,
              f.when(f.col('cond_ltc') < splits[0][0], None)).otherwise(splits[-1][0]).alias('cond_ltc_cat')
_patient_mltc = _patient_mltc.select(*_patient_mltc.columns, bins)

# COMMAND ----------

display(_patient_mltc)

# COMMAND ----------

# MAGIC %md ## 5.2 Save

# COMMAND ----------

save_dataset('_cur_mltc_cohort', _patient_mltc)
_patient_mltc = read_dataset('_cur_mltc_cohort')

# COMMAND ----------

def print_mm_summary(df):
  a=[];b=[];c=[];d=[]
  selected = [s for s in df.columns if 'cond_date' in s]
  #selected.remove('cond_total')
  #selected.remove('cond_concat')
  for var in selected:
    a.append(var)
    b.append(df.select(f.col(var)).filter(df[var].isNotNull()).count())
    c.append(df.agg(f.min(var)).collect()[0][0])
    d.append(df.agg(f.max(var)).collect()[0][0])

  mm_summary = pd.DataFrame({
    'mm': a,
    'count': b,
    'minimum_date': c,
    'maximum_date': d
      })

  print(mm_summary)