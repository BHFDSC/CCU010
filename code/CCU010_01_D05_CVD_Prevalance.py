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

start_date = '1900-01-01'
end_date = '2022-10-31' 

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

cvd_names = ['AAA' ,'AF','AMI','Angina_ST','Angina_UNST','CHD','Cardiac_Arrest','DVT_DVT','HF', 'IH','PAD', 'PE','Stroke_IS','Stroke_NOS','Stroke_SAH','Stroke_TIA']
codelist = codelist.filter(f.col('name').isin(cvd_names)).distinct()
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
#display(codelist_hes)

# COMMAND ----------

# MAGIC %md # 4 Prepare datasets

# COMMAND ----------

# MAGIC %md ## 4.1 Skinny table

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_skinny}""")
_patient = (spark.table(path_skinny)
            .withColumn('CENSOR_DATE_START', f.date_add(f.col('DOB'), -0))
            .withColumn('CENSOR_DATE_END',
                      f.when(
                        (f.col('DOD').isNotNull()),
                        f.col('DOD'))
                      .otherwise(f.to_date(f.lit('2022-10-31')))
                     )
           )
_patient = _patient.select([f.col(x).alias(x.lower()) for x in _patient.columns])  
_patient = _patient.select(['person_id','censor_date_start','censor_date_end'])  
display(_patient)

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

save_dataset('_tmp_gdppr_cvd_snomed_codes', _gdppr_codes)
_gdppr_codes = read_dataset('_tmp_gdppr_cvd_snomed_codes')


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
# display(_gdppr_codes)

# COMMAND ----------

# MAGIC %md ## 4.2 HES APC

# COMMAND ----------

codelist_hes = codelist_hes.select(['code', 'cond'])
codelist_hes_dict = {row['code']:row['cond'] for row in codelist_hes.collect()}
#print(codelist_hes_dict)

codelist_hes_dict_keys = list(codelist_hes_dict.keys())
codelist_hes_dict_keys = '|'.join(codelist_hes_dict_keys)
#print(codelist_hes_dict_keys)

def get_code_list(codelist): 
  code_list = (codelist.select(f.col("code")).toPandas()["code"])
  code_list = list(map(lambda x: str(x), code_list))
  return code_list

codelist_hes_list = get_code_list(codelist_hes)
# print(codelist_hes_list)

# COMMAND ----------

# ==========================================
#              HES APC
# ==========================================
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
save_dataset('_tmp_hes_cvd_codes', _hes_apc_codes)
_hes_apc_codes = read_dataset('_tmp_hes_cvd_codes')
count_var(_hes_apc_codes, 'PERSON_ID')

# COMMAND ----------

_hes_apc_codes = (_hes_apc_codes
                  .withColumn('cond', f.regexp_extract('code', codelist_hes_dict_keys, 0))
                  .replace(codelist_hes_dict, subset=['cond'])
                  .select(['PERSON_ID', 'code', 'date', 'cond'])
                  .withColumnRenamed('PERSON_ID','person_id')
                  .distinct()
                  )
  
_hes_apc_codes = (_patient
                 .join(_hes_apc_codes, ['person_id'], 'left')
                 .where((f.col('date') >= f.col('censor_date_start')) 
                        & (f.col('date') <= f.col('censor_date_end')))
                  .orderBy(['person_id', 'date', 'cond'])
                  .select(['person_id', 'censor_date_start', 'censor_date_end', 'date', 'code', 'cond'])
            )

# COMMAND ----------

# MAGIC %md # 3 Combine

# COMMAND ----------

# 1.	Stable angina (Angina_ST), 
# 2.	Unstable angina (Angina_UNST), 
# 3.	Myocardial infarction (AMI),  
# 4.	Coronary heart disease unspecified (CHD), 
# 5.	Heart failure (HF), 
# 6.	Cardiac arrest, 
# 7.	Transient ischaemic attack (Stroke_TIA),
# 8.	Ischaemic stroke (Stroke_IS), 
# 9.	Stroke unspecified (Stroke_NOS),
# 10.	Intracerebral  haemorrhage (IH),
# 11.	Subarachnoid haemorrhage (Stroke_SAH),
# 12.	Peripheral arterial disease (PAD), 
# 13.	Atrial fibrillation (AF), 
# 14.	Abdominal aortic aneurysm (AAA), 
# 15.	Deep vein thrombosis (DVT) 
# 16.	Pulmonary embolism (PE). 

# COMMAND ----------

_patient_cvd = (
  _hes_apc_codes
  .unionByName(_gdppr_codes)
  .orderBy(['person_id', 'date', 'cond'])
  )
  
_win = Window.partitionBy(['person_id', 'cond']).orderBy('date')  
_patient_cvd = (_patient_cvd
    .withColumn('_rownum', f.row_number().over(_win))
    .where(f.col('_rownum') == 1)
    .select('person_id', 'censor_date_start', 'censor_date_end', 'date', 'code', 'cond')
    .orderBy('person_id', 'date', 'cond')
               )

_name_prefix='cond_'
_patient_cvd = (_patient_cvd
                       .withColumn('cond', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('cond'))))
    .groupBy('person_id', 'censor_date_start', 'censor_date_end')
    .pivot('cond')
    .agg(f.first('date'))
    .where(f.col('person_id').isNotNull())
    .orderBy('person_id') 
                      )
save_dataset('_tmp_cvd_cohort', _patient_cvd)
_patient_cvd = read_dataset('_tmp_cvd_cohort').drop('censor_date_start', 'censor_date_end')
count_var(_patient_cvd, 'person_id')

# COMMAND ----------

display(_patient_cvd)

# COMMAND ----------

cvd_names = [x.lower() for x in cvd_names]
cols = ['cond_' + col for col in cvd_names]
colsdate = ['cond_date_' + col for col in cvd_names]
for var in cvd_names:
  _patient_cvd = (_patient_cvd
                 .withColumnRenamed(f'cond_{var}', f'cond_date_{var}')
                 .withColumn(f'cond_{var}',
                             f.when((f.col(f'cond_date_{var}').isNotNull()),f.lit(1)).otherwise(f.lit(None)))
                )
             
  
_patient_cvd = (_patient_cvd
               .withColumn("cond_cvd",f.greatest(*cols))
               .withColumn("cond_date_cvd",f.greatest(*colsdate))
              )

# COMMAND ----------

display(_patient_cvd)

# COMMAND ----------

save_dataset('_cur_cvd_cohort', _patient_cvd)
_patient_cvd = read_dataset('_cur_cvd_cohort')
count_var(_patient_cvd, 'person_id')

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