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
from pyspark.sql.functions import isnan, when, count, col
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

_datetimenow = datetime.datetime.now() 
print(f"_datetimenow:  {_datetimenow}")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.sql('CLEAR CACHE')
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 10)
pd.set_option('expand_frame_repr', False)

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

path_covid         = f'{dbc}.{proj}_cur_covid_pos'
path_cvd           = f'{dbc}.{proj}_cur_cvd_cohort'
path_skinny        = f'{dbc}.{proj}_final_skinny_assembled'

#Date parameters
index_date = '2020-01-31'

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

spark.sql(F"""REFRESH TABLE {path_covid}""")
spark.sql(F"""REFRESH TABLE {path_cvd}""")
spark.sql(F"""REFRESH TABLE {path_skinny}""")

# COMMAND ----------

covid_data         = spark.table(path_covid)
cvd_data           = spark.table(path_cvd)
skinny_data        = spark.table(path_skinny)
skinny_data = skinny_data.select([f.col(x).alias(x.lower()) for x in skinny_data.columns])  

# COMMAND ----------

# MAGIC %md ## 2.1 Check COVID Dataset

# COMMAND ----------

count_var(covid_data, 'PERSON_ID')
tab(covid_data, 'COVID_PHENOTYPE', 'SOURCE', var2_unstyled=1)
for var in [
  'COVID_STATUS',
  'CODE_TYPE',
  'SOURCE'
]:
  covid_data.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

covid_data = (covid_data
              .select(['PERSON_ID', 'DATE', 'SOURCE'])
              .withColumnRenamed('DATE', 'cond_date_COVID')
              .withColumnRenamed('SOURCE', 'cond_source_COVID')
              .withColumn('cond_COVID', f.lit(1))
         )
covid_data = covid_data.select([f.col(x).alias(x.lower()) for x in covid_data.columns])  

# COMMAND ----------

# MAGIC %md ## 2.2 Check CVD Dataset

# COMMAND ----------

cvd_data.columns

# COMMAND ----------

cvd_data = (cvd_data
          .select(['person_id',
 'cond_date_aaa',
 'cond_date_af',
 'cond_date_ami',
 'cond_date_angina_st',
 'cond_date_angina_unst',
 'cond_date_cardiac_arrest',
 'cond_date_chd',
 'cond_date_dvt_dvt',
 'cond_date_hf',
 'cond_date_ih',
 'cond_date_pad',
 'cond_date_pe',
 'cond_date_stroke_is',
 'cond_date_stroke_nos',
 'cond_date_stroke_sah',
 'cond_date_stroke_tia',
 'cond_aaa',
 'cond_af',
 'cond_ami',
 'cond_angina_st',
 'cond_angina_unst',
 'cond_chd',
 'cond_cardiac_arrest',
 'cond_dvt_dvt',
 'cond_hf',
 'cond_ih',
 'cond_pad',
 'cond_pe',
 'cond_stroke_is',
 'cond_stroke_nos',
 'cond_stroke_sah',
 'cond_stroke_tia',
 'cond_cvd',
 'cond_date_cvd'])
           )

# cvd_names = ['AAA' ,'AF','AMI','Angina_ST','Angina_UNST','CHD','Cardiac_Arrest','DVT_DVT','HF', 'IH','PAD', #'PE',
#              'Stroke_IS','Stroke_NOS','Stroke_SAH','Stroke_TIA']
# cols = ['cond_' + col for col in cvd_names]
# colsdate = ['cond_date_' + col for col in cvd_names]
# cvd_data = (cvd_data
#             .drop('cond_PE','cond_date_PE')
#                .withColumn("cond_CVD",f.greatest(*cols))
#                .withColumn("cond_date_CVD",f.greatest(*colsdate))
#               )

tab(cvd_data, 'cond_cvd')

# COMMAND ----------

# MAGIC %md # 3 Apply exclusion to skinny data

# COMMAND ----------

# MAGIC %md ## 3.1 GDPPR+Alive patients

# COMMAND ----------

schema = t.StructType([t.StructField('Step', t.StringType(), True),
                           t.StructField('Patients', t.IntegerType(), True)]
                       )
flow_chart = spark.createDataFrame([], schema)
flow_chart = flow_chart.union(spark.createDataFrame([('Total patients at the start', skinny_data.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

# COMMAND ----------

skinny_data.select(f.col('in_gdppr')).groupBy('in_gdppr').count().sort(f.col('in_gdppr').desc()).show()
cohort = (skinny_data
               .filter((f.col("in_gdppr")==1))
              ) 


flow_chart = flow_chart.union(spark.createDataFrame([('Total patients in GDPPR', cohort.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

# COMMAND ----------

cohort = (cohort
          .withColumn('age_at_start', f.datediff(f.to_date(f.lit(index_date)), f.col('dob'))/365.25)
          .withColumn("age_at_start", f.round(f.col("age_at_start"), 0))
          .filter((f.col("age_at_start")>=18) & (f.col("age_at_start")<=120))
          .where((f.col('dod').isNull()) | (f.col('dod') > f.to_date(f.lit(index_date))))
    )
flow_chart = flow_chart.union(spark.createDataFrame([('Total patients alive and >18 years in GDPPR', cohort.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

# COMMAND ----------

# MAGIC %md ## 3.2 COVID patients

# COMMAND ----------

cohort = (cohort
          .join(covid_data, on='person_id', how='left')
          .filter(f.col("cond_covid")==1)
         )
count_var(cohort, 'person_id')

flow_chart = flow_chart.union(spark.createDataFrame([('Total patients with positive COVID test', cohort.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

# COMMAND ----------

# MAGIC %md ## 3.2 CVD patients

# COMMAND ----------

cohort = (cohort
          .join(cvd_data, on='person_id', how='left')
          .filter(f.col("cond_cvd")==1)
          .withColumn("time_cvd_covid", f.datediff(f.col("cond_date_cvd"),f.col("cond_date_covid")))
         )
count_var(cohort, 'person_id')
cohort.describe(['time_cvd_covid']).show()

# COMMAND ----------

cohort = (cohort
          .filter(f.col("time_cvd_covid")<=-30)
    )
count_var(cohort, 'person_id')
cohort.describe(['time_cvd_covid']).show()

flow_chart = flow_chart.union(spark.createDataFrame([('Total patients with prevalent CVD', cohort.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

# COMMAND ----------

# MAGIC %md ## 3.4 Remove missing data

# COMMAND ----------

for var in [
  'sex',
  'ethnic_cat',
  'imd_2019_quintiles',
  'age_at_start',
]:
  cohort.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

display(cohort
        .filter(
          (f.col("cond_date_cvd") > f.col("dod")) 
          | (f.col("cond_date_covid") > f.col("dod"))
               )
        .select(['person_id', "dod","cond_date_covid", "cond_date_cvd"])
    )

# COMMAND ----------

cohort = (cohort
          .filter(f.col("ethnic_cat") != 'Unknown')
          .filter(f.col("imd_2019_quintiles").isNotNull())
          .withColumn('cond_date_covid',
                      f.when(((f.col("dod").isNotNull()) & (f.col("cond_date_covid") > f.col("dod"))), 
                            f.lit(None)).otherwise(f.col('cond_date_covid')))
          .withColumn('cond_date_cvd',
                      f.when(((f.col("dod").isNotNull()) & (f.col("cond_date_cvd") > f.col("dod"))), 
                            f.lit(None)).otherwise(f.col('cond_date_cvd')))
          .filter((f.col("cond_date_covid").isNotNull()) & (f.col("cond_date_cvd").isNotNull()))
          )
count_var(cohort, 'person_id')

flow_chart = flow_chart.union(spark.createDataFrame([('Total patients with no missing data',cohort.select('person_id').filter(f.col('person_id').isNotNull()).distinct().count())], schema))
flow_chart.show()

null_counts = {col:cohort.filter(cohort[col].isNull()).count() for col in cohort.columns}
null_counts

# COMMAND ----------

display(cohort
        .filter((f.col("cond_date_cvd") > f.col("dod")) | (f.col("cond_date_covid") > f.col("dod")))
        .select(['person_id', "dod","cond_date_covid", "cond_date_cvd"])
    )

# COMMAND ----------

for var in ['sex','ethnic_cat','imd_2019_quintiles', 'in_gdppr','cond_COVID','cond_CVD']:
  cohort.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

flow_chart2 = flow_chart.withColumn('Context', f.lit(''))
schema = flow_chart2.schema
pandas_df = flow_chart2.toPandas()
pandas_df.loc[0, 'Context'] = 'This is table describing the flow chart for the cohort of the project CCU010. The first column describes each step of the flow chart and the second column represents the total number of patients remaining at each step'
flow_chart2 = spark.createDataFrame(pandas_df,schema=schema)
#del pandas_df
display(flow_chart2)
def custom_round(x, base=5):
    return int(base * round(float(x)/base))
pandas_df['Patients'] = pandas_df['Patients'].apply(lambda x: custom_round(x, base=5))
pandas_df
pandas_df['Patients'] = pandas_df['Patients'].astype(int)

flow_chart2 = spark.createDataFrame(pandas_df,schema=schema)
#del pandas_df
display(flow_chart2)

# COMMAND ----------

cohort = (cohort.select([
 'person_id',
 'dob',
 'sex',
 'dod',
 #'lsoa',
 #'ethnic',
 #'ethnic_desc',
 'ethnic_cat',
 #'region',
 #'imd_2019_deciles',
 'imd_2019_quintiles',
 #'in_gdppr',
 'age_at_start',
 'cond_date_covid',
 'cond_source_covid',
 'cond_date_cvd',
 'time_cvd_covid',
  #'cond_covid',
 'cond_date_aaa',
 'cond_date_af',
 'cond_date_ami',
 'cond_date_angina_st',
 'cond_date_angina_unst',
 'cond_date_cardiac_arrest',
 'cond_date_chd',
 'cond_date_dvt_dvt',
 'cond_date_hf',
 'cond_date_ih',
 'cond_date_pad',
 'cond_date_pe',
 'cond_date_stroke_is',
 'cond_date_stroke_nos',
 'cond_date_stroke_sah',
 'cond_date_stroke_tia',
 'cond_aaa',
 'cond_af',
 'cond_ami',
 'cond_angina_st',
 'cond_angina_unst',
 'cond_chd',
 'cond_cardiac_arrest',
 'cond_dvt_dvt',
 'cond_hf',
 'cond_ih',
 'cond_pad',
 'cond_pe',
 'cond_stroke_is',
 'cond_stroke_nos',
 'cond_stroke_sah',
 'cond_stroke_tia',
 #'cond_cvd',
])
          .withColumnRenamed('imd_2019_quintiles', 'imd')
          .withColumnRenamed('cond_date_covid', 'date_covid')
          .withColumnRenamed('cond_source_COVID', 'source_covid')
          .withColumnRenamed('cond_date_cvd', 'date_cvd')
          .withColumnRenamed('ethnic_cat', 'ethnicity')
          .withColumn('censor_date_start', f.date_add(f.col('dob'), -0))
          .withColumn('censor_date_end',
                      f.when(
                        (f.col('dod').isNotNull()),
                        f.col('dod'))
                      .otherwise(f.to_date(f.lit('2022-10-31')))
                     )
         )
cohort = cohort.select([f.col(x).alias(x.lower()) for x in cohort.columns])

# COMMAND ----------

# MAGIC %md ##3.5 Save

# COMMAND ----------

save_dataset('_final_cohort_1', cohort)
cohort = read_dataset('_final_cohort_1')
count_var(cohort, 'person_id')

# COMMAND ----------

display(cohort)

# COMMAND ----------

null_counts = {col:cohort.filter(cohort[col].isNull()).count() for col in cohort.columns}
null_counts

# COMMAND ----------

for var in ['sex','ethnicity','imd']:
  cohort.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()