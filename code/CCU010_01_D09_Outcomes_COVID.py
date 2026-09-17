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
path_skinny        = f'{dbc}.{proj}_final_skinny_assembled'
path_cohort        = f'{dbc}.{proj}_final_cohort_1'

start_date = '2020-01-31'
end_date = '2022-10-31' 

detail_run = False
save_data = False

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

# MAGIC %md # 1 Data

# COMMAND ----------

codelist = spark.table(path_codelist)  
cohort   = spark.table(path_cohort)  

gdppr    = spark.table(path_gdppr)
hes_apc  = spark.table(path_hes_apc)
hes_cc   = spark.table(path_hes_cc)
#sus      = spark.table(path_sus)
#sgss     = spark.table(path_sgss)
chess    = spark.table(path_chess)
deaths   = spark.table(path_deaths)

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
           .distinct() 
          )
_deaths = (_deaths
           .withColumn('S_COD_CODE_CONCAT',
                       f.concat_ws(',', *[col for col in _deaths.columns if re.match('.*_COD_.*', col)]))
          )
_deaths = _deaths.select([f.col(x).alias(x.lower()) for x in _deaths.columns])
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID')

count_var(_deaths, 'person_id')
_deathsx = _deaths.filter(f.col('reg_date_of_death') < '2019-01-01')
count_var(_deathsx, 'person_id')

# COMMAND ----------

df = (_deathsx
      .withColumn('date_ym', f.date_format(f.col('reg_date_of_death'), 'yyyy-MM'))
      .groupBy('date_ym')
      .agg(
       f.count(f.lit(1)).alias('n'),
       f.countDistinct('person_id').alias('distinct_id'),
    )
      .orderBy('date_ym')
     ).toPandas()

# re-format the date field for plotting
df['date_ym_formatted'] = pd.to_datetime(df['date_ym'], errors='coerce')

sns.set(style="whitegrid", rc={'figure.figsize':(15,6), "xtick.bottom" : True, "ytick.left" : False, 'grid.linestyle':':'})
chosen_palette = 'colorblind'
myFmt = mdates.DateFormatter('%Y-%m')

dc = sns.lineplot(data=df, x='date_ym_formatted', y="n", marker = 'o')#, #palette=chosen_palette, alpha=0.8)
ylabels = ['{:,.0f}'.format(y)  for y in dc.get_yticks()]
dc.set_yticklabels(ylabels)
dc.set_title("Count of death records by time")
dc.set_xlabel("Year-Month")
dc.set_ylabel("Count of records")
display(dc)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 Cohort

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_cohort}""")
_patient = spark.table(path_cohort)
_patient = _patient.select(['person_id', 'censor_date_start', 'censor_date_end'])
display(_patient)

# COMMAND ----------

# MAGIC %md ## 2.2 HES APC

# COMMAND ----------

start_date = '2018-03-01'
end_date = '2021-02-28' 

spark.sql(F"""REFRESH TABLE {path_hes_apc}""")
hes_apc  = (spark.table(path_hes_apc)
            .select(["PERSON_ID_DEID", "DIAG_4_01", "DIAG_4_CONCAT", 'OPERTN_4_CONCAT',
                     "ADMIDATE", "DISDATE", "DISMETH", "EPISTART", "EPIEND",
                     "EPISTAT", "FDE", 'SUSRECID'
                    ])
            .withColumnRenamed('PERSON_ID_DEID', 'person_id') 
            .withColumnRenamed('SUSRECID', 'susrecid') 
            .filter(f.col("person_id").isNotNull())
            .where((f.col('ADMIDATE') >= start_date) & (f.col('ADMIDATE') <= end_date))
           )
hes_apc = hes_apc.select([f.col(x).alias(x.lower()) for x in hes_apc.columns])

# COMMAND ----------

df = (hes_apc
      .withColumn('date_ym', f.date_format(f.col('ADMIDATE'), 'yyyy-MM'))
      .groupBy('date_ym')
      .agg(
       f.count(f.lit(1)).alias('n'),
       f.countDistinct('person_id').alias('distinct_id'),
    )
      .orderBy('date_ym')
     ).toPandas()

# re-format the date field for plotting
df['date_ym_formatted'] = pd.to_datetime(df['date_ym'], errors='coerce')

# set the grid size and style
sns.set(style="whitegrid", rc={'figure.figsize':(15,6), "xtick.bottom" : True, "ytick.left" : False, 'grid.linestyle':':'})

#Palette 
chosen_palette = 'colorblind'

# date format
myFmt = mdates.DateFormatter('%Y-%m')

dc = sns.lineplot(data=df, x='date_ym_formatted', y="n", marker = 'o')#, #palette=chosen_palette, alpha=0.8)
ylabels = ['{:,.0f}'.format(y)  for y in dc.get_yticks()]
dc.set_yticklabels(ylabels)
dc.set_title("Count of hospital admissions by time")
dc.set_xlabel("Year-Month")
dc.set_ylabel("Count of records")
display(dc)

# COMMAND ----------

w_max_date = Window.partitionBy("person_id", "admidate").orderBy(f.col("disdate").desc_nulls_last())
_hes_apc = (hes_apc
            .filter(f.col("disdate").isNotNull())
            .withColumn("max_date_rank", f.row_number().over(w_max_date))
            .withColumn("max_date_flag", f.when(f.col("max_date_rank")==1, f.lit(1)).otherwise(f.lit(0)))
            .filter(f.col("max_date_flag")==1)
            .withColumn("final_discharge_date", f.lit("1800-01-01"))
            .withColumn("final_discharge_date",
                        f.when(( 
                          (f.col("max_date_flag")==1) 
                          & ((f.col("fde")==1)
                             |((f.col("fde")==0) & (f.col("epistat")=="3") 
                               & (f.col("dismeth").isin(["1", "2", "3", "4", "5"]))))), 
                                          f.col("disdate")).otherwise(f.col("final_discharge_date")))
            .withColumn("final_discharge_flag",
                        f.when(f.col("final_discharge_date")=="1800-01-01", f.lit(0)).otherwise(f.lit(1)))
            .filter(f.col("final_discharge_flag")==1)
           )

# COMMAND ----------

_hes_apc = (_hes_apc
            .join(_patient.select('person_id'), ['person_id'], 'leftsemi')
            .join(_patient, ['person_id'], 'left')
            .dropDuplicates()
            .where((f.col('admidate') >= start_date) & (f.col('admidate') <= end_date))
            .where((f.col('admidate') >= f.col('censor_date_start')) 
                   & (f.col('admidate') <= f.col('censor_date_end')))
            .where(f.col('admidate') <= f.col('disdate'))
            .orderBy(['person_id','admidate'])
            .select(['person_id','admidate','disdate','epistart', 'diag_4_01',
                     'diag_4_concat','opertn_4_concat', 'susrecid'])
            .distinct()
           )
save_dataset('_tmp_hes_apc', _hes_apc)
_hes_apc = read_dataset('_tmp_hes_apc')

# COMMAND ----------

# MAGIC %md ## 2.3 HES CC

# COMMAND ----------

# ==========================================
#              HES CC
# ==========================================

_hes_cc_apc = (hes_cc
           .select(['PERSON_ID_DEID','EPISTART','ADMIDATE','DISDATE',
                    'CCSTARTDATE','SUSRECID'])
           .withColumnRenamed('PERSON_ID_DEID', 'person_id')
           .withColumnRenamed('SUSRECID', 'susrecid') 
           .withColumnRenamed('ADMIDATE', 'admidate_cc')
           .withColumnRenamed('DISDATE', 'disdate_cc')
           .withColumnRenamed('EPISTART', 'epistart_cc')
           .withColumnRenamed('CCSTARTDATE', 'startdate_cc')
           .where((f.col('admidate_cc') >= start_date) & (f.col('admidate_cc') <= end_date))
           .join(_patient.select(['person_id']), ['person_id'], 'leftsemi')
           .join(hes_apc.select(['person_id','admidate','disdate','epistart','diag_4_concat',
                                 'diag_4_01','susrecid']),
                 ['person_id', 'susrecid'], 'left')
           .distinct()
           .orderBy(['person_id','admidate_cc'])
           .withColumn('startdate_cc', f.to_date(f.substring('startdate_cc', 0, 8), 'yyyyMMdd'))       
          )

# COMMAND ----------

# MAGIC %md ## 2.4 CHESS

# COMMAND ----------

# ==========================================
#              CHESS
# ==========================================
# chess
_chess = (chess
          .select(['PERSON_ID_DEID', 'Typeofspecimen', 'Covid19', 'AdmittedToICU', 'Highflownasaloxygen', 
                   'NoninvasiveMechanicalventilation', 'Invasivemechanicalventilation', 'RespiratorySupportECMO', 
                   'DateAdmittedICU', 'HospitalAdmissionDate', 'InfectionSwabDate', 'FinalOutcomeDate'])
          .withColumnRenamed('PERSON_ID_DEID', 'person_id')
          .withColumnRenamed('HospitalAdmissionDate', 'admidate')
          .withColumnRenamed('FinalOutcomeDate', 'disdate')
          .where(f.col('Covid19') == 'Yes')
          .where(f.col('person_id').isNotNull())
          .where(
            ((f.col('admidate') >= start_date) | (f.col('admidate').isNull()))
            & ((f.col('admidate') <= end_date) | (f.col('admidate').isNull()))
          )
          .where(
            ((f.col('DateAdmittedICU') >= start_date) | (f.col('DateAdmittedICU').isNull()))
            & ((f.col('DateAdmittedICU') <= end_date) | (f.col('DateAdmittedICU').isNull()))
          )
          .distinct()
          .join(_patient.select(['person_id']), ['person_id'], 'leftsemi')
          .withColumn('admidate',
                         f.when(f.col('admidate') > f.col('disdate'),
                                f.col('InfectionSwabDate')).otherwise(f.col('admidate')))
          .where(f.col('admidate') <= f.col('disdate'))
         )
_chess = _chess.select([f.col(x).alias(x.lower()) for x in _chess.columns])

# COMMAND ----------

# MAGIC %md ## 2.2 Death

# COMMAND ----------

# ==========================================
#              DEATHS
# ==========================================
# from datetime import date, timedelta

# sdate = date(1900,1,1)     # start date
# edate = date(2022,10,31)   # end date
# dates = pd.date_range(sdate,edate-timedelta(days=1),freq='d').strftime('%Y%m%d').tolist()
# _deaths_dates_invalid = (deaths
#                          .select(['REG_DATE_OF_DEATH'])
#                          .distinct()
#                          .filter(~f.col("REG_DATE_OF_DEATH").isin(dates))
#                         )
# _deaths_dates_invalid = [row.REG_DATE_OF_DEATH for row in _deaths_dates_invalid.select("REG_DATE_OF_DEATH").collect()]

# _win = (Window
#       .partitionBy('PERSON_ID')
#       .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))
#            )

# _deaths = (deaths
#            .select(['DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH', 'REG_DATE',
#                     'S_UNDERLYING_COD_ICD10', 'S_COD_CODE_1', 
#                     'S_COD_CODE_2', 'S_COD_CODE_3', 'S_COD_CODE_4', 'S_COD_CODE_5', 'S_COD_CODE_6',
#                     'S_COD_CODE_7', 'S_COD_CODE_8', 'S_COD_CODE_9', 'S_COD_CODE_10', 'S_COD_CODE_11', 
#                     'S_COD_CODE_12', 'S_COD_CODE_13', 'S_COD_CODE_14', 'S_COD_CODE_15'])
#            .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')
#            .withColumn("REG_DATE_OF_DEATH",
#                        f.when(f.col("REG_DATE_OF_DEATH").isin(_deaths_dates_invalid), f.col("REG_DATE"))
#                        .otherwise(f.col("REG_DATE_OF_DEATH")))
#            .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
#            .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))
#            .withColumn('_rownum', f.row_number().over(_win))
#            .where(
#              (f.col('_rownum') == 1)
#              & (f.col('PERSON_ID').isNotNull())
#              & (f.col('REG_DATE_OF_DEATH').isNotNull())
#              & (f.col('REG_DATE_OF_DEATH') > '1900-01-01')
#              & (f.col('REG_DATE_OF_DEATH') <= '2022-10-31') 
#            )
#            .drop('_rownum')
#            .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_0')
#            .where((f.col('REG_DATE_OF_DEATH') >= start_date) & (f.col('REG_DATE_OF_DEATH') <= end_date))
#            .distinct() 
#           )
# _deaths = (_deaths
#            .withColumn('S_COD_CODE_CONCAT',
#                        f.concat_ws(',', *[col for col in _deaths.columns if re.match('.*_COD_.*', col)]))
#           )
# save_dataset('_tmp_deaths', _deaths)
_deaths = read_dataset('_tmp_deaths')

_deaths = _deaths.select([f.col(x).alias(x.lower()) for x in _deaths.columns])
_deaths = (_deaths
           .join(_patient.select(['person_id']), ['person_id'], 'leftsemi')
           .select(['person_id', 'reg_date_of_death', 'reg_date', 's_cod_code_0', 's_cod_code_concat'])
          )

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 COVID Admission

# COMMAND ----------

# MAGIC %md ## 3.0 HES APC

# COMMAND ----------

_hes_apc_adm_any = (_hes_apc
                    .where(f.col('diag_4_concat').rlike('U071'))
                    .withColumn('type', f.lit('01_Covid_admission_any_position'))
                    .withColumn('source', f.lit('1.hes_apc'))
                    .select('person_id', 'admidate', 'disdate', 'type', 'source', 'susrecid')
                   )
#count_var(_hes_apc_adm_any, 'person_id')


_hes_apc_adm_pri = (_hes_apc
                    .where(f.col('diag_4_01').rlike('U071'))
                    .withColumn('type', f.lit('02_Covid_admission_pri_position'))
                    .withColumn('source', f.lit('1.hes_apc'))
                    .select('person_id', 'admidate', 'disdate', 'type', 'source', 'susrecid')
                   )
#count_var(_hes_apc_adm_pri, 'person_id')

_hes_apc_readm_all = (_hes_apc
                    .withColumn('type', f.lit('03_hosp_readmission_all_cause'))
                    .withColumn('covid_status', f.lit('NA'))
                    .withColumn('source', f.lit('1.hes_apc'))
                    .select('person_id', 'admidate', 'disdate', 'type', 'source', 'susrecid')
                   )
#count_var(_hes_apc_readm_all, 'person_id')

_hes_apc_readm_pri = (_hes_apc
                    .where(f.col('diag_4_01').rlike('U071'))
                      .withColumn('type', f.lit('04_hosp_readmission_covid'))
                    .withColumn('covid_status', f.lit('NA'))
                    .withColumn('source', f.lit('1.hes_apc'))
                    .select('person_id', 'admidate', 'disdate', 'type', 'source', 'susrecid')
                   )


# COMMAND ----------

# MAGIC %md ## 3.1 CHESS

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess
# ------------------------------------------------------------------------------
_chess_adm_any = (_chess
                  .withColumn('type', f.lit('01_Covid_admission_any_position'))
                  .withColumn('source', f.lit('2.chess'))
                  .withColumn('susrecid', f.lit(None))
                  .select('person_id', 'admidate', 'disdate', 'type', 'source', 'susrecid')
             )

# COMMAND ----------

# MAGIC %md ## 3.3 Death

# COMMAND ----------

# ==========================================
#              DEATH POSITIVE
# ==========================================
_deaths_pos_any = (_deaths
               .where(f.col('s_cod_code_concat').rlike('U071'))
               .withColumn('type', f.lit('04_Covid_Death_with_anypos'))
               .withColumn('source', f.lit('deaths'))
               .withColumnRenamed('reg_date_of_death', 'death_date')
               .select('person_id', 'death_date', 'type', 'source')
                   )
#count_var(_deaths_pos_any, 'person_id')


_deaths_pos_pri = (_deaths
               .where(f.col('s_cod_code_0').rlike('U071'))
               .withColumn('type', f.lit('05_Covid_Death_with_pripos'))
               .withColumn('source', f.lit('deaths'))
               .withColumnRenamed('reg_date_of_death', 'death_date')
               .select('person_id', 'death_date', 'type', 'source')
                   )
#count_var(_deaths_pos_pri, 'person_id')


_deaths_pos_allcause = (_deaths
               .withColumn('type', f.lit('06_Death_all_cause'))
               .withColumn('source', f.lit('deaths'))
               .withColumnRenamed('REG_DATE_OF_DEATH', 'death_date')
               .select('person_id', 'death_date', 'type', 'source')
                   )
#count_var(_deaths_pos_allcause, 'person_id')

# COMMAND ----------

# MAGIC %md ## 3.4 Long COVID

# COMMAND ----------

data = io.StringIO('''
"name","terminology","code","term","nchar"
"longcovid","SNOMED","1325021000000106","Signposting to Your COVID Recovery",16
"longcovid","SNOMED","1325031000000108","Referral to post-COVID assessment clinic",16
"longcovid","SNOMED","1325041000000104","Referral to Your COVID Recovery rehabilitation platform",16
"longcovid","SNOMED","1325051000000101","Newcastle post-COVID syndrome Follow-up Screening Questionnaire",16
"longcovid","SNOMED","1325061000000103","Assessment using Newcastle post-COVID syndrome Follow-up Screening Questionnaire",16
"longcovid","SNOMED","1325071000000105","C19-YRS (COVID-19 Yorkshire Rehabilitation Screening) tool",16
"longcovid","SNOMED","1325081000000107","Assessment using C19-YRS (COVID-19 Yorkshire Rehabilitation Screening) tool",16
"longcovid","SNOMED","1325091000000109","PCFS (Post-COVID-19 Functional Status) Scale patient self-report",16
"longcovid","SNOMED","1325101000000101","Assessment using PCFS (Post-COVID-19 Functional Status) Scale patient self-report",16
"longcovid","SNOMED","1325121000000105","PCFS (Post-COVID-19 Functional Status) Scale patient self-report final scale grade",16
"longcovid","SNOMED","1325131000000107","PCFS (Post-COVID-19 Functional Status) Scale structured interview final scale grade",16
"longcovid","SNOMED","1325141000000103","Assessment using PCFS (Post-COVID-19 Functional Status) Scale structured interview",16
"longcovid","SNOMED","1325151000000100","PCFS (Post-COVID-19 Functional Status) Scale structured interview",16
"longcovid","SNOMED","1325161000000102","Post-COVID-19 syndrome",16
"longcovid","SNOMED","1325181000000106","Ongoing symptomatic COVID-19",16
"longcovid","SNOMED","1326351000000108","Post-COVID-19 syndrome resolved",16
'''
                  )

pd_df = pd.read_csv(data, sep=",")
lc_code = spark.createDataFrame(pd_df)
display(lc_code)

# COMMAND ----------

_gdppr = (gdppr
          .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])
          .withColumnRenamed('NHS_NUMBER_DEID', 'person_id')
          .withColumnRenamed('DATE', 'date')
          .withColumnRenamed('CODE', 'code')
          .filter(f.col("person_id").isNotNull())
          .where((f.col('date') >= start_date) & (f.col('date') <= end_date))
          .distinct()
          .join(lc_code, ['code'], 'leftsemi')
         )
#save_dataset('_tmp_gdppr_longcovid_codes', _gdppr)
_gdppr = read_dataset('_tmp_gdppr_longcovid_codes')
_gdppr = (_gdppr
          .select(['person_id', 'date', 'code'])
          .join(_patient.select(['person_id']), ['person_id'], 'leftsemi')
          .select(['person_id', 'date'])
          .withColumnRenamed('date', 'date_longcovid')
          .distinct()
               )

window_spec = Window.partitionBy(_gdppr["person_id"]).orderBy(f.col("date_longcovid").asc_nulls_last())
_gdppr = (_gdppr
                       .withColumn("rank_col", f.row_number().over(window_spec))
                       .filter(f.col("rank_col")==1)
                       .drop("rank_col")
                       .orderBy('person_id', 'date_longcovid')
                      )

# COMMAND ----------

# MAGIC %md # 4 Combine

# COMMAND ----------

covid_hosp_data = (
  _hes_apc_adm_any
  .unionByName(_hes_apc_adm_pri)
  .unionByName(_hes_apc_readm_all)
  .unionByName(_hes_apc_readm_pri)
  .unionByName(_chess_adm_any)
  .orderBy(f.desc('admidate'))
  .distinct()
  )
covid_death_data = (
  _deaths_pos_any
  .unionByName(_deaths_pos_pri)
  .unionByName(_deaths_pos_allcause)
  .orderBy(f.desc('death_date'))
  )

# COMMAND ----------

# MAGIC %md ## 4.1 Save

# COMMAND ----------

#if save_data:
save_dataset('_tmp_covid_admission', covid_hosp_data)
save_dataset('_tmp_covid_death', covid_death_data)

covid_hosp_data = read_dataset('_tmp_covid_admission')
covid_death_data = read_dataset('_tmp_covid_death')

# COMMAND ----------

display(covid_hosp_data.orderBy('person_id','admidate'))

# COMMAND ----------

# MAGIC %md ## 4.2 Pri Admission

# COMMAND ----------

covid_pri_hosp_data = (covid_hosp_data
                       .filter(f.col("type")=='02_Covid_admission_pri_position')
                       .select(['person_id', 'admidate', 'disdate', 'source', 'susrecid'])
                      )
window_spec = Window.partitionBy(covid_pri_hosp_data["person_id"]).orderBy(f.col("admidate").asc_nulls_last())
covid_pri_hosp_data = (covid_pri_hosp_data
                       .withColumn("rank_col", f.row_number().over(window_spec))
                       .filter(f.col("rank_col")==1)
                       .drop("rank_col")
                       .orderBy('person_id', 'admidate')
                       .withColumnRenamed('admidate', 'admidate_covid_pri')
                       .withColumnRenamed('disdate', 'disdate_covid_pri')
                       .withColumnRenamed('source', 'source_covid_pri')
                       .withColumn('covid_pri', f.lit(1))
                      )

# COMMAND ----------

save_dataset('_tmp_covid_pri_admission', covid_pri_hosp_data)
covid_pri_hosp_data = read_dataset('_tmp_covid_pri_admission')

# COMMAND ----------

# MAGIC %md ## 4.3 Pri Death

# COMMAND ----------

covid_pri_death = (covid_death_data
                   .filter(f.col("type")=='05_Covid_Death_with_pripos')
                   .select(['person_id', 'death_date', 'source'])
                   .orderBy('person_id', 'death_date')
                   .withColumnRenamed('death_date', 'deathdate_covid_pri')
                   .withColumnRenamed('source', 'source_covid_pri')
                  )
save_dataset('_tmp_covid_pri_death', covid_pri_death)
covid_pri_death = read_dataset('_tmp_covid_pri_death')

# COMMAND ----------

all_death = (covid_death_data
                   .filter(f.col("type")=='06_Death_all_cause')
                   .select(['person_id', 'death_date', 'source'])
                   .orderBy('person_id', 'death_date')
                   .withColumnRenamed('death_date', 'deathdate_allcause')
                   .withColumnRenamed('source', 'source_allcause')
                  )
save_dataset('_tmp_all_death', all_death)
all_death = read_dataset('_tmp_all_death')

# COMMAND ----------

# MAGIC %md ## 4.4 Readmission

# COMMAND ----------

display(_hes_apc_readm_pri)

# COMMAND ----------

hes_readmission_covid = (_hes_apc_readm_pri
                   .select('person_id', 'admidate', 'disdate')
                   .join(covid_pri_hosp_data.select(['person_id', 'admidate_covid_pri', 'disdate_covid_pri']),
                         ['person_id'], 'left')
                   .filter(f.col("admidate_covid_pri").isNotNull())
                   .filter(f.col("admidate")>f.col("disdate_covid_pri"))
                   .orderBy('person_id', 'admidate')
                   )
#count_var(hes_readmission_covid, 'person_id')


window_spec = Window.partitionBy(hes_readmission_covid["person_id"]).orderBy(f.col("admidate").asc_nulls_last())
hes_readmission_covid = (hes_readmission_covid
                       .withColumn("rank_col", f.row_number().over(window_spec))
                       .filter(f.col("rank_col")==1)
                       .drop("rank_col")
                       .orderBy('person_id', 'admidate')
                       .withColumnRenamed('admidate', 'admidate_30dreadm_covid')
                   .select('person_id', 'admidate_30dreadm_covid')
                      )

# COMMAND ----------

hes_readmission_all = (_hes_apc_readm_all
                   .select('person_id', 'admidate', 'disdate')
                   .join(covid_pri_hosp_data.select(['person_id', 'admidate_covid_pri', 'disdate_covid_pri']),
                         ['person_id'], 'left')
                   .filter(f.col("admidate_covid_pri").isNotNull())
                   .filter(f.col("admidate")>f.col("disdate_covid_pri"))
                   .orderBy('person_id', 'admidate')
                   )
  
window_spec = Window.partitionBy(hes_readmission_all["person_id"]).orderBy(f.col("admidate").asc_nulls_last())
hes_readmission_all = (hes_readmission_all
                       .withColumn("rank_col", f.row_number().over(window_spec))
                       .filter(f.col("rank_col")==1)
                       .drop("rank_col")
                       .orderBy('person_id', 'admidate')
                       .withColumnRenamed('admidate', 'admidate_30dreadm_all')
                   .select('person_id', 'admidate_30dreadm_all')
                      )

# COMMAND ----------

save_dataset('_tmp_hosp_30day_readmission_covid', hes_readmission_covid)
hes_readmission_covid = read_dataset('_tmp_hosp_30day_readmission_covid')
count_var(hes_readmission_covid, 'person_id')
save_dataset('_tmp_hosp_30day_readmission_all', hes_readmission_all)
hes_readmission_all = read_dataset('_tmp_hosp_30day_readmission_all')
count_var(hes_readmission_all, 'person_id')

# COMMAND ----------

# MAGIC %md # 5 Covid ICU

# COMMAND ----------

_hec_cc_icu = (_hes_cc_apc
                 .where(f.col('diag_4_01').rlike('U071'))
                 .select(['person_id', 'startdate_cc', 'admidate_cc'])
                 .orderBy('person_id', 'admidate_cc','startdate_cc')
                 .withColumnRenamed('admidate_cc', 'admidate_covid_pri')
                 .join(covid_pri_hosp_data
                       .select(['person_id', 'admidate_covid_pri', 'disdate_covid_pri']),
                       on=['person_id', 'admidate_covid_pri'], how='inner')
                 .filter(f.col('startdate_cc') >= f.col('admidate_covid_pri'))
                 .filter(f.col('startdate_cc') <= f.col('disdate_covid_pri'))
                )

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess ICU
# ------------------------------------------------------------------------------
_chess_icu = (_chess
              .where(f.col('DateAdmittedICU').isNotNull())
              .select(['person_id', 'DateAdmittedICU'])
              .withColumnRenamed('DateAdmittedICU', 'startdate_cc_chess')
             )

# COMMAND ----------

covid_admcc_data = (covid_pri_hosp_data 
                    .join(_hec_cc_icu.drop('disdate_covid_pri'),
                          on=['person_id', 'admidate_covid_pri'], how='left')
                    .join(_chess_icu, on=['person_id', ], how='left')
                    .orderBy('person_id', 'admidate_covid_pri')
                    .select(['person_id','admidate_covid_pri','disdate_covid_pri',
                             'startdate_cc', 'startdate_cc_chess'])
                    .withColumn("date_cc_hes_chess", f.least(*["startdate_cc","startdate_cc_chess"]))
                    .withColumn('icu_within_hosp_adm',
                                f.when(
                                  (f.col('date_cc_hes_chess')>=f.col('admidate_covid_pri'))
                                  & (f.col('date_cc_hes_chess')<=f.col('disdate_covid_pri')),
                                       f.lit(1))
                                .otherwise(f.lit(None)))
                    .withColumn('date_icu',
                                f.when(
                                  (f.col('icu_within_hosp_adm')==1), f.lit(1))
                                .otherwise(f.lit(None)))
                    
                   )

# COMMAND ----------

window_spec = (Window.partitionBy(covid_admcc_data["person_id"])
               .orderBy(f.col("admidate_covid_pri").asc_nulls_last())
              )
covid_admcc_data = (covid_admcc_data
                    .withColumn("rank_col", f.row_number().over(window_spec))
                    .filter(f.col("rank_col")==1)
                    .drop("rank_col")
                    .orderBy('person_id', 'admidate_covid_pri')
                    .withColumn('date_icu',
                                f.when((f.col('icu_within_hosp_adm').isNotNull()),f.col('date_cc_hes_chess'))
                                .otherwise(f.lit(None)))
          )


# COMMAND ----------

# MAGIC %md # 6 Ventilatory Support

# COMMAND ----------

# MAGIC %md ## 6.1 NIV

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess NIV
# ------------------------------------------------------------------------------
_chess_niv = (_chess
              .filter((f.col("Highflownasaloxygen")=='Yes') | (f.col("NoninvasiveMechanicalventilation")=='Yes'))
              .where(f.col('DateAdmittedICU').isNotNull())
              .select(['person_id', 'DateAdmittedICU'])
              .withColumnRenamed('DateAdmittedICU', 'startdate_vent')
              .withColumn('source', f.lit('chess_niv'))
              .withColumn('pri_source', f.lit('chess'))
             )

# ------------------------------------------------------------------------------
# HES APC NIV
# ------------------------------------------------------------------------------
# E85.2. Non-invasive ventilation NEC
# E85.6. Continuous positive airway pressure
_hes_apc_niv = (_hes_apc
                .where(f.col('opertn_4_concat').rlike('E85(2|6)'))
                .where(f.col('diag_4_concat').rlike('U071'))
                .withColumn('source', f.lit('hes_apc_niv'))
                .withColumnRenamed('epistart', 'startdate_vent')
                .select('person_id', 'startdate_vent', 'source')
                .withColumn('pri_source', f.lit('hes_apc'))
                   )

# COMMAND ----------

# MAGIC %md ## 6.2 IMV

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess IMV
# ------------------------------------------------------------------------------
_chess_imv = (_chess
              .where(f.col('DateAdmittedICU').isNotNull())
              .where(f.col('Invasivemechanicalventilation')=="Yes")
              .select(['person_id', 'DateAdmittedICU'])
              .withColumnRenamed('DateAdmittedICU', 'startdate_vent')
              .withColumn('source', f.lit('chess_imv'))
              .withColumn('pri_source', f.lit('chess'))
             )

# ------------------------------------------------------------------------------
# HES APC IMV
# ------------------------------------------------------------------------------
# "%E851%" THEN 'Invasive ventilation'
# "%X56%" THEN 'Intubation of trachea'
_hes_apc_imv = (_hes_apc
                .where((f.col('opertn_4_concat').rlike('E851')) | (f.col('OPERTN_4_CONCAT').rlike('X56')))
                .where(f.col('diag_4_concat').rlike('U071'))
                .withColumn('source', f.lit('hes_apc_imv'))
                .withColumnRenamed('epistart', 'startdate_vent')
                .select('person_id', 'startdate_vent', 'source')
                .withColumn('pri_source', f.lit('hes_apc'))
                   )

# COMMAND ----------

# MAGIC %md ## 6.3 EMCO

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess EMCO
# ------------------------------------------------------------------------------
_chess_emco = (_chess
               .filter((f.col("RespiratorySupportECMO")=='Yes'))
               .where(f.col('DateAdmittedICU').isNotNull())
               .select(['person_id', 'DateAdmittedICU'])
               .withColumnRenamed('DateAdmittedICU', 'startdate_vent')
               .withColumn('source', f.lit('chess_emco'))
               .withColumn('pri_source', f.lit('chess'))
             )

# ------------------------------------------------------------------------------
# HES APC ECMO
# ------------------------------------------------------------------------------
# "X58.1" "Extracorporeal membrane oxygenation"
_hes_apc_ecmo = (_hes_apc
                 .where(f.col('opertn_4_concat').rlike('X581'))
                 .where(f.col('diag_4_concat').rlike('U071'))
                 .withColumnRenamed('epistart', 'startdate_vent')
                 .withColumn('source', f.lit('hes_apc_emco'))
                 .select('person_id', 'startdate_vent', 'source')
                 .withColumn('pri_source', f.lit('hes_apc'))
                   )

# COMMAND ----------

covid_vent_data = (_hes_apc_niv
                 .unionByName(_chess_niv)
                 .unionByName(_hes_apc_imv)
                 .unionByName(_chess_imv)
                 .unionByName(_hes_apc_ecmo)
                 .unionByName(_chess_emco)
                   .distinct()
      )

# COMMAND ----------

covid_admvent_data = (covid_pri_hosp_data 
                    .join(covid_vent_data.select('person_id', 'startdate_vent'),
                          on=['person_id'], how='left')
                    .orderBy('person_id', 'admidate_covid_pri')
                    .select(['person_id','admidate_covid_pri','disdate_covid_pri','startdate_vent'])
                    .withColumn('vent_within_hosp_adm',
                                f.when(
                                  (f.col('startdate_vent')>=f.col('admidate_covid_pri'))
                                  & (f.col('startdate_vent')<=f.col('disdate_covid_pri')),
                                       f.lit(1))
                                .otherwise(f.lit(None)))
                )

# COMMAND ----------

window_spec = (Window.partitionBy(covid_admvent_data["person_id"])
               .orderBy(f.col("admidate_covid_pri").asc_nulls_last())
              )
covid_admvent_data = (covid_admvent_data
                      .withColumn("rank_col", f.row_number().over(window_spec))
                      .filter(f.col("rank_col")==1)
                      .drop("rank_col")
                      .orderBy('person_id', 'admidate_covid_pri')
                      .withColumn('date_ventilation',
                                f.when((f.col('vent_within_hosp_adm').isNotNull()),f.col('startdate_vent'))
                                .otherwise(f.lit(None)))
          )

# COMMAND ----------

# MAGIC %md # 7 Save CC

# COMMAND ----------

covid_admcc_data = (covid_admcc_data
                    .select(['person_id','admidate_covid_pri','disdate_covid_pri',
                             'icu_within_hosp_adm', 'date_icu'])
                   )
covid_admvent_data = (covid_admvent_data
                      .select(['person_id','admidate_covid_pri','disdate_covid_pri',
                               'vent_within_hosp_adm', 'date_ventilation'])
                     )
covid_cc_data = (covid_admcc_data 
                    .join(covid_admvent_data,
                          on=['person_id','admidate_covid_pri','disdate_covid_pri'], how='left')
                    .orderBy('person_id', 'admidate_covid_pri')
                )

# COMMAND ----------

save_dataset('_tmp_covid_cc', covid_cc_data)
covid_cc_data = read_dataset('_tmp_covid_cc')

# COMMAND ----------

# MAGIC %md # 7 Covid outcomes

# COMMAND ----------

_cohort = spark.table(path_cohort)
_cohort.columns
_cohort = _cohort.select(['person_id',#'dob',#'age_at_start',#'sex',
 'dod',#'ethnicity',#'imd',
 'date_covid',#'source_covid',
 'date_cvd',#'time_cvd_covid',
 'censor_date_start','censor_date_end'])

_cohort = (_cohort
           .join(covid_cc_data, on='person_id', how='left')
           .join(covid_pri_death.drop('source_covid_pri'), on='person_id', how='left')  
           .join(_gdppr, on='person_id', how='left')            
           .join(hes_readmission_covid, on='person_id', how='left')            
           .join(hes_readmission_all, on='person_id', how='left')            
           .withColumn('date_longcovid',
                              f.when(
                                f.datediff(f.col("date_longcovid"), f.col("date_covid")) <28
                                     , f.lit(None)).otherwise(f.col("date_longcovid")))
           .withColumn('admidate_30dreadm_covid',
                              f.when(
                                f.datediff(f.col("admidate_30dreadm_covid"), f.col("admidate_covid_pri")) >30
                                     , f.lit(None)).otherwise(f.col("admidate_30dreadm_covid")))
           .withColumn('admidate_30dreadm_all',
                              f.when(
                                f.datediff(f.col("admidate_30dreadm_all"), f.col("admidate_covid_pri")) >30
                                     , f.lit(None)).otherwise(f.col("admidate_30dreadm_all")))
          )

# COMMAND ----------

_cohort = (_cohort
                  .withColumn('hosp_adm_2wks',
                              f.when(
                                (f.col('admidate_covid_pri').isNotNull())
                                & (f.datediff(f.col('admidate_covid_pri'), f.col("date_covid")) <= 14),
                                     f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('death_within_4wks',
                              f.when(
                                (f.col('admidate_covid_pri').isNotNull())
                                & (f.col('deathdate_covid_pri').isNotNull())
                                & (f.datediff(f.col('deathdate_covid_pri'), f.col("admidate_covid_pri")) <= 28),
                                     f.lit(1)).otherwise(f.lit(0)))
                )

_cohort = _cohort.fillna(value=0,subset=['icu_within_hosp_adm', 'vent_within_hosp_adm'])
display(_cohort)

# COMMAND ----------

for var in ['hosp_adm_2wks', 'death_within_4wks', 'icu_within_hosp_adm', 'vent_within_hosp_adm']:
  _cohort.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

null_counts = {col:_cohort.filter(_cohort[col].isNull()).count() for col in _cohort.columns}
null_counts

# COMMAND ----------

# MAGIC %md ## 7.1 Short Term

# COMMAND ----------

covid_outcomes = (_cohort
                   .withColumn('oc_covid_mod',
                              f.when(
                                (f.col('hosp_adm_2wks')==1)
                                & (f.col('icu_within_hosp_adm')==0)
                                & (f.col('death_within_4wks')==0)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_covid_mod_flwup_start',f.col('date_covid'))
                  .withColumn('oc_covid_mod_flwup_end',
                              f.when((f.col('oc_covid_mod')==1),f.col('admidate_covid_pri'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_covid_mod_flwup_time',
                              f.datediff(f.col("oc_covid_mod_flwup_end"),
                                         f.col("oc_covid_mod_flwup_start"))+1)
                  ##########################
                  .withColumn('oc_covid_sev',
                              f.when(
                                ((f.col('hosp_adm_2wks')==1) & (f.col('icu_within_hosp_adm')==1))
                                | (f.col('death_within_4wks')==1)
                                , f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_covid_sev_flwup_start',f.col('date_covid'))
                  .withColumn('oc_covid_sev_flwup_end',
                              f.when((f.col('oc_covid_sev')==1) 
                                     , f.least('date_icu', 'deathdate_covid_pri' ))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_covid_sev_flwup_time',
                              f.datediff(f.col("oc_covid_sev_flwup_end"),
                                         f.col("oc_covid_sev_flwup_start"))+1)
                  ##############################
                  ############################## 
                  .withColumn('oc_covid_mod2',
                              f.when(
                                (f.col('hosp_adm_2wks')==1)
                                & (f.col('icu_within_hosp_adm')==0)
                                & (f.col('vent_within_hosp_adm')==0)
                                & (f.col('death_within_4wks')==0)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_covid_mod2_flwup_start',f.col('date_covid'))
                  .withColumn('oc_covid_mod2_flwup_end',
                              f.when((f.col('oc_covid_mod2')==1),f.col('admidate_covid_pri'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_covid_mod2_flwup_time',
                              f.datediff(f.col("oc_covid_mod2_flwup_end"),
                                         f.col("oc_covid_mod2_flwup_start"))+1)
                  ##########################
                  .withColumn('oc_covid_sev2',
                              f.when(
                                (
                                  ((f.col('hosp_adm_2wks')==1) & (f.col('icu_within_hosp_adm')==1))
                                  | ((f.col('hosp_adm_2wks')==1) & (f.col('vent_within_hosp_adm')==1))
                                  | (f.col('death_within_4wks')==1)
                              )
                                , f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_covid_sev2_flwup_start',f.col('date_covid'))
                  .withColumn('oc_covid_sev2_flwup_end',
                              f.when((f.col('oc_covid_sev2')==1) 
                                     , f.least('date_icu', 'deathdate_covid_pri', 'date_ventilation'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_covid_se2v_flwup_time',
                              f.datediff(f.col("oc_covid_sev2_flwup_end"),
                                         f.col("oc_covid_sev2_flwup_start"))+1)
                 )

for var in ['oc_covid_mod', 'oc_covid_sev','oc_covid_mod2', 'oc_covid_sev2']:
  covid_outcomes.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

# MAGIC %md ## 7.2 Intermediate Term

# COMMAND ----------

covid_outcomes = (covid_outcomes
                  .withColumn('oc_longcovid',
                              f.when( 
                                (f.col('date_longcovid').isNotNull())
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_longcovid_flwup_start',f.col('date_covid'))
                  .withColumn('oc_longcovid_flwup_end',
                              f.when((f.col('oc_longcovid')==1),f.col('date_longcovid'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_longcovid_flwup_time',
                              f.datediff(f.col("oc_longcovid_flwup_start"),
                                         f.col("oc_longcovid_flwup_end"))+1)
                  ###############################
                   .withColumn('oc_30ddeath_all',
                              f.when( 
                                (f.col('dod').isNotNull())
                                & (f.datediff(f.col("dod"),f.col("date_covid")) <=30)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_30ddeath_all_flwup_start',f.col('date_covid'))
                  .withColumn('oc_30ddeath_all_flwup_end',
                              f.when((f.col('oc_30ddeath_all')==1),f.col('dod'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_30ddeath_all_flwup_time',
                              f.datediff(f.col("oc_30ddeath_all_flwup_end"),
                                         f.col("oc_30ddeath_all_flwup_start"))+1)
                  ##############################
                  .withColumn('oc_30ddeath_covid',
                              f.when( 
                                (f.col('deathdate_covid_pri').isNotNull())
                                & (f.datediff(f.col("deathdate_covid_pri"),f.col("date_covid")) <=30)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_30ddeath_covid_flwup_start',f.col('date_covid'))
                  .withColumn('oc_30ddeath_covid_flwup_end',
                              f.when((f.col('oc_30ddeath_covid')==1),f.col('deathdate_covid_pri'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_30ddeath_covid_flwup_time',
                              f.datediff(f.col("oc_30ddeath_covid_flwup_end"),
                                         f.col("oc_30ddeath_covid_flwup_start"))+1)
                  ###############################
                   .withColumn('oc_30dreadm_all',
                              f.when( 
                                (f.col('admidate_30dreadm_all').isNotNull())
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_30dreadm_all_flwup_start',f.col('date_covid'))
                  .withColumn('oc_30dreadm_all_flwup_end',
                              f.when((f.col('oc_30dreadm_all')==1),f.col('admidate_30dreadm_all'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_30dreadm_all_flwup_time',
                              f.datediff(f.col("oc_30dreadm_all_flwup_end"),
                                         f.col("oc_30dreadm_all_flwup_start"))+1)
                  ##############################
                  .withColumn('oc_30dreadm_covid',
                              f.when( 
                                (f.col('admidate_30dreadm_covid').isNotNull())
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_30dreadm_covid_flwup_start',f.col('date_covid'))
                  .withColumn('oc_30dreadm_covid_flwup_end',
                              f.when((f.col('oc_30dreadm_covid')==1),f.col('admidate_30dreadm_covid'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_30dreadm_covid_flwup_time',
                              f.datediff(f.col("oc_30dreadm_covid_flwup_end"),
                                         f.col("oc_30dreadm_covid_flwup_start"))+1)
                 )

for var in ['oc_longcovid', 'oc_30ddeath_all', 'oc_30ddeath_covid','oc_30dreadm_all','oc_30dreadm_covid']:
  covid_outcomes.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

# MAGIC %md ## 7.3 Long Term

# COMMAND ----------

covid_outcomes = (covid_outcomes
                   .withColumn('oc_1yrdeath_all',
                              f.when( 
                                (f.col('dod').isNotNull())
                                & (f.datediff(f.col("dod"),f.col("date_covid")) <=365)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_1yrdeath_all_flwup_start',f.col('date_covid'))
                  .withColumn('oc_1yrdeath_all_flwup_end',
                              f.when((f.col('oc_1yrdeath_all')==1),f.col('dod'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_1yrdeath_all_flwup_time',
                              f.datediff(f.col("oc_1yrdeath_all_flwup_end"),
                                         f.col("oc_1yrdeath_all_flwup_start"))+1)
                  ##############################
                  .withColumn('oc_1yrdeath_covid',
                              f.when( 
                                (f.col('deathdate_covid_pri').isNotNull())
                                & (f.datediff(f.col("deathdate_covid_pri"),f.col("date_covid")) <=365)
                                ,f.lit(1)).otherwise(f.lit(0)))
                  .withColumn('oc_1yrdeath_covid_flwup_start',f.col('date_covid'))
                  .withColumn('oc_1yrdeath_covid_flwup_end',
                              f.when((f.col('oc_1yrdeath_covid')==1),f.col('deathdate_covid_pri'))
                              .otherwise(f.col('censor_date_end')))
                  .withColumn('oc_1yrdeath_covid_flwup_time',
                              f.datediff(f.col("oc_1yrdeath_covid_flwup_end"),
                                         f.col("oc_1yrdeath_covid_flwup_start"))+1)
                 )
for var in ['oc_1yrdeath_all', 'oc_1yrdeath_covid']:
  covid_outcomes.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()

# COMMAND ----------

save_dataset('_cur_survival_dataset', covid_outcomes)
covid_outcomes = read_dataset('_cur_survival_dataset')
count_var(covid_outcomes, 'person_id')

# COMMAND ----------

covid_outcomes.columns

# COMMAND ----------

df=covid_outcomes.columns
from pyspark.sql.types import StringType
df=spark.createDataFrame(df, StringType()).toDF("columns")
display(df)
