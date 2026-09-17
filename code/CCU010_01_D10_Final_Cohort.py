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
path_mltc          = f'{dbc}.{proj}_cur_mltc_cohort'
path_cohort_1      = f'{dbc}.{proj}_final_cohort_1'
path_surv          = f'{dbc}.{proj}_cur_survival_dataset'

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
  proj = 'ccu010_01'
  db = ''
  dbc = f'{db}_collab'
  spark.sql('CLEAR CACHE')
  path_tmp_covid = f'{dbc}.{proj}{name}'
  spark.sql(F"""REFRESH TABLE {path_tmp_covid}""")
  dataset = spark.table(path_tmp_covid)
  return dataset

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_covid}""")
spark.sql(F"""REFRESH TABLE {path_mltc}""")
spark.sql(F"""REFRESH TABLE {path_cohort_1}""")
tmp_cohort         = spark.table(path_cohort_1)
codelist           = spark.table(path_codelist)
gdppr_data         = spark.table(path_gdppr)

# COMMAND ----------



# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_cohort_1}""")
tmp_cohort = spark.table(path_cohort_1)
#tmp_cohort = tmp_cohort.select(['person_id', 'censor_date_start', 'censor_date_end'])
count_var(tmp_cohort, 'person_id')

# COMMAND ----------

# MAGIC %md # 3 Add MLTC Data

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_mltc}""")
mltc_data = spark.table(path_mltc)
#outcomes_data = outcomes_data.select(['person_id', 'censor_date_start', 'censor_date_end'])
count_var(mltc_data, 'person_id')

# COMMAND ----------

display(mltc_data)

# COMMAND ----------

tab(mltc_data, 'cond_mltc')

# COMMAND ----------

final_cohort = (tmp_cohort
                .join(mltc_data, on='person_id', how='left')
               )
count_var(final_cohort, 'person_id')

# COMMAND ----------

# MAGIC %md # 4 Prepare GDPPR

# COMMAND ----------

_gdppr = (gdppr_data
          .select(['NHS_NUMBER_DEID', 'DATE', 'RECORD_DATE', 'CODE', 'VALUE1_CONDITION'])
          .withColumnRenamed('NHS_NUMBER_DEID', 'person_id')
          .join(final_cohort.select(['person_id']), ['person_id'], 'leftsemi')
          .join(final_cohort.select(['person_id', 'dob', 'dod', 'censor_date_start', 'censor_date_end']),
                ['person_id'], 'left')
         )


_gdppr = (_gdppr
          .withColumn('DATE', f.when(f.col('DATE').isNull(), f.col('RECORD_DATE')).otherwise(f.col('DATE')))
          .where((f.col('DATE') >= f.col('censor_date_start')))
          .where((f.col('DATE') <= 'censor_date_end'))
          .withColumnRenamed('DATE', 'date')
          .withColumnRenamed('CODE', 'code')
          .drop('RECORD_DATE', 'dob', 'dod')
         )

# COMMAND ----------

# MAGIC %md # 5 Smoking status

# COMMAND ----------

codelist_smoking = codelist.where(f.col('name').rlike('^*_smoker$'))
smok_dict = {'current_smoker':'1.current_smoker',
             'former_smoker':'2.former_smoker',
             'never_smoker':'0.never_smoker'}
codelist_smoking = codelist_smoking.replace(smok_dict,subset=['name'])

_gdppr_smok = (_gdppr
               .join(codelist_smoking, on='code', how='inner')
               .select(['person_id', 'date', 'code', 'censor_date_start', 'censor_date_end', 'name'])
               .orderBy('person_id', 'date', 'name')
               .distinct()
              )
save_dataset('_tmp_gdppr_smok', _gdppr_smok)
_gdppr_smok = read_dataset('_tmp_gdppr_smok')

# COMMAND ----------

_win = Window.partitionBy(['person_id']).orderBy(f.desc('date'))
_gdppr_smok = (_gdppr_smok
               .withColumn('_rownum', f.row_number().over(_win))
               .where(f.col('_rownum') == 1)
               .select('person_id', 'censor_date_start', 'censor_date_end', 'date', 'name')
               .orderBy('person_id', 'date', 'name')
              )
_gdppr_smok.select(f.col('name')).groupBy('name').count().sort(f.col('name').desc()).show()
count_var(_gdppr_smok, 'person_id')

# COMMAND ----------

display(_gdppr_smok)

# COMMAND ----------

final_cohort = (final_cohort
                .join(_gdppr_smok.select('person_id', 'date', 'name'), on='person_id', how='left')
                .withColumnRenamed('date', 'date_smoking')
                .withColumnRenamed('name', 'smoking_status')
               )
final_cohort.select(f.col('smoking_status')).groupBy('smoking_status').count().sort(f.col('smoking_status').desc()).show()
final_cohort.agg(f.min("date_smoking"), f.max("date_smoking")).show()

# COMMAND ----------

# MAGIC %md #5 BMI

# COMMAND ----------

codelist_bmi_values = [
  '722595002'
  , '914741000000103'
  , '914731000000107'
  , '914721000000105'
  , '35425004'
  , '48499001'
  , '301331008'
  , '6497000'
  , '310252000'
  , '427090001'
  , '408512008'
  , '162864005'
  , '162863004'
  , '412768003'
  , '60621009'
  , '846931000000101'
]
codelist_bmi_values = spark.createDataFrame(pd.DataFrame(codelist_bmi_values, columns = ['code']))

snomed_refset = (spark.table('.gpdata_snomed_refset_full')
                 .withColumnRenamed('SNOMED_conceptId', 'code')
                 .withColumnRenamed('SNOMED_conceptId_description', 'term')
                )
count_var(snomed_refset, 'code'); print()

codelist_bmi_values = merge(snomed_refset, codelist_bmi_values, ['code']); print()
assert all(codelist_bmi_values.toPandas()['_merge'].isin(['both', 'left_only']))

codelist_bmi_values = (codelist_bmi_values
                       .where(f.col('_merge') == 'both')
                       .select('code', 'term')
                       .distinct()
                       .withColumn('name', f.lit('BMI_values'))
                      )
count_var(codelist_bmi_values, 'code'); print()

print(codelist_bmi_values.toPandas().to_string())#; print()

# COMMAND ----------

codelist_bmi_values = codelist_bmi_values.select(['code', 'term'])
_gdppr_bmi = _gdppr.join(codelist_bmi_values, on='code', how='inner')

_gdppr_bmi = (_gdppr_bmi
              .where(
                (f.col('VALUE1_CONDITION').isNotNull())
                & (f.col('VALUE1_CONDITION') >= 12)
                & (f.col('VALUE1_CONDITION') <= 100))
               .dropDuplicates(['person_id','date', 'VALUE1_CONDITION'])
            )

save_dataset('_tmp_gdppr_bmi', _gdppr_bmi)
_gdppr_bmi = read_dataset('_tmp_gdppr_bmi')

count_var(_gdppr_bmi, 'person_id')

# COMMAND ----------

# filter to the last (latest / most recent) recorded status
_win = Window.partitionBy('person_id').orderBy(f.desc('date'))
_gdppr_bmi = (_gdppr_bmi
              .withColumn('_tie_bmi_value', f.dense_rank().over(_win))
              .where(f.col('_tie_bmi_value') == 1)
              .groupBy(['person_id', 'date'])
              .agg(
                f.countDistinct(f.col('VALUE1_CONDITION')).alias(f'_tie_bmi_value_n_distinct')
                , f.sort_array(f.collect_set(f.col('VALUE1_CONDITION'))).alias('_tie_bmi_value_list')
                , f.avg("VALUE1_CONDITION").alias("avg_bmi")
              )
              .withColumnRenamed('avg_bmi', 'bmi_value')
              .withColumnRenamed('date', 'date_bmi')
              .withColumnRenamed('_tie_bmi_value_list', 'bmi_list')
              .drop('_tie_bmi_value_n_distinct')
             )

# COMMAND ----------

final_cohort = (final_cohort
                .join(_gdppr_bmi.select('person_id', 'date_bmi', 'bmi_value'), on='person_id', how='left')
               )
final_cohort.select(f.col('bmi_value')).describe().show()
final_cohort.agg(f.min("date_bmi"), f.max("date_bmi")).show()

# COMMAND ----------

# MAGIC %md # 3 Apply outcomes to skinny data

# COMMAND ----------

spark.sql(F"""REFRESH TABLE {path_surv}""")
outcomes_data = spark.table(path_surv)
count_var(outcomes_data, 'person_id')

# COMMAND ----------

cols = ['person_id',
#  'dod',
#  'date_covid',
#  'date_cvd',
#  'censor_date_start',
#  'censor_date_end',
 'admidate_covid_pri',
 'disdate_covid_pri',
 'icu_within_hosp_adm',
 'date_icu',
 'vent_within_hosp_adm',
 'date_ventilation',
 'deathdate_covid_pri',
 'date_longcovid',
 'admidate_30dreadm_covid',
 'admidate_30dreadm_all',
 'hosp_adm_2wks',
 'death_within_4wks',
 'oc_covid_mod',
 'oc_covid_mod_flwup_start',
 'oc_covid_mod_flwup_end',
 'oc_covid_mod_flwup_time',
 'oc_covid_sev',
 'oc_covid_sev_flwup_start',
 'oc_covid_sev_flwup_end',
 'oc_covid_sev_flwup_time',
 'oc_covid_mod2',
 'oc_covid_mod2_flwup_start',
 'oc_covid_mod2_flwup_end',
 'oc_covid_mod2_flwup_time',
 'oc_covid_sev2',
 'oc_covid_sev2_flwup_start',
 'oc_covid_sev2_flwup_end',
 'oc_covid_se2v_flwup_time',
 'oc_longcovid',
 'oc_longcovid_flwup_start',
 'oc_longcovid_flwup_end',
 'oc_longcovid_flwup_time',
 'oc_30ddeath_all',
 'oc_30ddeath_all_flwup_start',
 'oc_30ddeath_all_flwup_end',
 'oc_30ddeath_all_flwup_time',
 'oc_30ddeath_covid',
 'oc_30ddeath_covid_flwup_start',
 'oc_30ddeath_covid_flwup_end',
 'oc_30ddeath_covid_flwup_time',
 'oc_30dreadm_all',
 'oc_30dreadm_all_flwup_start',
 'oc_30dreadm_all_flwup_end',
 'oc_30dreadm_all_flwup_time',
 'oc_30dreadm_covid',
 'oc_30dreadm_covid_flwup_start',
 'oc_30dreadm_covid_flwup_end',
 'oc_30dreadm_covid_flwup_time',
 'oc_1yrdeath_all',
 'oc_1yrdeath_all_flwup_start',
 'oc_1yrdeath_all_flwup_end',
 'oc_1yrdeath_all_flwup_time',
 'oc_1yrdeath_covid',
 'oc_1yrdeath_covid_flwup_start',
 'oc_1yrdeath_covid_flwup_end',
 'oc_1yrdeath_covid_flwup_time']
outcomes_data = outcomes_data.select(cols)

# COMMAND ----------

final_cohort = (final_cohort
                .join(outcomes_data, on='person_id', how='left')
               )

# COMMAND ----------

final_cohort.columns

# COMMAND ----------

cols = [
 'person_id',
 'dob',
 'sex',
 'dod',
 'ethnicity',
 'imd',
 'age_at_start',
 'date_covid',
 'source_covid',
 'date_cvd',
 'time_cvd_covid',
 'date_smoking',
 'smoking_status',
 'date_bmi',
 'bmi_value',
 'censor_date_start',
 'censor_date_end',
 ###############
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
 #########
 'cond_date_asthma',
 'cond_date_cancer',
 'cond_date_ckd',
 'cond_date_copd',
 'cond_date_dementia',
 'cond_date_depression',
 'cond_date_diabetes',
 'cond_date_hypertension',
 'cond_date_liver_disease',
 'cond_date_schi_bpd',
 'cond_asthma',
 'cond_cancer',
 'cond_ckd',
 'cond_copd',
 'cond_dementia',
 'cond_depression',
 'cond_diabetes',
 'cond_hypertension',
 'cond_liver_disease',
 'cond_schi_bpd',
 'cond_ltc',
 'cond_ltc_cat',
 'cond_mltc',
##########
 'admidate_covid_pri',
 'disdate_covid_pri',
 'icu_within_hosp_adm',
 'date_icu',
 'vent_within_hosp_adm',
 'date_ventilation',
 'deathdate_covid_pri',
 'date_longcovid',
 'admidate_30dreadm_covid',
 'admidate_30dreadm_all',
 'hosp_adm_2wks',
 'death_within_4wks',
 'oc_covid_mod',
 'oc_covid_mod_flwup_start',
 'oc_covid_mod_flwup_end',
 'oc_covid_mod_flwup_time',
 'oc_covid_sev',
 'oc_covid_sev_flwup_start',
 'oc_covid_sev_flwup_end',
 'oc_covid_sev_flwup_time',
 'oc_covid_mod2',
 'oc_covid_mod2_flwup_start',
 'oc_covid_mod2_flwup_end',
 'oc_covid_mod2_flwup_time',
 'oc_covid_sev2',
 'oc_covid_sev2_flwup_start',
 'oc_covid_sev2_flwup_end',
 'oc_covid_se2v_flwup_time',
 'oc_longcovid',
 'oc_longcovid_flwup_start',
 'oc_longcovid_flwup_end',
 'oc_longcovid_flwup_time',
 'oc_30ddeath_all',
 'oc_30ddeath_all_flwup_start',
 'oc_30ddeath_all_flwup_end',
 'oc_30ddeath_all_flwup_time',
 'oc_30ddeath_covid',
 'oc_30ddeath_covid_flwup_start',
 'oc_30ddeath_covid_flwup_end',
 'oc_30ddeath_covid_flwup_time',
 'oc_30dreadm_all',
 'oc_30dreadm_all_flwup_start',
 'oc_30dreadm_all_flwup_end',
 'oc_30dreadm_all_flwup_time',
 'oc_30dreadm_covid',
 'oc_30dreadm_covid_flwup_start',
 'oc_30dreadm_covid_flwup_end',
 'oc_30dreadm_covid_flwup_time',
 'oc_1yrdeath_all',
 'oc_1yrdeath_all_flwup_start',
 'oc_1yrdeath_all_flwup_end',
 'oc_1yrdeath_all_flwup_time',
 'oc_1yrdeath_covid',
 'oc_1yrdeath_covid_flwup_start',
 'oc_1yrdeath_covid_flwup_end',
 'oc_1yrdeath_covid_flwup_time']
final_cohort = final_cohort.select(cols)

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

save_dataset('_final_cohort', final_cohort)
final_cohort = read_dataset('_final_cohort')
display(final_cohort)

# COMMAND ----------

count_var(final_cohort, 'person_id')

# COMMAND ----------

null_counts = {col:final_cohort.filter(final_cohort[col].isNotNull()).count() for col in final_cohort.columns}
null_counts

# COMMAND ----------

for var in ['sex','ethnicity','imd']:
  final_cohort.select(f.col(var)).groupBy(var).count().sort(f.col(var).desc()).show()