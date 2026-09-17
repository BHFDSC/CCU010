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

_datetimenow = datetime.datetime.now()
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %run "Helper_functions/functions"

# COMMAND ----------

# MAGIC %md # 2 Test

# COMMAND ----------

db_collab = ""
gdppr_arch = "gdppr__archive" 
hes_apc_arch = "hes_apc_all_years_archive"
hes_ae_arch = "hes_ae_all_years_archive"
hes_op_arch = "hes_op_all_years_archive"
hes_cc_arch = "hes_cc_all_years_archive"
chess_arch = "chess__archive"
sgss_arch = "sgss__archive"
sus_arch = "sus__archive"
pillar2_arch =  "covid_antigen_testing_pillar2__archive" 
pillar3_arch =  "covid_antibody_testing_pillar3__archive"
death_arch = "deaths__archive"
primary_care_meds_arch = "primary_care_meds__archive" 
vaccine_arch = "vaccine_status__archive"

# COMMAND ----------

# Load archive datasets, PySpark method
gdppr_arch = spark.sql(f'select * from {db_collab}.{gdppr_arch}')
hes_apc_arch = spark.sql(f'select * from {db_collab}.{hes_apc_arch}')
hes_ae_arch = spark.sql(f'select * from {db_collab}.{hes_ae_arch}')
hes_op_arch = spark.sql(f'select * from {db_collab}.{hes_op_arch}')
hes_cc_arch = spark.sql(f'select * from {db_collab}.{hes_cc_arch}')
chess_arch = spark.sql(f'select * from {db_collab}.{chess_arch}')
sgss_arch = spark.sql(f'select * from {db_collab}.{sgss_arch}')
sus_arch = spark.sql(f'select * from {db_collab}.{sus_arch}')
pillar2_arch = spark.sql(f'select * from {db_collab}.{pillar2_arch}')
pillar3_arch = spark.sql(f'select * from {db_collab}.{pillar3_arch}')
death_arch = spark.sql(f'select * from {db_collab}.{death_arch}')
primary_care_meds_arch = spark.sql(f'select * from {db_collab}.{primary_care_meds_arch}')
vaccine_arch = spark.sql(f'select * from {db_collab}.{vaccine_arch}')

# COMMAND ----------

# production_dates = list_of_production_dates(hes_apc_arch, "ProductionDate")
# print(f"Latest production date for hes_apc_arch : {max(production_dates)}")

# COMMAND ----------

#This funciton is in the notebook date_management called with %run above
production_dates = list_of_production_dates(gdppr_arch, "ProductionDate")
print(f"Latest production date for gdppr_arch : {max(production_dates)}")

production_dates = list_of_production_dates(hes_apc_arch, "ProductionDate")
print(f"Latest production date for hes_apc_arch : {max(production_dates)}")

production_dates = list_of_production_dates(hes_ae_arch, "ProductionDate")
print(f"Latest production date for hes_ae_arch : {max(production_dates)}")

production_dates = list_of_production_dates(hes_op_arch, "ProductionDate")
print(f"Latest production date for hes_op_arch : {max(production_dates)}")

production_dates = list_of_production_dates(hes_cc_arch, "ProductionDate")
print(f"Latest production date for hes_cc_arch : {max(production_dates)}")

production_dates = list_of_production_dates(chess_arch, "ProductionDate")
print(f"Latest production date for chess_arch : {max(production_dates)}")

production_dates = list_of_production_dates(sgss_arch, "ProductionDate")
print(f"Latest production date for sgss_arch : {max(production_dates)}")

production_dates = list_of_production_dates(sus_arch, "ProductionDate")
print(f"Latest production date for sus_arch : {max(production_dates)}")

production_dates = list_of_production_dates(pillar2_arch, "ProductionDate")
print(f"Latest production date for pillar2_arch : {max(production_dates)}")

production_dates = list_of_production_dates(pillar3_arch, "ProductionDate")
print(f"Latest production date for pillar3_arch : {max(production_dates)}")

production_dates = list_of_production_dates(death_arch, "ProductionDate")
print(f"Latest production date for death_arch : {max(production_dates)}")

production_dates = list_of_production_dates(primary_care_meds_arch, "ProductionDate")
print(f"Latest production date for primary_care_meds : {max(production_dates)}")

production_dates = list_of_production_dates(vaccine_arch, "ProductionDate")
print(f"Latest production date for vaccine_arch : {max(production_dates)}")

# COMMAND ----------

# MAGIC %md # 3 Table Parameters

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
# archive tables
tmp_archive_date = '2022-09-30'
data = [
    ['deaths',      dbc, f'deaths_{db}_archive', tmp_archive_date, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',       dbc, f'gdppr_{db}_archive',             tmp_archive_date, 'NHS_NUMBER_DEID', 'DATE']
  , ['hes_apc',     dbc, f'hes_apc_all_years_archive',      tmp_archive_date, 'PERSON_ID_DEID', 'EPISTART'] 
  , ['hes_op',      dbc, f'hes_op_all_years_archive',       tmp_archive_date, 'PERSON_ID_DEID', 'APPTDATE'] 
  , ['hes_ae',      dbc, f'hes_ae_all_years_archive',       tmp_archive_date, 'PERSON_ID_DEID', 'ARRIVALDATE'] 
  , ['hes_cc',      dbc, f'hes_cc_all_years_archive',       tmp_archive_date, 'PERSON_ID_DEID', 'CCSTARTDATE']
  , ['chess',       dbc, f'chess_{db}_archive',             tmp_archive_date, 'PERSON_ID_DEID', 'InfectionSwabDate']
  , ['sgss',        dbc, f'sgss_{db}_archive',              tmp_archive_date, 'PERSON_ID_DEID', 'Specimen_Date']
  , ['sus',         dbc, f'sus_{db}_archive',               tmp_archive_date, 'NHS_NUMBER_DEID',  'EPISODE_START_DATE']
]
df_archive = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'production_date', 'id_var', 'date_var'])
df_archive

# COMMAND ----------

# MAGIC %md # 4 Freeze Tables

# COMMAND ----------

## 5 hours
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
datetimenow = "20220930" #datetime.datetime.now().strftime("%Y%m%d")

for ind in df_archive.index:
  row = df_archive.iloc[ind]
  
  tabpath = row['database'] + '.' + row['table'] 
  prodDate = row['production_date']
  id = row['id_var']  

  dataset_name = row['dataset']
  table_name = f'{proj}_freeze_{dataset_name}_{datetimenow}'  
  
  #get archive table
  tmp = spark.table(tabpath).where(f.col('ProductionDate').startswith(prodDate))  
  #display(tmp)
  tmp.createOrReplaceGlobalTempView(f"{proj}_{dataset_name}")
  drop_table(table_name, if_exists=True)
  create_table(table_name , select_sql_script=f"SELECT * FROM global_temp.{proj}_{dataset_name}") 

  person_id_deid = row['id_var']
  print(f'{0 if ind<10 else ""}' + str(ind) + ' ' + tabpath + ' (' + prodDate + ')' + ' [' + table_name + ']', flush=True)
  count_var(tmp, person_id_deid)

# COMMAND ----------

for v in list(vars().keys()):
  if(re.search("dataframe", type(vars()[v]).__name__.lower())):
    print(v, type(vars()[v]))

# COMMAND ----------

# MAGIC %md # 5 Check

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

# COMMAND ----------

hes_apc_freeze         = spark.table(path_hes_apc)
hes_op_freeze          = spark.table(path_hes_op)
hes_ae_freeze          = spark.table(path_hes_ae)
gdppr_freeze           = spark.table(path_gdppr)
deaths_freeze          = spark.table(path_deaths)
chess_freeze           = spark.table(path_chess)
sgss_freeze            = spark.table(path_sgss)
sus_freeze             = spark.table(path_sus)
#display(deaths_freeze)
#deaths_freeze.dtypes

# COMMAND ----------

import pyspark.sql.functions as F

gdppr_freeze.agg(F.min("DATE"), F.max("DATE")).show()
deaths_freeze.agg(F.min("REG_DATE_OF_DEATH"), F.max("REG_DATE_OF_DEATH")).show()
hes_apc_freeze.agg(F.min("EPISTART"), F.max("EPISTART")).show()
chess_freeze.agg(F.min("InfectionSwabDate"), F.max("InfectionSwabDate")).show()
sgss_freeze.agg(F.min("Specimen_Date"), F.max("Specimen_Date")).show()
sus_freeze.agg(F.min("EPISODE_START_DATE"), F.max("EPISODE_START_DATE")).show()

# COMMAND ----------

gdppr_freeze.select(F.col("DATE")).dtypes
deaths_freeze.select(F.col("REG_DATE_OF_DEATH")).dtypes
hes_apc_freeze.select(F.col("EPISTART")).dtypes
chess_freeze.select(F.col("InfectionSwabDate")).dtypes
sgss_freeze.select(F.col("Specimen_Date")).dtypes
sus_freeze.select(F.col("EPISODE_START_DATE")).dtypes

# COMMAND ----------

gdppr_ids = (gdppr_freeze
             .select("NHS_NUMBER_DEID")
             .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
             .distinct()
     )
display(gdppr_ids)