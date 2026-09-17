** CCU010: COHORT SET UP **

* Age categories
sum age_at_start, d
hist age_at_start
sum age_at_start if age_at_start>100
drop if age_at_start>100

* Age intervals
gen age_group = .
replace age_group = 0 if age_at_start <= 49
replace age_group = 1 if age_at_start >= 50 & age_at_start <= 59
replace age_group = 2 if age_at_start >= 60 & age_at_start <= 69
replace age_group = 3 if age_at_start >= 70 & age_at_start <= 79
replace age_group = 4 if age_at_start >= 80
label define ageg 0 "18-49" 1 "50-59" 2 "60-69" 3 "70-79" 4 "80+"
label values age_group ageg
tab age_group
order age_group, after(age_at_start)

* Age 65 - <65 reference
gen age_65 = 0
replace age_65 = 1 if age_at_start >=65
label define agegg 0 "<65" 1 "65+"
label values age_65 agegg
tab age_65
order age_65, after(age_group)

* Sex - currently male=1 and females=2
* change to dummy variables where female==0 and male ==1 - female reference
destring sex, replace
replace sex = 0 if sex == 2
label define sexla 0 "Female" 1 "Male"
label values sex sexla
tab sex

* Ethnicity
* White as the reference (0) ****
tab ethnicity
gen ethn = 0
replace ethn = 1 if ethnicity == "South Asian"
replace ethn = 2 if ethnicity == "Black"
replace ethn = 3 if ethnicity == "Mixed/Other"
label define eth 0 "White" 1 "South Asian" 2 "Black" 3 "Other/Mixed"
label values ethn eth
tab ethn , m
order ethn, after(ethnicity)

* Smoking status - has some missing data
tab smoking_status, m
replace smoking_status="." if smoking_status==""
gen smok = 0
replace smok = 1 if smoking_status == "1.current_smoker"
replace smok = 2 if smoking_status == "2.former_smoker"
replace smok = 3 if smoking_status == "."
label define smmk 0 "never_smoked" 1 "current_smoker" 2 "former_smoker" 3 "missing"
label values smok smmk
tab smok , m
order smok, after(smoking_status)

* IMD - 1 is the most deprived and 5 is the least deprived - want it the other way, so least deprived is 0 (reference)
sum imd
sum imd, d
gen imd_grp = 0
replace imd_grp = 1 if imd = 2
replace imd_grp = 2 if imd = 3
replace imd_grp = 3 if imd = 4
replace imd_grp = 4 if imd = 5
label define im 0 "IMD quintile 1-most deprived" 1 "2" 2 "3" 3 "4" 4 "IMD quintile 1-least deprived"
label values imd_grp im
tab imd_grp , m
order imd_grp, after(imd)

* BMI - has some missing data and needs to be checked for extreme values 
sum bmi_value, d
sum bmi_value if bmi_value>=50
sum bmi_value if bmi_value>=60
sum bmi_value if bmi_value>=70
sum bmi_value if bmi_value>=80
** have range of BMI between 10-80 - remove those >80 change to missing
replace bmi_value=. if bmi_value>80

gen bmi_grp = 0
replace bmi_grp = 1 if bmi_value >= 25 & bmi_value < 30
replace bmi_grp = 2 if bmi_value >= 30
replace bmi_grp = 3 if bmi_value < 18.5
replace bmi_grp = . if bmi_value == .
label define bmg 0 "Normal" 1 "Overweight" 2 "Obese" 3 "Underweight" 999 "Missing"
label values bmi_grp bmg
tab bmi_grp, m

* 16 CVD phenotype
foreach var in cond_aaa cond_af cond_ami cond_angina_st cond_angina_unst cond_chd cond_cardiac_arrest cond_dvt_dvt cond_hf cond_ih cond_pad cond_pe cond_stroke_is cond_stroke_nos cond_stroke_sah cond_stroke_tia {
replace `var' = 0 if `var' ==.
}
* count how many CVD conditions
egen total_CVDcond = rowtotal(cond_aaa-cond_stroke_tia)
tab total_CVDcond
sum total_CVDcond, d

* count how many other
egen total_MLTCcond = rowtotal(cond_asthma-cond_schi_bpd)
tab total_MLTCcond
sum total_MLTCcond, d

* how many patients have a CVD and MLTC conditions
egen total_conditions = rowtotal(cond_aaa cond_af cond_ami cond_angina_st cond_angina_unst cond_chd cond_cardiac_arrest cond_dvt_dvt cond_hf cond_ih cond_pad cond_pe cond_stroke_is cond_stroke_nos cond_stroke_sah cond_stroke_tia cond_asthma cond_cancer cond_ckd cond_copd cond_dementia cond_depression cond_diabetes cond_hypertension cond_liver_disease cond_schi_bpd)
tab total_conditions
sum total_MLTCcond, d

** Outcome: 1-year all-cause mortality
drop disdate_covid_pri icu_within_hosp_adm date_icu vent_within_hosp_adm date_ventilation deathdate_covid_pri date_longcovid ///
admidate_30dreadm_covid admidate_30dreadm_all hosp_adm_2wks death_within_4wks oc_covid_mod oc_covid_mod_flwup_start oc_covid_mod_flwup_end ///
oc_covid_mod_flwup_time oc_covid_sev oc_covid_sev_flwup_start oc_covid_sev_flwup_end oc_covid_sev_flwup_time oc_covid_mod2 ///
oc_covid_mod2_flwup_start oc_covid_mod2_flwup_end oc_covid_mod2_flwup_time oc_covid_sev2 oc_covid_sev2_flwup_start oc_covid_sev2_flwup_end ///
oc_covid_se2v_flwup_time oc_longcovid oc_longcovid_flwup_start oc_longcovid_flwup_end oc_longcovid_flwup_time oc_30ddeath_all ///
oc_30ddeath_all_flwup_start oc_30ddeath_all_flwup_end oc_30ddeath_all_flwup_time oc_30ddeath_covid oc_30ddeath_covid_flwup_start ///
oc_30ddeath_covid_flwup_end oc_30ddeath_covid_flwup_time oc_30dreadm_all oc_30dreadm_all_flwup_start oc_30dreadm_all_flwup_end ///
oc_30dreadm_all_flwup_time oc_30dreadm_covid oc_30dreadm_covid_flwup_start oc_30dreadm_covid_flwup_end oc_30dreadm_covid_flwup_time ///
oc_1yrdeath_covid oc_1yrdeath_covid_flwup_start oc_1yrdeath_covid_flwup_end oc_1yrdeath_covid_flwup_time admidate_covid_pri source_covid time_cvd_covid
drop smoking_status ethnicity date_cvd censor_date_start

tab bmi_grp, m
tab smoking_status, m

* Drop if BMI or smoking is missing
drop if bmi_grp==.
drop if smoking_status=="."

* CVD subtypes
gen asc = 1 if cond_angina_unst==1 | cond_ami==1
gen chd = 1 if cond_angina_st==1
gen hf = 1 if cond_hf==1
gen stroke_tia = 1 if cond_stroke_tia==1 | cond_stroke_is==1 | cond_stroke_nos==1 | cond_ih==1| cond_stroke_sah==1
gen pad = 1 if cond_pad==1
gen af = 1 if cond_af==1
gen other = 1 if cond_dvt_dvt==1 | cond_pe==1
foreach var in asc chd hf stroke_tia pad af other {
replace `var' = 0 if `var' ==.
}

tab asc
tab chd
tab hf
tab stroke_tia
tab pad
tab af
tab other

* Exposure: MLTC - number of conditions
* Count how many other
egen total_MLTC = rowtotal(cond_asthma cond_cancer cond_ckd cond_copd cond_dementia cond_depression cond_diabetes cond_hypertension cond_liver_disease cond_schi_bpd)
tab total_MLTC
sum total_MLTC, d

gen MLTC_number = 0 if total_MLTC==0
replace MLTC_number = 1 if total_MLTC==1
replace MLTC_number = 2 if total_MLTC==2
replace MLTC_number = 3 if total_MLTC>=3

tab MLTC_number
gen MLTC = 0 if total_MLTC==0
replace MLTC = 1 if total_MLTC>=1
tab MLTC
drop cond_ltc cond_ltc_cat cond_mltc total_MLTCcond

** OUTCOME:
gen died = 1 if oc_1yrdeath_all
replace died = 0 if died ==.
tab died

***************************************************************************************
** SURVIVAL TIME **
* Date of first COVID-19 - date_covid
sum oc_1yrdeath_all_flwup_time // ranges from 1 to 1,005 days
sum died if oc_1yrdeath_all_flwup_time <= 365
gen t1y = min(oc_1yrdeath_all_flwup_time, 365) // right censored - truncated at 1 year
sum t1y, d

***************************************************************************************
* Age at start is different from age at COVID-19 diagnosis
gen study_start = td(31jan2020), after(age_65)
format study_start %td
gen start_diff = (date_covid-study_start)/365.25, after(date_covid)
gen age_covid = age_at_start + start_diff, after(start_diff)
* redo the age groups
drop age_65 age_group age_at_start

* Age intervals
gen age_covid_group = . , after(age_covid)
replace age_covid_group = 0 if age_covid < 50
replace age_covid_group = 1 if age_covid >= 50 & age_covid < 60
replace age_covid_group = 2 if age_covid >= 60 & age_covid < 70
replace age_covid_group = 3 if age_covid >= 70 & age_covid < 80
replace age_covid_group = 4 if age_covid >= 80
label define agl 0 "18-49" 1 "50-59" 2 "60-69" 3 "70-79" 4 "80+"
label values age_covid_group agl
tab age_covid_group

* Age 65 - <65 reference
gen age_covid_65 = 0
replace age_covid_65 = 1 if age_covid >=65
*label define agegg 0 "<65" 1 "65+"
label values age_covid_65 agegg
tab age_covid_65
order age_covid_65, after(age_covid_group)

* Calendar time at start date of covid
gen calendar_time = year(date_covid), after(date_covid)
gen cal_time = 0 if calendar_time==2020, after(calendar_time)
replace cal_time = 1 if calendar_time==2021
replace cal_time = 2 if calendar_time==2022
label define cal 0 "2020" 1 "2021" 2 "2022"
label values cal_time cal
tab cal_time

** save cohort file 
