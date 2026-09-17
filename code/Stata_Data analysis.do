** CCU010: DATA ANALYSIS **
* Prevalence CVD 16 phenotypes - whole popululation
cls
foreach var in cond_chd cond_angina_st cond_af cond_hf cond_ami cond_stroke_tia cond_stroke_is cond_pad cond_stroke_nos cond_dvt_dvt cond_pe cond_angina_unst cond_aaa cond_ih cond_stroke_sah cond_cardiac_arrest {
tab `var', sort
}
************************************************************************************
* Prevalence CVD subtypes overall
cls
foreach var in chd af stroke_tia hf asc other pad {
tab `var', sort
}
* Prevalence CVD subtypes overall and by MLTC
cls
tab MLTC_number
foreach var in chd af stroke_tia hf asc other pad {
tab `var' MLTC_number, row
}
************************************************************************************
* Row percentage
cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab chd `var' , row
}
cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab af `var' , row
}
cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab stroke_tia `var' , row
}
cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab hf `var' , row
}

cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab asc `var' , row
}
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab other `var' , row
}
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab pad `var' , row
}
************************************************************************************
cls
* Baseline characteristics of patients table
tab MLTC_number
* Age at start is different from age at COVID-19 diagnosis
hist age_covid
sum age_covid, d
bysort MLTC_number: sum age_covid, d
tab age_covid_group
tab age_covid_group MLTC_number, col
tab sex
tab sex MLTC_number, col
tab ethn
tab ethn MLTC_number, col
tab imd
tab imd MLTC_number, col
sum bmi_value
bysort MLTC_number: sum bmi_value
tab bmi_grp, m
tab bmi_grp MLTC_number, col
tab smok
tab smok MLTC_number, col
tab cal_time
tab cal_time MLTC_number, col
* Prevalence MLTC - whole population
cls
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
tab `var', sort
}
foreach var in cond_hypertension cond_depression cond_ckd cond_diabetes cond_cancer cond_asthma cond_copd cond_dementia cond_schi_bpd cond_liver_disease {
bysort MLTC_number: tab `var'
}
***********************************************************************************
* Outcome
tab died MLTC_number, col
***********************************************************************************
* Survival analysis
stset t1y, failure(died) id(person_id)
sts graph, by(MLTC_number) xlab(0(30)370, grid) ylab(0.75(.1)1) ci ///
legend(order (2 "No LTC" 1 "95% CI" 4 "1 LTC" 3 "95% CI" 6 "2 LTC" 5 "95% CI" 8 "+3 LTC" 7 "95% CI") pos(5) ring(0) row(2)) ///
xtitle("Time from positive COVID-19, days") ytitle("Proportion") graphregion(fcolor(white))
* Mortality rates
strate MLTC_number, per(1000)
stptime, per(1000) by(MLTC_number)
*** MAIN ANALYSIS
** Natural cubic spline of age with 4 degrees of freedom
* For age and BMI as they are non-linear
rcsgen age_covid, gen(rs_age_covid) df(4)
rcsgen bmi_value, gen(rs_bmi_value) df(4)
stpm2 i.MLTC_number rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store main_effects_model
*-------------------------------------------------------------------------------
* Sensitivity analysis - include calendar time
stpm2 i.MLTC_number rs_age_covid* i.ethn i.sex i.imd i.cal_time rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
*-------------------------------------------------------------------------------
* BY CVD SUBTYPES **
cls
stptime if chd==1, per(1000) by(MLTC_number)
tab died MLTC_number if chd==1, col
stptime if af==1, per(1000) by(MLTC_number)
tab died MLTC_number if af==1, col
stptime if stroke_tia==1, per(1000) by(MLTC_number)
tab died MLTC_number if stroke_tia==1, col
stptime if hf==1, per(1000) by(MLTC_number)
tab died MLTC_number if hf==1, col
stptime if asc==1, per(1000) by(MLTC_number)
tab died MLTC_number if asc==1, col
stptime if other==1, per(1000) by(MLTC_number)
tab died MLTC_number if other==1, col
stptime if pad==1, per(1000) by(MLTC_number)
tab died MLTC_number if pad==1, col
** MAIN MODEL **
cls
* Model + CVD subtypes
stpm2 i.MLTC_number i.chd i.af i.stroke_tia i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store main_effects_model
* By CVD subtypes
** CHD
stpm2 i.MLTC_number i.af i.stroke_tia i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if chd==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.chd i.af i.stroke_tia i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_CHD_effects_model
lrtest main_effects_model int_CHD_effects_model
** AF
stpm2 i.MLTC_number i.chd i.stroke_tia i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if af==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.af i.chd i.stroke_tia i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_AF_effects_model
lrtest main_effects_model int_AF_effects_model
** stroke_tia
cls
stpm2 i.MLTC_number i.chd i.af i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if stroke_tia==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.stroke_tia i.chd i.af i.hf i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_stroke_effects_model
lrtest main_effects_model int_stroke_effects_model
** hf
cls
stpm2 i.MLTC_number i.chd i.af i.stroke_tia i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if hf==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.hf i.chd i.af i.stroke_tia i.asc i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_hf_effects_model
lrtest main_effects_model int_hf_effects_model
** asc
cls
stpm2 i.MLTC_number i.chd i.af i.hf i.stroke_tia i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if asc==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.asc i.chd i.af i.hf i.stroke_tia i.other i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_asc_effects_model
lrtest main_effects_model int_asc_effects_model
** other
cls
stpm2 i.MLTC_number i.chd i.af i.hf i.stroke_tia i.asc i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if other==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.other i.chd i.af i.hf i.stroke_tia i.asc i.pad rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_other_effects_model
lrtest main_effects_model int_other_effects_model
** pad
cls
stpm2 i.MLTC_number i.chd i.af i.hf i.stroke_tia i.asc i.other rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok if pad==1, scale(hazard) df(4) eform baselevels
stpm2 i.MLTC_number##i.pad i.chd i.af i.hf i.stroke_tia i.asc i.other rs_age_covid* i.ethn i.sex i.imd rs_bmi_value* i.smok, scale(hazard) df(4) eform baselevels
estimates store int_pad_effects_model
lrtest main_effects_model int_pad_effects_model

* Forest plot
use "HR_CVD_subtypes.dta", clear
label var adjustedhr95ci "Hazard Ratio (95% CI)"
label var factor "CVD subtypes"
label var pvalueint "P-value for interaction"
foreach var of varlist  hr lci uci {
	gen ln`var' = ln(`var')
}
gen seln = (lnuci-lnlci)/3.92
replace lnhr   = 1   if adjustedhr95ci=="Reference"   
replace seln   = 1  if adjustedhr95ci=="Reference"     
replace lnlci   = 0   if adjustedhr95ci=="Reference"   
replace lnuci   = 0  if adjustedhr95ci=="Reference"  
metan 	lnhr seln, eform nooverall ///
		lcols(factor) rcols(adjustedhr95ci pvalueint) by(group)  textsize(120) ///
		nowt nosubgroup nostats nohet ///
		xlabel(0.6, 0.8, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7) force ///
	    favours("{bf:← Lower mortality}" #   "{bf:Higher mortality →}") ///
		graphregion(fcolor(white)) ///
		pointopt(mcolor(black) msize(vsmall) mlcolor( black) mlstyle(foreground) msymbol(S))	///
	     notable 
** END



















