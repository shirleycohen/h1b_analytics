-- Dependencies: 
-- 1) load raw h1b files as tables applications_<year> where year = 2015 - 2018 into dataset h1b_raw
-- 2) create new dataset h1b_split1
-- 3) run each create-table-as-select statement in this script to create and populate the new tables in h1b_split. 

-- Create Employer_Temp tables and assign each record a unique employer_id 
-- Table contains duplicate employer records, will need to remove duplicates through Beam
CREATE TABLE h1b_split.Employer_Temp AS
SELECT generate_uuid() as employer_id, * 
FROM 
(SELECT DISTINCT employer_name, employer_address, employer_city, employer_state, 
 employer_postal_code, employer_country, employer_province, CAST(employer_phone AS STRING) as employer_phone, 
 CAST(CASE WHEN h1b_dependent = 'N' THEN 'False'
 WHEN h1b_dependent = 'Y' THEN 'True'
 ELSE NULL END as BOOL) AS h1b_dependent,
 willful_violator 
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2018`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, employer_address, employer_city, employer_state, 
 employer_postal_code, employer_country, employer_province, employer_phone, h1b_dependent, willful_violator 
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2017`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, employer_address, employer_city, employer_state, 
 employer_postal_code, employer_country, employer_province, employer_phone, h1b_dependent, willful_violator 
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2016`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, CONCAT(employer_address1, ' ', employer_address2) as employer_address, employer_city, employer_state, 
 employer_postal_code, employer_country, employer_province, employer_phone, h1b_dependent, willful_violator 
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2015`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
)
ORDER BY employer_name, employer_city;



-- Create Job_Temp table
CREATE TABLE h1b_split.Job_Temp AS
SELECT generate_uuid() as job_id, * 
FROM 
(
 SELECT DISTINCT employer_name, employer_city, employment_start_date, employment_end_date, 
 job_title, 
 SAFE_CAST(wage_rate_of_pay_from as FLOAT64) as wage_rate_of_pay_from, wage_rate_of_pay_to, wage_unit_of_pay, 
 worksite_city, worksite_county, worksite_state, worksite_postal_code, soc_code, soc_name, total_workers, SAFE_CAST(full_time_position AS BOOL) as full_time_position, 
 SAFE_CAST(prevailing_wage as FLOAT64) as prevailing_wage, pw_unit_of_pay, pw_source, CAST(pw_source_year AS STRING) as pw_source_year, pw_source_other
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2018`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL AND employment_start_date IS NOT NULL AND employment_end_date IS NOT NULL AND job_title IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, employer_city, CAST(employment_start_date AS DATE) as employment_start_date, CAST(employment_end_date AS DATE) as employment_end_date, 
 job_title, wage_rate_of_pay_from, wage_rate_of_pay_to, wage_unit_of_pay, 
 worksite_city, worksite_county, worksite_state, worksite_postal_code, soc_code, soc_name, total_workers, full_time_position, 
 prevailing_wage, pw_unit_of_pay, pw_source, pw_source_year, pw_source_other
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2017`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL AND employment_start_date IS NOT NULL AND employment_end_date IS NOT NULL AND job_title IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, employer_city, CAST(employment_start_date AS DATE) as employment_start_date, CAST(employment_end_date AS DATE) as employment_end_date, 
  job_title, wage_rate_of_pay_from, wage_rate_of_pay_to, wage_unit_of_pay, 
  worksite_city, worksite_county, worksite_state, worksite_postal_code, soc_code, soc_name, total_workers, SAFE_CAST(full_time_position AS BOOL) as full_time_position, 
  prevailing_wage, pw_unit_of_pay, pw_wage_source as pw_source, pw_wage_source_year as pw_source_year, pw_wage_source_other as pw_source_other
 FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2016`
 WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL AND employment_start_date IS NOT NULL AND employment_end_date IS NOT NULL AND job_title IS NOT NULL
 UNION DISTINCT
 SELECT DISTINCT employer_name, employer_city, CAST(employment_start_date AS DATE) as employment_start_date, CAST(employment_end_date AS DATE) as employment_end_date, 
  job_title, SAFE_CAST(TRIM(SUBSTR(wage_rate_of_pay, 0, (STRPOS(wage_rate_of_pay, "-")-1))) AS FLOAT64) as wage_rate_of_pay_from, 
  SAFE_CAST(TRIM(SUBSTR(wage_rate_of_pay, (STRPOS(wage_rate_of_pay, "-")+1))) AS FLOAT64) as wage_rate_of_pay_to,
  wage_unit_of_pay, worksite_city, worksite_county, worksite_state, worksite_postal_code, soc_code, soc_name, total_workers, full_time_position, 
  prevailing_wage, pw_unit_of_pay, pw_source, CAST(pw_wage_source_year AS STRING) as pw_source_year, pw_source_other
  FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2015`
  WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL AND employment_start_date IS NOT NULL AND employment_end_date IS NOT NULL AND job_title IS NOT NULL
)
ORDER BY employer_name, employer_city; 


-- Create Attorney table
CREATE TABLE h1b_split.Attorney AS
SELECT generate_uuid() as attorney_id, *
FROM
(SELECT DISTINCT agent_attorney_name as attorney_name, agent_attorney_city as attorney_city, agent_attorney_state as attorney_state
FROM `cs327e-fa2018.h1b_raw.H1B_Applications_*`
WHERE agent_attorney_name IS NOT NULL AND agent_attorney_city IS NOT NULL AND agent_attorney_state IS NOT NULL
AND employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
ORDER BY attorney_name);


-- Create Application_Temp table
CREATE TABLE h1b_split.Application_Temp AS
SELECT case_number, case_status, case_submitted, decision_date, visa_class, 
employer_name, employer_city, attorney_id
FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2018` a LEFT OUTER JOIN `cs327e-fa2018.h1b_split.Attorney` t ON a.agent_attorney_name = t.attorney_name
AND a.agent_attorney_city = t.attorney_city
WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
UNION ALL
SELECT case_number, case_status, SAFE_CAST(case_submitted AS DATE) as case_submitted, SAFE_CAST(decision_date AS DATE) as decision_date, visa_class, 
employer_name, employer_city, attorney_id
FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2017` a LEFT OUTER JOIN `cs327e-fa2018.h1b_split.Attorney` t ON a.agent_attorney_name = t.attorney_name
AND a.agent_attorney_city = t.attorney_city
WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
UNION ALL
SELECT case_number, case_status, SAFE_CAST(case_submitted AS DATE) as case_submitted, SAFE_CAST(decision_date AS DATE) as decision_date, visa_class, 
employer_name, employer_city, attorney_id
FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2016` a LEFT OUTER JOIN `cs327e-fa2018.h1b_split.Attorney` t ON a.agent_attorney_name = t.attorney_name
AND a.agent_attorney_city = t.attorney_city
WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL
UNION ALL
SELECT case_number, case_status, SAFE_CAST(case_submitted AS DATE) as case_submitted, SAFE_CAST(decision_date AS DATE) as decision_date, visa_class, 
employer_name, employer_city, attorney_id
FROM `cs327e-fa2018.h1b_raw.H1B_Applications_2015` a LEFT OUTER JOIN `cs327e-fa2018.h1b_split.Attorney` t ON a.agent_attorney_name = t.attorney_name
AND a.agent_attorney_city = t.attorney_city
WHERE employer_name IS NOT NULL AND employer_name != '1' AND employer_city IS NOT NULL;






