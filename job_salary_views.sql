CREATE OR REPLACE VIEW h1b_split.v_Tech_Job AS
 SELECT *
 FROM (SELECT * FROM `cs327e-fa2018.h1b_split.Job` WHERE soc_name != 'ELECTRONICS ENGINEERS, EXCEPT COMPUTER')
 WHERE soc_name like '%COMPUTER%' or soc_name like '%SOFTWARE%' or soc_name like '%DEVELOPER%' or soc_name like '%NETWORK%' or soc_name like '%DATABASE%' or soc_name like '%DATA%' or soc_name like '%APPLICATIONS%' or soc_name like '%TECHNOLOGY%' or soc_name like '% TECH %'  or soc_name like '%APPS%' or soc_name like '%PROGRAMMER%' or soc_name like '%SOLUTION ARCHITECT%'
 ORDER BY soc_code;
 

CREATE OR REPLACE VIEW h1b_split.v_Tech_Job_Salary_Comparison AS  
 SELECT j.job_id, j.worksite_county, g.state, j.soc_code, 
 j.soc_name, j.employment_start_year as year, j.employment_start_date, j.wage_rate_of_pay_from as h1b_job_salary,  
 w.annual_salary as national_average_salary, 
 (CASE WHEN w.annual_salary > j.wage_rate_of_pay_from THEN 'NATIONAL_SALARY_GREATER'
	   WHEN j.wage_rate_of_pay_from > w.annual_salary THEN 'JOB_SALARY_GREATER'
	   ELSE 'EQUAL_SALARY'
	   END) as salary_label,	   
(CASE WHEN w.annual_salary > j.wage_rate_of_pay_from THEN ROUND((w.annual_salary - j.wage_rate_of_pay_from), 2)
	  WHEN j.wage_rate_of_pay_from > w.annual_salary THEN ROUND((j.wage_rate_of_pay_from - w.annual_salary), 2) 
	  ELSE 0
	  END) as salary_delta
 FROM `cs327e-fa2018.h1b_split.v_Tech_Job` j JOIN `cs327e-fa2018.bureau_labor_stats.Geography` g 
     ON (j.worksite_county = g.county AND j.worksite_state = g.state AND j.employment_start_year = g.year)	 
 JOIN `cs327e-fa2018.bureau_labor_stats.All_Industries_Wages` w 
 	 ON (j.soc_code = w.soc_code and j.employment_start_year = w.year)
 WHERE g.area = w.area and g.year = w.year
 AND j.wage_rate_of_pay_from IS NOT NULL
 ORDER BY salary_delta DESC;
  
 
 CREATE OR REPLACE VIEW h1b_split.v_Tech_Job_Salary_Delta_by_Occupation AS
   SELECT soc_code, soc_name, ROUND(AVG(h1b_job_salary), 2) as h1b_job_pay, ROUND(AVG(national_average_salary), 2) as national_average_pay, ROUND(AVG(h1b_job_salary) - AVG(national_average_salary), 2) as h1b_minus_national_delta
   FROM `cs327e-fa2018.h1b_split.v_Tech_Job_Salary_Comparison` 
   GROUP BY soc_code, soc_name
   ORDER BY h1b_minus_national_delta DESC;
   
