CREATE OR REPLACE VIEW h1b_split.v_Tech_Job_Salary_Comparison AS  
 SELECT j.job_id, j.worksite_county, g.state, j.soc_code, 
 j.soc_name, j.employment_start_year as year, j.employment_start_date, j.wage_rate_of_pay_from as job_salary,  
 w.annual_salary as national_salary, 
 (CASE WHEN w.annual_salary > j.wage_rate_of_pay_from THEN 'NATIONAL_SALARY_GREATER'
	   WHEN j.wage_rate_of_pay_from > w.annual_salary THEN 'JOB_SALARY_GREATER'
	   ELSE 'EQUAL_SALARY'
	   END) as salary_label,	   
(CASE WHEN w.annual_salary > j.wage_rate_of_pay_from THEN ROUND((w.annual_salary - j.wage_rate_of_pay_from), 2)
	  WHEN j.wage_rate_of_pay_from > w.annual_salary THEN ROUND((j.wage_rate_of_pay_from - w.annual_salary), 2) 
	  ELSE 0
	  END) as salary_gap
 FROM `cs327e-fa2018.h1b_split.v_Tech_Job` j JOIN `cs327e-fa2018.bureau_labor_stats.Geography` g 
     ON (j.worksite_county = g.county AND j.worksite_state = g.state AND j.employment_start_year = g.year)	 
 JOIN `cs327e-fa2018.bureau_labor_stats.All_Industries_Wages` w 
 	 ON (j.soc_code = w.soc_code and j.employment_start_year = w.year)
 WHERE g.area = w.area and g.year = w.year
 AND j.wage_rate_of_pay_from IS NOT NULL
 ORDER BY salary_gap;
  
 
 CREATE OR REPLACE VIEW h1b_split.v_Tech_Job_Salary_Gap_by_Occupation AS
  SELECT state, soc_code, soc_name, salary_label, round(AVG(salary_gap), 2) as avg_salary_gap
  FROM `cs327e-fa2018.h1b_split.v_Tech_Job_Salary_Comparison` 
  WHERE soc_name != 'ELECTRONICS ENGINEERS, EXCEPT COMPUTER'
  GROUP BY state, soc_code, soc_name, salary_label
  ORDER BY avg_salary_gap DESC;