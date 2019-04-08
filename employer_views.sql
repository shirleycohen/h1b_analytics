CREATE OR REPLACE VIEW h1b_split.v_Tech_Job AS
 SELECT *
 FROM cs327e-fa2018.h1b_split.Job
 WHERE (soc_name like '%COMPUTER%' or soc_name like '%SOFTWARE%' or soc_name like '%DEVELOPER%' 
        or soc_name like '%NETWORK%' or soc_name like '%DATABASE%' or soc_name like '%DATA%' 
        or soc_name like '%APPLICATIONS%' or soc_name like '%TECHNOLOGY%' or soc_name like '% TECH %'  
        or soc_name like '%APPS%' or soc_name like '%PROGRAMMER%' or soc_name like '%SOLUTION ARCHITECT%') 
        and soc_name != 'ELECTRONICS ENGINEERS, EXCEPT COMPUTER';
 
 
CREATE OR REPLACE VIEW h1b_split.v_Tech_Employer_13_States AS
 SELECT DISTINCT e.* 
 FROM `cs327e-fa2018.h1b_split.Employer` e JOIN `cs327e-fa2018.h1b_split.v_Tech_Job` tj on e.employer_id = tj.employer_id  
 WHERE employer_state in 
 ('AZ', 'CA', 'CO', 'CT', 'GA', 'MA', 'MN', 'MO', 'NC', 'NY', 'OH', 'VA', 'WA');


CREATE OR REPLACE VIEW h1b_split.v_Tech_Employer_Age AS
 SELECT DISTINCT e.employer_name, e.employer_state, cr.registration_date,
       DATE_DIFF(CURRENT_DATE(), cr.registration_date, YEAR) AS employer_age
 FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_13_States` e
 JOIN `cs327e-fa2018.sec_of_state.Corporate_Registrations_Cleaned` cr
 ON e.employer_name = cr.corporation_name AND e.employer_state = cr.corporation_state
 ORDER BY e.employer_name, e.employer_state;
  
  
CREATE OR REPLACE VIEW h1b_split.v_Tech_Employer_Age_Label AS
 SELECT *,
    CASE
      WHEN employer_age = 0 THEN 'Age 0'
      WHEN employer_age BETWEEN 1
    AND 2 THEN 'Ages 1-2'
      WHEN employer_age BETWEEN 3 AND 12 THEN 'Ages 3-12'
      WHEN employer_age BETWEEN 13
    AND 17 THEN 'Ages 13-17'
      WHEN employer_age >= 18 THEN 'Ages 18+'
      ELSE NULL
    END AS age_label
 FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_Age`;


CREATE OR REPLACE VIEW h1b_split.v_Tech_Employer_Age_Label_report AS
 SELECT age_label, employer_state, COUNT(*) AS employer_count
 FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_Age_Label`
 WHERE age_label IS NOT NULL
 GROUP BY age_label, employer_state
 ORDER BY age_label, employer_state;
