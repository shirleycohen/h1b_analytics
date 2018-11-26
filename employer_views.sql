CREATE VIEW v_Tech_Employer_Age AS
SELECT DISTINCT e.employer_name, e.employer_state, cr.registration_date,
       DATE_DIFF(CURRENT_DATE(), cr.registration_date, YEAR) AS employer_age
FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_13_States` e
JOIN `cs327e-fa2018.sec_of_state.Corporate_Registrations_Cleaned` cr
ON e.employer_name = cr.corporation_name AND e.employer_state = cr.corporation_state
ORDER BY
  e.employer_name,
  e.employer_state;
  

CREATE VIEW v_Tech_Employer_Age_Label AS
SELECT *,
    CASE
      WHEN employer_age = 0 THEN 'Baby (0)'
      WHEN employer_age BETWEEN 1
    AND 2 THEN 'Toddler (1-2)'
      WHEN employer_age BETWEEN 3 AND 12 THEN 'Child (3-12)'
      WHEN employer_age BETWEEN 13
    AND 17 THEN 'Teenager (13-17)'
      WHEN employer_age >= 18 THEN 'Grownup (18+)'
      ELSE NULL
    END AS age_label
FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_Age`;



CREATE VIEW v_Tech_Employer_Age_Label_report AS
SELECT age_label, employer_state, COUNT(*) AS employer_count
FROM `cs327e-fa2018.h1b_split.v_Tech_Employer_Age_Label`
WHERE age_label IS NOT NULL
GROUP BY age_label, employer_state
ORDER BY age_label, employer_state;