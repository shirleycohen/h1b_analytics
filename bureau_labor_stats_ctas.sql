create table bureau_labor_stats.All_Industries_Wages
(
	area INT64,
	soc_code STRING,
	year INT64,
	hourly_wage FLOAT64,
	annual_salary FLOAT64,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY year;

insert into bureau_labor_stats.All_Industries_Wages (area, soc_code, year, hourly_wage, annual_salary) 
select Area, SocCode, 2015, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2015;
	
insert into bureau_labor_stats.All_Industries_Wages (area, soc_code, year, hourly_wage, annual_salary) 
select Area, SocCode, 2016, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2016;

insert into bureau_labor_stats.All_Industries_Wages (area, soc_code, year, hourly_wage, annual_salary) 
select Area, SocCode, 2017, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2017;

insert into bureau_labor_stats.All_Industries_Wages (area, soc_code, year, hourly_wage, annual_salary) 
select Area, SocCode, 2018, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2018;
	

create table bureau_labor_stats.Geography
(
	area INT64,
	state STRING,
	county STRING,
	year INT64,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY year;
	
insert into bureau_labor_stats.Geography (area, state, county, year) 
select Area, StateAb, CountyTownName, 2015
from bureau_labor_stats.Geography_2015;

insert into bureau_labor_stats.Geography (area, state, county, year) 
select Area, StateAb, CountyTownName, 2016
from bureau_labor_stats.Geography_2016;

insert into bureau_labor_stats.Geography (area, state, county, year) 
select Area, StateAb, CountyTownName, 2017
from bureau_labor_stats.Geography_2017;

insert into bureau_labor_stats.Geography (area, state, county, year) 
select Area, StateAb, CountyTownName, 2018
from bureau_labor_stats.Geography_2018;



	

