create table bureau_labor_stats.All_Industries_Wages
(
	area INT64,
	year INT64,
	soc_code STRING,
	hourly_wage FLOAT64,
	annual_salary FLOAT64,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY year;

insert into bureau_labor_stats.All_Industries_Wages (area, year, soc_code, hourly_wage, annual_salary) 
select Area, 2015, SocCode, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2015;
	
insert into bureau_labor_stats.All_Industries_Wages (area, year, soc_code, hourly_wage, annual_salary) 
select Area, 2016, SocCode, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2016;

insert into bureau_labor_stats.All_Industries_Wages (area, year, soc_code, hourly_wage, annual_salary) 
select Area, 2017, SocCode, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2017;

insert into bureau_labor_stats.All_Industries_Wages (area, year, soc_code, hourly_wage, annual_salary) 
select Area, 2018, SocCode, Average, ((Average*8)*365) 
from bureau_labor_stats.All_Industries_Wages_2018;
	

create table bureau_labor_stats.Geography
(
	area INT64,
	year INT64,
	state STRING,
	county STRING,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY year;
	
insert into bureau_labor_stats.Geography (area, year, state, county) 
select Area, 2015, StateAb, CountyTownName
from bureau_labor_stats.Geography_2015;

insert into bureau_labor_stats.Geography (area, year, state, county) 
select Area, 2016, StateAb, CountyTownName
from bureau_labor_stats.Geography_2016;

insert into bureau_labor_stats.Geography (area, year, state, county) 
select Area, 2017, StateAb, CountyTownName
from bureau_labor_stats.Geography_2017;

insert into bureau_labor_stats.Geography (area, year, state, county) 
select Area, 2018, StateAb, CountyTownName
from bureau_labor_stats.Geography_2018;



	

