create table sec_of_state.Corporate_Registrations_Merged
(
	corporation_id STRING,
	corporation_name STRING,
	corporation_city STRING,
	corporation_state STRING,
	registration_date DATE,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY corporation_state;

--AZ
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct File_Number, Corporation_Name, First_Address_City, 'AZ', Date_of_Incorporation
from sec_of_state.Corporate_Registrations_AZ
where First_Address_State = 'AZ'
order by corporation_name;

--CA
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select CAST(corporation_number as STRING), corporation_name, mail_address_city, 'CA', incorporation_date
from sec_of_state.Corporate_Registrations_CA
where corporation_type = 'Articles of Incorporation' 
and mail_address_state_or_country = 'CA'
order by corporation_name;

--CO
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select CAST(entity_id as STRING), entity_name, entity_principal_city, 'CO', entity_formation_date
from sec_of_state.Corporate_Registrations_CO
where entity_principal_state = 'CO'
order by entity_name;

--CT
--city not in its own field, is contained in business address. TO DO: extract the city from the address from Beam
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select CAST(business_id as STRING), business_name, business_address, 'CT', SAFE_CAST(registration_date as DATE)
from sec_of_state.Corporate_Registrations_CT
where business_address like '%, CT, %' and registration_date is not null
order by business_name;

--GA
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select CAST(ga1.bizentityid as STRING), ga1.businessname, ga2.city, 'GA', SAFE_CAST(ga1.commencementdate as DATE)
from sec_of_state.Corporate_Registrations_GA_1 ga1 join sec_of_state.Corporate_Registrations_GA_2 ga2
on ga1.bizentityid = ga2.bizentityid
and ga2.city is not null and ga2.city != '0' and ga2.city != '1' and ga2.state = 'GA'
order by ga1.businessname;

--MA
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select FEIN, EntityName, City, 'MA', SAFE_CAST(DateOfOrganization as DATE)
from sec_of_state.Corporate_Registrations_MA
where EntityName is not null and City is not null and State = 'MA'
order by EntityName;

--MN
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct mn1.master_id, mn1.minnesota_business_name, mn2.city_name, 'MN', SAFE_CAST(mn1.filing_date as DATE)
from sec_of_state.Corporate_Registrations_MN_1 mn1 
join sec_of_state.Corporate_Registrations_MN_2 mn2 on mn1.master_id = mn2.master_id
where mn1.minnesota_business_name is not null and mn2.city_name is not null 
and mn2.region_code = 'MN' and mn1.filing_date is not null
order by mn1.minnesota_business_name;

--MO
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct CAST(mo1.corporation_id as STRING), mo1.name, mo2.city, 'MO', SAFE_CAST(mo2.date_formed as DATE) 
from sec_of_state.Corporate_Registrations_MO_1 mo1 
join sec_of_state.Corporate_Registrations_MO_2 mo2 on mo1.corporation_id = mo2.corporation_id
where mo2.state = 'MO'
order by mo1.name;

--NC
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct sosid, corpname, princity, 'NC', SAFE_CAST(dateformed as DATE)
from sec_of_state.Corporate_Registrations_NC
where corpname is not null and princity is not null and prinstate = 'NC'
order by corpname;

--NY
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct CAST(dos_id as STRING), current_entity_name, dos_process_city, 'NY', initial_dos_filing_date
from sec_of_state.Corporate_Registrations_NY
where jurisdiction = 'NEW YORK' and dos_process_city is not null and current_entity_name is not null 
and initial_dos_filing_date is not null
order by current_entity_name;

--OH
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct charter_num, business_name, business_location_name, 'OH', SAFE_CAST(effective_date_time as DATE)
from sec_of_state.Corporate_Registrations_OH
where business_state = 'OH'
order by business_name;

--VA
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct corporation_id, corporation_name, prinicipal_city, 'VA', SAFE_CAST(incorporation_date as DATE)
from sec_of_state.Corporate_Registrations_VA
where principal_state = 'VA'
order by corporation_name;

--WA
insert into sec_of_state.Corporate_Registrations_Merged (corporation_id, corporation_name, corporation_city, 
							 corporation_state, registration_date)	
select distinct CAST(UBI as STRING), BusinessName, RegisteredAgentCity, 'WA', SAFE_CAST(DateOfIncorporation as DATE)
from sec_of_state.Corporate_Registrations_WA
where StateOfIncorporation = 'WA';

create table sec_of_state.Corporate_Registrations_Cleaned
(
	corporation_id STRING,
	corporation_name STRING,
	corporation_city STRING,
	corporation_state STRING,
	registration_date DATE,
	empty_date DATE
)
PARTITION BY empty_date
CLUSTER BY corporation_state;
