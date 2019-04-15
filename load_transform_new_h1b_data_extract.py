import os, datetime
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn with multiple side-outputs
class SplitFn(beam.DoFn):
    
  OUTPUT_TAG_APPLICATION = 'tag_application'
  OUTPUT_TAG_JOB = 'tag_job'
  OUTPUT_TAG_EMPLOYER = 'tag_employer'
  OUTPUT_TAG_ATTORNEY = 'tag_attorney'
  
  def process(self, element):
    values = element.strip().split('\t')

    case_number	= values[0]
    case_status	= values[1]
    case_submitted = values[2]
    decision_date = values[3]	
    visa_class = values[4]
    employment_start_date = values[5]
    employment_end_date	= values[6]
    employer_name = values[7]
    employer_business_dba = values[8]	
    employer_address = values[9]	
    employer_city = values[10]
    employer_state = values[11]
    employer_postal_code = values[12]
    employer_country = values[13]	
    employer_province = values[14]	
    employer_phone = values[15]
    employer_phone_ext = values[16]	
    agent_representing_employer	= values[17]
    agent_attorney_name	= values[18]
    agent_attorney_city	= values[19]
    agent_attorney_state = values[20]
    job_title = values[21]
    soc_code = values[22]
    soc_name = values[23]	
    naics_code = values[24]	
    total_workers = values[25]
    new_employment = values[26]
    continued_employment = values[27]
    change_previous_employment = values[28]
    new_concurrent_emp = values[29]
    change_employer	= values[30]
    amended_petition = values[31]
    full_time_position = values[32]	
    prevailing_wage	= values[33]
    pw_unit_of_pay = values[34]	
    pw_wage_level = values[35]
    pw_source = values[36]
    pw_source_year = values[37]
    pw_source_other	= values[38]
    wage_rate_of_pay_from = values[39]	
    wage_rate_of_pay_to	= values[40]
    
    if len(values) > 41:
        wage_unit_of_pay = values[41]
    else:
        wage_unit_of_pay = None
        	
    if len(values) > 42:
        h1b_dependent = values[42]
    else:
        h1b_dependent = None
        
    if len(values) > 43:
        willful_violator = values[43]
    else:
        willful_violator = None
        
    if len(values) > 44:
        support_h1b	= values[44]
    
    if len(values) > 45:
        labor_con_agree	= values[45]
    
    if len(values) > 46:
        public_disclosure_location = values[46]	
    
    if len(values) > 47:
        worksite_city = values[47]	
    else:
        worksite_city = None
    
    if len(values) > 48:    
        worksite_county	= values[48]
    else:
        worksite_county = None
    
    if len(values) > 49:
        worksite_state = values[49]
    else:
        worksite_state = None
    
    if len(values) > 50:	
        worksite_postal_code = values[50]
    else:
        worksite_postal_code = None
    
    if len(values) > 51:   	
        original_cert_date = values[51]

    # normalize employer_name
    employer_name = employer_name.replace('&QUOT;', '')
    employer_name = employer_name.replace('"', '')
    employer_name = employer_name.replace('\'', '')
    employer_name = employer_name.replace('/', '')
    employer_name = employer_name.replace('.', '')
    employer_name = employer_name.replace(',', '')
    employer_name = employer_name.replace('(', '')
    employer_name = employer_name.replace(')', '')
    employer_name = employer_name.replace(' INC', '')
    employer_name = employer_name.replace(' CORPORATION', '')
    employer_name = employer_name.replace(' CORP', '')
    employer_name = employer_name.replace(' LLC', '')
    employer_name = employer_name.replace(' LIMITED', '')
    employer_name = employer_name.replace(' LTD', '')
    employer_name = employer_name.replace(' LP', '')
    employer_name = employer_name.replace(' PC', '')
    employer_name = employer_name.strip()
    
    # make application record
    application_record = {}
    application_record['case_number'] = case_number
    application_record['case_status'] = case_status
    
    # format date fields case_submitted and decision_date
    # input format: mm/dd/yyyy, output format: yyyy-mm-dd
    case_submitted_split = case_submitted.split('/')
    month = case_submitted_split[0]
    day = case_submitted_split[1]
    year = case_submitted_split[2]
    application_record['case_submitted'] = year + '-' + month + '-' + day
    
    decision_date_split = decision_date.split('/')
    month = decision_date_split[0]
    day = decision_date_split[1]
    year = decision_date_split[2]
    application_record['decision_date'] = year + '-' + month + '-' + day
    
    application_record['visa_class'] = visa_class
    # link to employer record
    application_record['employer_name'] = employer_name
    application_record['employer_city'] = employer_city
    application_record['employer_state'] = employer_state
    # link to attorney record
    application_record['agent_representing_employer'] = agent_representing_employer
    application_record['agent_attorney_name'] = agent_attorney_name
    application_record['agent_attorney_city'] = agent_attorney_city
    application_record['agent_attorney_state'] = agent_attorney_state
    
    # make job record
    job_record = {}
    
    employment_start_date_split = employment_start_date.split('/')
    month = employment_start_date_split[0]
    day = employment_start_date_split[1]
    year = employment_start_date_split[2]
    job_record['employment_start_date'] = year + '-' + month + '-' + day
    
    employment_end_date_split = employment_end_date.split('/')
    month = employment_end_date_split[0]
    day = employment_end_date_split[1]
    year = employment_end_date_split[2]
    job_record['employment_end_date'] = year + '-' + month + '-' + day
    
    job_record['job_title'] = job_title
    job_record['soc_code'] = soc_code
    job_record['soc_name'] = soc_name
    job_record['total_workers'] = total_workers
    
    if full_time_position == 'Y' or full_time_position == 'Yes':
        job_record['full_time_position'] = True
    else:
        job_record['full_time_position'] = False
    
    if prevailing_wage != None and len(prevailing_wage) > 0:
        prevailing_wage = prevailing_wage.replace(',', '')
        try:
            prevailing_wage = float(prevailing_wage)
        except ValueError:
            print('error converting prevailing_wage to float. value: ' + prevailing_wage)
        
        job_record['prevailing_wage'] = prevailing_wage
    
    job_record['pw_unit_of_pay'] = pw_unit_of_pay
    job_record['pw_source'] = pw_source
    job_record['pw_source_year'] = pw_source_year
    job_record['pw_source_other'] = pw_source_other
    
    wage_rate_of_pay_from = wage_rate_of_pay_from.replace(',', '')
    
    try:
        wage_rate_of_pay_from = float(wage_rate_of_pay_from)
    except ValueError:
        print('error converting wage_rate_of_pay_from to float. value: ' + wage_rate_of_pay_from)
    
    job_record['wage_rate_of_pay_from'] = wage_rate_of_pay_from
    
    wage_rate_of_pay_to = wage_rate_of_pay_to.replace(',', '')
    
    try:
        wage_rate_of_pay_from = float(wage_rate_of_pay_to)
    except ValueError:
        print('error converting wage_rate_of_pay_to to float. value: ' + wage_rate_of_pay_to)
    
    job_record['wage_rate_of_pay_to'] = wage_rate_of_pay_to
    
    job_record['wage_unit_of_pay'] = wage_unit_of_pay
    
    if worksite_city != None:
        job_record['worksite_city'] = worksite_city
    
    if worksite_county != None:
        job_record['worksite_county'] = worksite_county
    
    if worksite_state != None:
        job_record['worksite_state'] = worksite_state
    
    if worksite_postal_code != None:
        job_record['worksite_postal_code'] = worksite_postal_code
     
    # link to employer record    
    job_record['employer_name'] = employer_name
    job_record['employer_city'] = employer_city
    job_record['employer_state'] = employer_state
    
    # make employer record
    employer_record = {}
    employer_record['employer_name'] = employer_name
    employer_record['employer_address'] = employer_address
    employer_record['employer_city'] = employer_city
    employer_record['employer_state'] = employer_state
    employer_record['employer_postal_code'] = employer_postal_code
    employer_record['employer_country'] = employer_country
    employer_record['employer_province'] = employer_province
    employer_record['employer_phone'] = employer_phone
    
    if h1b_dependent == 'Y' or h1b_dependent == 'Yes':
        employer_record['h1b_dependent'] = True
    else:
        employer_record['h1b_dependent'] = False
    
    if willful_violator == 'Y' or willful_violator == 'Yes':
        employer_record['willful_violator'] = True
    else:
        employer_record['willful_violator'] = False
    
    # make attorney record
    attorney_record = {}
    attorney_record['agent_representing_employer'] = agent_representing_employer
    attorney_record['agent_attorney_name'] = agent_attorney_name
    attorney_record['agent_attorney_city'] = agent_attorney_city
    attorney_record['agent_attorney_state'] = agent_attorney_state
    
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_APPLICATION, application_record)  
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_JOB, job_record)  
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_EMPLOYER, employer_record) 
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_ATTORNEY, attorney_record) 
    

PROJECT_ID = os.environ['PROJECT_ID']

options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using the local runner 
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read File' >> ReadFromText('H-1B_Disclosure_Data_FY2019.tsv', skip_header_lines=1)

    # apply a ParDo to the PCollection 
    out_pcoll = in_pcoll | 'Extract Actor and Actress' >> beam.ParDo(SplitFn()).with_outputs(
                                                          SplitFn.OUTPUT_TAG_APPLICATION,
                                                          SplitFn.OUTPUT_TAG_JOB,
                                                          SplitFn.OUTPUT_TAG_EMPLOYER,
                                                          SplitFn.OUTPUT_TAG_ATTORNEY)
                                                          
    application_pcoll = out_pcoll[SplitFn.OUTPUT_TAG_APPLICATION]
    job_pcoll = out_pcoll[SplitFn.OUTPUT_TAG_JOB]
    employer_pcoll = out_pcoll[SplitFn.OUTPUT_TAG_EMPLOYER]
    attorney_pcoll = out_pcoll[SplitFn.OUTPUT_TAG_ATTORNEY]

    # write PCollections to files
    application_pcoll | 'Write Application File' >> WriteToText('application_log.out')
    job_pcoll | 'Write Job File' >> WriteToText('job_log.out')
    employer_pcoll | 'Write Employer File' >> WriteToText('employer_log.out')
    attorney_pcoll | 'Write Attorney File' >> WriteToText('attorney_log.out')
    
    application_table_name = PROJECT_ID + ':h1b_split.New_Application'
    job_table_name = PROJECT_ID + ':h1b_split.New_Job'
    employer_table_name = PROJECT_ID + ':h1b_split.New_Employer'
    attorney_table_name = PROJECT_ID + ':h1b_split.New_Attorney'
    
    application_table_schema = 'case_number:STRING,case_status:STRING,case_submitted:DATE,decision_date:DATE,visa_class:STRING,' \
                               'employer_name:STRING,employer_city:STRING,employer_state:STRING,agent_representing_employer:STRING,' \
                               'agent_attorney_name:STRING,agent_attorney_city:STRING,agent_attorney_state:STRING'
                               
      
    job_table_schema = 'employment_start_date:DATE,employment_end_date:DATE,job_title:STRING,soc_code:STRING,soc_name:STRING,total_workers:INTEGER,' \
                       'full_time_position:BOOLEAN,prevailing_wage:FLOAT,pw_unit_of_pay:STRING,pw_source:STRING,pw_source_year:STRING,' \
                       'pw_source_other:STRING,wage_rate_of_pay_from:FLOAT,wage_rate_of_pay_to:FLOAT,wage_unit_of_pay:STRING,worksite_city:STRING,' \
                       'worksite_county:STRING,worksite_state:STRING,worksite_postal_code:STRING,employer_name:STRING,employer_city:STRING,employer_state:STRING'

    
    employer_table_schema = 'employer_name:STRING,employer_address:STRING,employer_city:STRING,employer_state:STRING,employer_postal_code:STRING,' \
                            'employer_country:STRING,employer_province:STRING,employer_phone:STRING,h1b_dependent:BOOLEAN,willful_violator:BOOLEAN' 
                           
    
    attorney_table_schema = 'agent_representing_employer:STRING,agent_attorney_name:STRING,agent_attorney_city:STRING,agent_attorney_state:STRING'

        
    # write PCollections to BQ tables
   
    application_pcoll | 'Write Application Table' >> beam.io.Write(beam.io.BigQuerySink(application_table_name, 
                                                    schema=application_table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
                                                  
    job_pcoll | 'Write Job Table' >> beam.io.Write(beam.io.BigQuerySink(job_table_name, 
                                                    schema=job_table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
   
                                                     
    employer_pcoll | 'Write Employer Table' >> beam.io.Write(beam.io.BigQuerySink(employer_table_name, 
                                                    schema=employer_table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
                                                    
    attorney_pcoll | 'Write Attorney Table' >> beam.io.Write(beam.io.BigQuerySink(attorney_table_name, 
                                                    schema=attorney_table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

