import logging, os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class TransformEmployerRecord(beam.DoFn):
  def process(self, element):
    employer_record = element
    
    employer_id = employer_record.get('employer_id')
    employer_name = employer_record.get('employer_name')
    employer_city = employer_record.get('employer_city')
        
    employer_key = {'employer_name': employer_name, 'employer_city': employer_city}
    employer_val = {'employer_id': employer_id}    
    employer_tuple = (employer_key, employer_val)
    
    return [employer_tuple]

class TransformJobRecord(beam.DoFn):
  def process(self, element):
    job_record = element
    
    employer_name = job_record.get('employer_name')
    employer_city = job_record.get('employer_city').strip()
    
    # remove punctuation and suffixes in the employer's name
    employer_name = employer_name.replace('&QUOT;', '')
    employer_name = employer_name.replace('"', '')
    employer_name = employer_name.replace('\'', '')
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
    
    # overwrite dictionary entries 
    job_record['employer_name'] = employer_name
    job_record['employer_city'] = employer_city

    application_key = {'employer_name': employer_name, 'employer_city': employer_city}    
    job_tuple = (application_key, job_record)
    
    return [job_tuple]

class MakeBigQueryRecord(beam.DoFn):
  def process(self, element):
    key, records = element

    number_records = len(records)
    if number_records == 2:
        job_matches = records[0] # there can be multiple jobs per employer
        emp_match = records[1]   # there is always a single employer per job 
        if len(job_matches) == 0 or len(emp_match) == 0:
            return
        
        # we have an inner join
        emp_record = emp_match[0]
        employer_id = emp_record.get('employer_id')
        job_results = []
        for job_record in job_matches:
            job_record['employer_id'] = employer_id
 
            if job_record['wage_rate_of_pay_to'] == None:
                job_record.pop('wage_rate_of_pay_to')           
            if job_record['full_time_position'] == None:
                job_record.pop('full_time_position')
            if job_record['pw_source_year'] == None:
                job_record.pop('pw_source_year')
            if job_record['pw_source_other'] == None:
                job_record.pop('pw_source_other')
                
            job_record.pop('employer_name')
            job_record.pop('employer_city')
            
            job_results.append(job_record)

        return job_results

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-application-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-8',
    'num_workers': 8
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    job_query_results = p | 'Read from BigQuery Job' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM h1b_split.Job_Temp order by employer_name'))
    emp_query_results = p | 'Read from BigQuery Employer' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT employer_id, employer_name, employer_city FROM ' \
                                                                                                       'h1b_split.Employer order by employer_name'))
    # apply ParDo to the Job records 
    job_tuple_pcoll = job_query_results | 'Transform Job Record' >> beam.ParDo(TransformJobRecord())
    emp_tuple_pcoll = emp_query_results | 'Transform Employer Record' >> beam.ParDo(TransformEmployerRecord())
    
    job_tuple_pcoll | 'Write to File 1' >> WriteToText(DIR_PATH + 'output_job_tuples.txt')
    emp_tuple_pcoll | 'Write to File 2' >> WriteToText(DIR_PATH + 'output_emp_tuples.txt')

    # Join Job and Employer on employer_name, employer_city 
    joined_pcoll = (job_tuple_pcoll, emp_tuple_pcoll) | 'Join Job and Employer' >> beam.CoGroupByKey()
    joined_pcoll | 'Write to File 3' >> WriteToText(DIR_PATH + 'output_joined_pcoll.txt')
    
    job_bq_pcoll = joined_pcoll | 'Transform to BigQuery Record' >> beam.ParDo(MakeBigQueryRecord())
    job_bq_pcoll | 'Write to File 4' >> WriteToText(DIR_PATH + 'output_bq_record.txt')
    
    qualified_table_name = PROJECT_ID + ':h1b_split.Job'
    table_schema = 'job_id:STRING,employer_id:STRING,employment_start_date:DATE,employment_end_date:DATE,job_title:STRING,'\
                    'wage_rate_of_pay_from:FLOAT,wage_rate_of_pay_to:FLOAT,wage_unit_of_pay:STRING,worksite_city:STRING,worksite_county:STRING,'\
                    'worksite_state:STRING,worksite_postal_code:STRING,soc_code:STRING,soc_name:STRING,total_workers:INTEGER,full_time_position:BOOLEAN,'\
                    'prevailing_wage:FLOAT,pw_unit_of_pay:STRING,pw_source:STRING,pw_source_year:STRING,pw_source_other:STRING'
    
    job_bq_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
