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

class TransformApplicationRecord(beam.DoFn):
  def process(self, element):
    application_record = element
    
    employer_name = application_record.get('employer_name')
    employer_city = application_record.get('employer_city').strip()
    employer_state = application_record.get('employer_state')
    
    # clean employer name
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
    
    # clean employer city
    employer_city = employer_city.replace(',', '')
    employer_city = employer_city.strip()
    employer_city_start = employer_city[0]
    if employer_city.isdigit():
        # looks like a zipcode, not a city
        employer_city = None  
    elif employer_city_start.isdigit(): 
        # looks like an address, not a city
        employer_city = None
    
    if employer_city == None and employer_state != None and len(employer_state) > 2:
        employer_city = employer_state
    
    if employer_city == None:
        return
    
    # overwrite dictionary entries 
    application_record['employer_name'] = employer_name
    application_record['employer_city'] = employer_city    

    application_key = {'employer_name': employer_name, 'employer_city': employer_city}    
    application_tuple = (application_key, application_record)
    
    return [application_tuple]

class MakeBigQueryRecord(beam.DoFn):
  def process(self, element):
    key, records = element

    number_records = len(records)
    if number_records == 2:
        app_matches = records[0] # there may be multiple applications per employer
        emp_match = records[1] # there is a single employer per application 
        if len(app_matches) == 0 or len(emp_match) == 0:
            return
        
        # we have an inner join
        emp_record = emp_match[0]
        employer_id = emp_record.get('employer_id')
        app_results = []
        for app_record in app_matches:
            app_record['employer_id'] = employer_id
            
            if app_record['attorney_id'] == None:
                app_record.pop('attorney_id')
            app_record.pop('employer_name')
            app_record.pop('employer_city')
            app_record.pop('employer_state')
            
            app_results.append(app_record)

        return app_results

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
    
    app_query_results = p | 'Read from BigQuery Application' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM h1b_split.Application_Temp ' \
                                                                                                          'ORDER BY employer_name'))
    emp_query_results = p | 'Read from BigQuery Employer' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT employer_id, employer_name, employer_city FROM ' \
                                                                                                        'h1b_split.Employer ORDER BY employer_name'))
    # apply ParDo to the Application records 
    app_tuple_pcoll = app_query_results | 'Transform Application Record' >> beam.ParDo(TransformApplicationRecord())
    emp_tuple_pcoll = emp_query_results | 'Transform Employer Record' >> beam.ParDo(TransformEmployerRecord())
    
    app_tuple_pcoll | 'Write to File 1' >> WriteToText(DIR_PATH + 'output_app_tuples.txt')
    emp_tuple_pcoll | 'Write to File 2' >> WriteToText(DIR_PATH + 'output_emp_tuples.txt')

    # Join Application and Employer on employer_name, employer_city 
    joined_pcoll = (app_tuple_pcoll, emp_tuple_pcoll) | 'Join Application and Employer' >> beam.CoGroupByKey()
    joined_pcoll | 'Write to File 3' >> WriteToText(DIR_PATH + 'output_joined_pcoll.txt')
    
    app_bq_pcoll = joined_pcoll | 'Transform to BigQuery Record' >> beam.ParDo(MakeBigQueryRecord())
    app_bq_pcoll | 'Write to File 4' >> WriteToText(DIR_PATH + 'output_bq_record.txt')
    
    qualified_table_name = PROJECT_ID + ':h1b_split.Application'
    table_schema = 'case_number:STRING,case_status:STRING,case_submitted:DATE,decision_date:DATE,visa_class:STRING,employer_id:STRING,attorney_id:STRING'
    
    app_bq_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

