import logging, os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class TransformEmployer(beam.DoFn):
  def process(self, element):
    employer_val = element
    
    #print('element: ' + str(element) + '\n')
    
    employer_name = employer_val.get('employer_name')
    employer_city = employer_val.get('employer_city')
    employer_address = employer_val.get('employer_address')
    employer_state = employer_val.get('employer_state')
    employer_province = employer_val.get('employer_province')
    employer_postal_code = employer_val.get('employer_postal_code')
    employer_phone = employer_val.get('employer_phone')
    employer_country = employer_val.get('employer_country')
    
    # remove punctuation and suffixes in the employer's name
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
    
    # employer address, city, state, zipcode may be in the wrong columns!
    employer_city = employer_city.replace(',', '')
    employer_city = employer_city.strip()
    employer_city_start = employer_city[0]
    
    # employer_city checks
    if employer_city.isdigit():
        # looks like a zipcode, not a city
        employer_postal_code = employer_city
        employer_city = None  
    elif employer_city_start.isdigit(): 
        # looks like an address, not a city
        employer_address = employer_city
        employer_city = None
    
    # employer_state checks
    if employer_state != None and len(employer_state) > 2:
        employer_city = employer_state
        employer_state = None
    
    # employer_postal_code checks
    if employer_postal_code != None and len(employer_postal_code) == 2 and not employer_postal_code.isdigit():
       employer_state = employer_postal_code
       employer_postal_code = None
    
    if employer_phone != None:
        employer_phone = employer_phone.replace('.0', '')
        
    if employer_country != None and employer_country.isdigit(): 
        employer_postal_code = employer_country
        employer_country = None
        
    if employer_province != None and employer_province == 'UNITED STATES OF AMERICA':
        employer_country = 'UNITED STATES OF AMERICA'
        employer_province = None
       
    # overwrite dictionary entries 
    employer_val['employer_name'] = employer_name
    
    if employer_city == None:
        # we don't have a valid city, so exit function
        return
    employer_val['employer_city'] = employer_city
    
    if employer_address == None:
        employer_val.pop('employer_address')
    else:
        employer_val['employer_address'] = employer_address
    
    if employer_state == None:
        employer_val.pop('employer_state')
    else:
        employer_val['employer_state'] = employer_state

    if employer_postal_code == None:
        employer_val.pop('employer_postal_code')
    else:
        employer_val['employer_postal_code'] = employer_postal_code

    if employer_phone == None:
        employer_val.pop('employer_phone')
    else:
        employer_val['employer_phone'] = employer_phone
    
    if employer_country == None:
        employer_val.pop('employer_country')
    else:
        employer_val['employer_country'] = employer_country
        
    if employer_province == None:
        employer_val.pop('employer_province')
    else:
        employer_val['employer_province'] = employer_province

    employer_key = {'employer_name': employer_name, 'employer_city': employer_city}    
    employer_tuple = (employer_key, employer_val)
    
    print('employer_tuple: ' + str(employer_tuple) + '\n')
    
    return [employer_tuple]

class MakeBigQueryRecord(beam.DoFn):
  def process(self, element):
    key, record_values = element
    
    record_list = list(record_values) # GroupByKey returns an '_UnwindowedValues' object, which must be cast to a list
    if len(record_list) == 0:
        return
    
    record = record_list[0]          
    if record.get('h1b_dependent') == None:
        record.pop('h1b_dependent')
    if record.get('willful_violator') == None:
        record.pop('willful_violator')

    #print('bq record: ' + str(record) + '\n')

    return [record]

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * 
                                                                                    FROM h1b_split.Employer_Temp LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to File 1' >> WriteToText('output_query_results.txt')

    # apply ParDo to the Employer records
    tuple_pcoll = query_results | 'Transform Employer' >> beam.ParDo(TransformEmployer())
    
    # write PCollection to log file
    tuple_pcoll | 'Write to File 2' >> WriteToText('output_pardo_employer_tuple.txt')
    
    deduped_pcoll = tuple_pcoll | 'Dedup Employer Records' >> beam.GroupByKey()
    
    # write PCollection to log file
    deduped_pcoll | 'Write to File 3' >> WriteToText('output_group_by_key.txt')
    
    # apply second ParDo to the PCollection 
    out_pcoll = deduped_pcoll | 'Make BigQuery Records' >> beam.ParDo(MakeBigQueryRecord())
    
    # write PCollection to log file
    out_pcoll | 'Write to File 4' >> WriteToText('output_bq_records.txt')
    
    qualified_table_name = PROJECT_ID + ':h1b_split.Employer'
    table_schema = 'employer_id:STRING,employer_name:STRING,employer_address:STRING,employer_city:STRING,employer_state:STRING,employer_postal_code:STRING,' \
                   'employer_country:STRING,employer_province:STRING,employer_phone:STRING,h1b_dependent:BOOLEAN,willful_violator:BOOLEAN'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    print('job completed.')

