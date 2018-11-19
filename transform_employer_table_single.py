import logging, os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class TransformEmployerName(beam.DoFn):
  def process(self, element):
    employer_val = element
    
    employer_name = employer_val.get('employer_name')
    employer_city = employer_val.get('employer_city').strip()
    
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
    employer_val['employer_name'] = employer_name
    employer_val['employer_city'] = employer_city

    employer_key = {'employer_name': employer_name, 'employer_city': employer_city}    
    employer_tuple = (employer_key, employer_val)
    
    return [employer_tuple]

class MakeRecord(beam.DoFn):
  def process(self, element):
    key, record_list = element
    print('record_list: ' + str(record_list) + '\n')
    
    for record in record_list:
        break
    print ('raw record: ' + str(record) + '\n')
    
    if record.get('employer_address') == None:
        record.pop('employer_address')
    if record.get('employer_state') == None:
        record.pop('employer_state')
    if record.get('employer_province') == None:
        record.pop('employer_province')
    if record.get('employer_postal_code') == None:
        record.pop('employer_postal_code')
    if record.get('employer_country') == None:
        record.pop('employer_country')
            
    if record.get('employer_phone') == None:
        record.pop('employer_phone')
    else:
        employer_phone = record.get('employer_phone')
        employer_phone = employer_phone.replace('.0', '')
        record['employer_phone'] = employer_phone
                
    if record.get('h1b_dependent') == None:
        record.pop('h1b_dependent')
    if record.get('willful_violator') == None:
        record.pop('willful_violator')

    print('output record: ' + str(record) + '\n')

    return [record]

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM h1b_split.Employer WHERE employer_name != \'1\' ORDER BY employer_name limit 100'))

    # write PCollection to log file
    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    # apply ParDo to the Employer records
    tuple_pcoll = query_results | 'Transform Employer Name' >> beam.ParDo(TransformEmployerName())
    
    # write PCollection to log file
    tuple_pcoll | 'Write to File 2' >> WriteToText('output_pardo_employer_tuple.txt')
    
    deduped_pcoll = tuple_pcoll | 'Dedup Employer Records' >> beam.GroupByKey()
    
    # write PCollection to log file
    deduped_pcoll | 'Write to File 3' >> WriteToText('output_group_by_key.txt')
    
    # apply second ParDo to the PCollection 
    out_pcoll = deduped_pcoll | 'Create Employer Record' >> beam.ParDo(MakeRecord())
    
    # write PCollection to log file
    out_pcoll | 'Write to File 4' >> WriteToText('output_pardo_employer_record.txt')
    
    qualified_table_name = PROJECT_ID + ':h1b_split.Employer'
    table_schema = 'employer_id:STRING,employer_name:STRING,employer_address:STRING,employer_city:STRING,employer_state:STRING,employer_postal_code:STRING,employer_country:STRING,employer_province:STRING,employer_phone:STRING,h1b_dependent:BOOLEAN,willful_violator:BOOLEAN'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)