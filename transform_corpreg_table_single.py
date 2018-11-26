import logging, os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class TransformCorpRegRecord(beam.DoFn):
  
  def process(self, element):
    
    corporation_id = element.get('corporation_id')
    corporation_name = element.get('corporation_name')
    corporation_city = element.get('corporation_city')
    corporation_state = element.get('corporation_state')
    registration_date = element.get('registration_date')
    
    # check and clean the corporation's name
    corporation_name = corporation_name.upper()
    corporation_name = corporation_name.replace('&QUOT;', '')
    corporation_name = corporation_name.replace('"', '')
    corporation_name = corporation_name.replace('\'', '')
    corporation_name = corporation_name.replace('/', '')
    corporation_name = corporation_name.replace('.', '')
    corporation_name = corporation_name.replace(',', '')
    corporation_name = corporation_name.replace('(', '')
    corporation_name = corporation_name.replace(')', '')
    corporation_name = corporation_name.replace(' INC', '')
    corporation_name = corporation_name.replace(' CORPORATION', '')
    corporation_name = corporation_name.replace(' CORP', '')
    corporation_name = corporation_name.replace(' LLC', '')
    corporation_name = corporation_name.replace(' LIMITED', '')
    corporation_name = corporation_name.replace(' LTD', '')
    corporation_name = corporation_name.replace(' LP', '')
    corporation_name = corporation_name.replace(' PC', '')
    corporation_name = corporation_name.strip()
    
    # for CT only: extract the city name from the street address 
    if corporation_state == 'CT':
        addr_splits = corporation_city.rsplit(',', 3)
        corporation_city = addr_splits[1]
    
    # check and clean the corporation's city
    if corporation_city == None:
        return
    if corporation_city != None and len(corporation_city) > 1:
        corporation_city_start = corporation_city[0]
        if corporation_city.isdigit():
            # looks like a zipcode, not a city
            corporation_city = None  
        elif corporation_city_start.isdigit(): 
            # looks like an address, not a city
            corporation_city = None
        elif corporation_city == corporation_state:
            # looks like a state, not city
            corporation_city = None
        elif len(corporation_city.split(',')) == 2:
            # looks like an address, not a city
            corporation_city = corporation_city.split(',')[0]
    
    if corporation_city == None or len(corporation_city) == 0:
        return
    
    corporation_city = corporation_city.strip()
    corporation_city = corporation_city.upper()
    
    clean_record = {'corporation_id': corporation_id, 'corporation_name': corporation_name, 'corporation_city': corporation_city, 
                'corporation_state': corporation_state, 'registration_date': registration_date} 
    
    return [clean_record]
    
    
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_str = 'SELECT corporation_id, corporation_name, corporation_city, corporation_state, registration_date ' \
                    'FROM `sec_of_state.Corporate_Registrations_Merged`'
    
    query_results = p | 'Read from BQ CorpReg' >> beam.io.Read(beam.io.BigQuerySource(query=query_str, use_standard_sql=True))

    query_results | 'Write to File 1' >> WriteToText('output_query_results.txt')
 
    clean_pcoll = query_results | 'Transform CorpReg Record' >> beam.ParDo(TransformCorpRegRecord())
    
    clean_pcoll | 'Write to File 2' >> WriteToText('output_bq_records.txt')
    
    qualified_table_name = PROJECT_ID + ':sec_of_state.Corporate_Registrations_Cleaned'
    table_schema = 'corporation_id:STRING,corporation_name:STRING,corporation_city:STRING,corporation_state:STRING,registration_date:DATE'
    
    clean_pcoll | 'Write to BQ CorpReg' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

