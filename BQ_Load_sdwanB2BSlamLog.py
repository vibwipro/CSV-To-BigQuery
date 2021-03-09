#**************************************************************************
#Script Name 	: BQ_Load_sdwanB2BSlamLog.py
#Description 	: This script will load data in bwMonLog BQ table.
#Created by		: Vibhor Gupta
#Version	Author		Created Date 	Comments
#1.0		Vibhor		2020-09-15  	Initial version
#****************************************************************

#------------Import Lib-----------------------#
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os, sys
from apache_beam.runners.dataflow.ptransform_overrides import CreatePTransformOverride
from apache_beam.runners.dataflow.ptransform_overrides import ReadPTransformOverride
import argparse
import logging
from apache_beam.options.pipeline_options import SetupOptions

#------------Set up BQ parameters-----------------------#
# Replace with Project Id
project = 'PROJ'

#-------------Splitting Of Records----------------------#
class Transaction(beam.DoFn):
    def process(self, element):
        return [{"partition_date": element[0].split('.')[0], "C1": element[0], "applianceName": element[1].split('=')[1], "tenantName": element[2].split('=')[1], "localAccCktName": element[3].split('=')[1], "remoteAccCktName": element[4].split('=')[1], "localSiteName": element[5].split('=')[1], "remoteSiteName": element[6].split('=')[1], "fwdClass": element[7].split('=')[1], "tenantId": element[8].split('=')[1], "delay": element[9].split('=')[1], "fwdDelayVar": element[10].split('=')[1], "revDelayVar": element[11].split('=')[1], "fwdLoss": element[12].split('=')[1], "revLoss": element[13].split('=')[1], "fwdLossRatio": element[14].split('=')[1], "revLossRatio": element[15].split('=')[1], "pduLossRatio": element[16].split('=')[1], "fwdSent": element[17].split('=')[1], "revSent": element[18].split('=')[1]}]


#------------Apache Beam Code to load data over BQ Table-----------------------#
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
          '--input',
          dest='input',
          help='Input file to process.')
    parser.add_argument(
        '--pro_id',
        dest='pro_id',
        type=str,
        default='xxxxxxxxxx',
        help='project id')
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)



    #data_f = sys.argv[1]
    logging.info('***********')
    logging.info(known_args.input)
    data_loading = (
        p1
        |'Read from File' >> beam.io.ReadFromText(known_args.input,skip_header_lines=0)
        |'Spliting of Fields' >> beam.Map(lambda record: record.split(','))
        |'Clean-Data' >> beam.ParDo(Transaction())
    )


    project_id = "PROJ"
    dataset_id = 'Prod_Networking'
    table_id = known_args.pro_id
    table_schema = ('partition_date:DATETIME, C1:STRING, applianceName:STRING, tenantName:STRING, localAccCktName:STRING, remoteAccCktName:STRING, localSiteName:STRING, remoteSiteName:STRING, fwdClass:STRING, tenantId:INTEGER, delay:INTEGER, fwdDelayVar:INTEGER, revDelayVar:INTEGER, fwdLoss:INTEGER, revLoss:INTEGER, fwdLossRatio:STRING, revLossRatio:STRING, pduLossRatio:STRING, fwdSent:INTEGER, revSent:INTEGER')

    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects

    result = (
        data_loading
        | 'Write-dwanB2BSlamLog' >> beam.io.WriteToBigQuery(
                                                    table=table_id,
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))

#-------------------------------End -------------------------------------------------------------------------------------------------------------
    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  path_service_account = '/home/vibhg/PROJ-fbft436-jh4527.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()


