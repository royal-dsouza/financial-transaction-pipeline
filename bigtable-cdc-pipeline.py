"""
Dataflow Pipeline: Bigtable CDC to Pub/Sub

This Apache Beam pipeline reads CDC events from Bigtable and publishes them to 
a Pub/Sub topic with an Avro schema attached.
"""

import argparse
import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.bigtable import ReadFromBigtableCDC
from apache_beam.io.gcp.pubsub import WriteToPubSub
from google.cloud import pubsub_v1
from google.cloud import schema
import avro.schema
from avro.io import DatumWriter
import io
import fastavro

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# class FormatTransactionRecord(beam.DoFn):
#     """Format transaction record from Bigtable CDC event for Pub/Sub."""
    
#     def process(self, element):
#         """
#         Process a Bigtable CDC event and prepare it for Pub/Sub.
        
#         Args:
#             element: A Bigtable CDC event containing row data
            
#         Returns:
#             A tuple of (Pub/Sub message, attributes)
#         """
#         try:
#             # Extract transaction data from the CDC event
#             # Structure of element depends on Bigtable CDC format
#             row_key = element['row_key'].decode('utf-8')
#             mutation_type = element['mutation_type']  # INSERT, UPDATE, DELETE
#             timestamp = element['timestamp']
            
#             # Extract actual transaction data from cells
#             transaction_data = {}
            
#             # Extract data from the latest column family
#             if 'cells' in element and 'cf_latest' in element['cells']:
#                 latest_cells = element['cells']['cf_latest']
#                 for column, value in latest_cells.items():
#                     if column.startswith(b'data:'):
#                         # Extract field name from column (format is data:field_name)
#                         field_name = column.decode('utf-8').split(':', 1)[1]
#                         # Use the latest value (values are timestamped)
#                         if value and len(value) > 0:
#                             transaction_data[field_name] = value[0]['value'].decode('utf-8')
            
#             # Create a transaction update message
#             transaction_update = {
#                 'transaction_id': row_key,
#                 'mutation_type': mutation_type,
#                 'timestamp': timestamp,
#                 'data': transaction_data
#             }
            
#             # Serialize using Avro schema
#             serialized_message = self.serialize_message(transaction_update)
            
#             # Add attributes for any metadata
#             attributes = {
#                 'mutation_type': mutation_type,
#                 'timestamp': str(timestamp)
#             }
            
#             yield (serialized_message, attributes)
            
#         except Exception as e:
#             logger.error(f"Error processing CDC event: {str(e)}")
#             logger.error(f"Problematic element: {element}")
    
#     def serialize_message(self, transaction_update):
#         """
#         Serialize the transaction update using Avro schema.
        
#         Args:
#             transaction_update: Dictionary containing transaction update data
            
#         Returns:
#             Avro serialized binary data
#         """
#         # Define the Avro schema
#         schema_str = '''{
#             "namespace": "com.finserv.transactions",
#             "type": "record",
#             "name": "TransactionUpdate",
#             "fields": [
#                 {"name": "transaction_id", "type": "string"},
#                 {"name": "mutation_type", "type": "string"},
#                 {"name": "timestamp", "type": "long"},
#                 {"name": "data", "type": {
#                     "type": "record",
#                     "name": "TransactionData",
#                     "fields": [
#                         {"name": "account_id", "type": ["string", "null"]},
#                         {"name": "transaction_date", "type": ["string", "null"]},
#                         {"name": "amount", "type": ["string", "null"]},
#                         {"name": "transaction_type", "type": ["string", "null"]},
#                         {"name": "status", "type": ["string", "null"]},
#                         {"name": "merchant_name", "type": ["string", "null"]},
#                         {"name": "merchant_category", "type": ["string", "null"]},
#                         {"name": "description", "type": ["string", "null"]},
#                         {"name": "created_at", "type": ["string", "null"]},
#                         {"name": "updated_at", "type": ["string", "null"]}
#                     ]
#                 }}
#             ]
#         }'''
        
#         # Parse the schema
#         schema = fastavro.parse_schema(json.loads(schema_str))
        
#         # Prepare a normalized transaction data object that matches the schema
#         transaction_data = transaction_update['data']
#         normalized_data = {
#             'account_id': transaction_data.get('account_id'),
#             'transaction_date': transaction_data.get('transaction_date'),
#             'amount': transaction_data.get('amount'),
#             'transaction_type': transaction_data.get('transaction_type'),
#             'status': transaction_data.get('status'),
#             'merchant_name': transaction_data.get('merchant_name'),
#             'merchant_category': transaction_data.get('merchant_category'),
#             'description': transaction_data.get('description'),
#             'created_at': transaction_data.get('created_at'),
#             'updated_at': transaction_data.get('updated_at')
#         }
        
#         normalized_update = {
#             'transaction_id': transaction_update['transaction_id'],
#             'mutation_type': transaction_update['mutation_type'],
#             'timestamp': transaction_update['timestamp'],
#             'data': normalized_data
#         }
        
#         # Serialize to binary
#         out_buffer = io.BytesIO()
#         fastavro.schemaless_writer(out_buffer, schema, normalized_update)
#         return out_buffer.getvalue()


# def register_avro_schema():
#     """
#     Register an Avro schema with Pub/Sub.
#     Returns the schema ID for attaching to the topic.
#     """
#     schema_client = schema.SchemaServiceClient()
#     parent = schema_client.schema_path(
#         os.environ.get('GOOGLE_CLOUD_PROJECT', 'your-project-id'),
#         'transaction-update-schema'
#     )
    
#     avro_schema = '''{
#         "namespace": "com.finserv.transactions",
#         "type": "record",
#         "name": "TransactionUpdate",
#         "fields": [
#             {"name": "transaction_id", "type": "string"},
#             {"name": "mutation_type", "type": "string"},
#             {"name": "timestamp", "type": "long"},
#             {"name": "data", "type": {
#                 "type": "record",
#                 "name": "TransactionData",
#                 "fields": [
#                     {"name": "account_id", "type": ["string", "null"]},
#                     {"name": "transaction_date", "type": ["string", "null"]},
#                     {"name": "amount", "type": ["string", "null"]},
#                     {"name": "transaction_type", "type": ["string", "null"]},
#                     {"name": "status", "type": ["string", "null"]},
#                     {"name": "merchant_name", "type": ["string", "null"]},
#                     {"name": "merchant_category", "type": ["string", "null"]},
#                     {"name": "description", "type": ["string", "null"]},
#                     {"name": "created_at", "type": ["string", "null"]},
#                     {"name": "updated_at", "type": ["string", "null"]}
#                 ]
#             }}
#         ]
#     }'''
    
#     schema_obj = schema.Schema(
#         name=parent,
#         type_=schema.Schema.Type.AVRO,
#         definition=avro_schema
#     )
    
#     try:
#         # Try to create the schema
#         response = schema_client.create_schema(
#             request={"parent": f"projects/{os.environ.get('GOOGLE_CLOUD_PROJECT', 'your-project-id')}", "schema": schema_obj}
#         )
#         logger.info(f"Created schema: {response.name}")
#         return response.name
#     except Exception as e:
#         # Schema might already exist
#         logger.info(f"Schema creation error (might already exist): {str(e)}")
#         return parent


# def attach_schema_to_topic(topic_path, schema_id):
#     """
#     Attach an existing Avro schema to a Pub/Sub topic.
#     """
#     client = pubsub_v1.PublisherClient()
#     topic = pubsub_v1.Topic(
#         name=topic_path,
#         schema_settings=pubsub_v1.SchemaSettings(
#             schema=schema_id,
#             encoding=pubsub_v1.SchemaSettings.Encoding.BINARY
#         )
#     )
    
#     try:
#         topic = client.update_topic(
#             request={"topic": topic, "update_mask": {"paths": ["schema_settings"]}}
#         )
#         logger.info(f"Updated topic with schema: {topic.name}")
#     except Exception as e:
#         logger.error(f"Error attaching schema to topic: {str(e)}")


# def run(argv=None):
#     """Run the Dataflow pipeline."""
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         '--project',
#         dest='project',
#         required=True,
#         help='GCP project ID')
#     parser.add_argument(
#         '--bigtable_instance',
#         dest='bigtable_instance',
#         default='fin-bt',
#         help='Bigtable instance ID')
#     parser.add_argument(
#         '--bigtable_table',
#         dest='bigtable_table',
#         default='txn_tbl',
#         help='Bigtable table ID')
#     parser.add_argument(
#         '--pubsub_topic',
#         dest='pubsub_topic',
#         default='transactions-updates',
#         help='Pub/Sub topic ID')
#     parser.add_argument(
#         '--streaming',
#         dest='streaming',
#         default=True,
#         action='store_true',
#         help='Whether to run the pipeline in streaming mode')
    
#     known_args, pipeline_args = parser.parse_known_args(argv)
    
#     # Set Dataflow pipeline options
#     pipeline_options = PipelineOptions(pipeline_args)
#     pipeline_options.view_as(SetupOptions).save_main_session = True
#     pipeline_options.view_as(StandardOptions).streaming = known_args.streaming
    
#     # Fully qualified topic name
#     topic_path = f"projects/{known_args.project}/topics/{known_args.pubsub_topic}"
    
#     # Create Pub/Sub topic if it doesn't exist
#     publisher = pubsub_v1.PublisherClient()
#     try:
#         publisher.create_topic(name=topic_path)
#         logger.info(f"Created Pub/Sub topic: {topic_path}")
#     except Exception as e:
#         logger.info(f"Topic creation error (might already exist): {str(e)}")
    
#     # Register schema and attach to topic
#     schema_id = register_avro_schema()
#     attach_schema_to_topic(topic_path, schema_id)
    
#     # Run the pipeline
#     with beam.Pipeline(options=pipeline_options) as p:
#         # Read CDC events from Bigtable
#         cdc_events = (p 
#             | "Read CDC Events" >> ReadFromBigtableCDC(
#                 project_id=known_args.project,
#                 instance_id=known_args.bigtable_instance,
#                 table_id=known_args.bigtable_table)
#         )
        
#         # Format the events for Pub/Sub
#         formatted_events = (cdc_events
#             | "Format Transaction Records" >> beam.ParDo(FormatTransactionRecord())
#         )
        
#         # Write to Pub/Sub
#         formatted_events | "Write to Pub/Sub" >> WriteToPubSub(
#             topic=topic_path,
#             with_attributes=True
#         )


# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     run()