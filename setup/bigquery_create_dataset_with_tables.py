# Imports the Google Cloud client library
from google.cloud import bigquery

"""Creating a Dataset"""
# Instantiates a client
bigquery_client = bigquery.Client()

# The name for the new dataset
dataset_id = 'infusionsoft_dataset'

# Prepares a reference to the new dataset
dataset_ref = bigquery_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

# Creates the new dataset
dataset = bigquery_client.create_dataset(dataset)

print('Dataset {} created.'.format(dataset.dataset_id))


"""Creating all  100 tables"""
dataset_ref = bigquery_client.dataset(dataset_id)


schema = [
    bigquery.SchemaField('Timestamp', 'TIMESTAMP'),
    bigquery.SchemaField('A', 'INTEGER'),
    bigquery.SchemaField('B', 'INTEGER'),
    bigquery.SchemaField('C', 'INTEGER'),
    bigquery.SchemaField('D', 'INTEGER'),
    bigquery.SchemaField('E', 'INTEGER'),
]
for i in range(1,101):
    table_name='table_'+str(i)
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)  # API request
    assert table.table_id == table_name
