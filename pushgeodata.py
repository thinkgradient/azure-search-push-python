import sys
import json
import requests
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex 
import hashlib
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
import argparse
import time


config_file_name = "config.json"

parser = argparse.ArgumentParser()
parser.add_argument("YEAR")
parser.add_argument("MONTH")
parser.add_argument("DAY")
parser.add_argument("HOUR")
args = parser.parse_args()


with open(config_file_name, 'r') as f:
	configuration = json.load(f)

# Search service values
service_name = configuration["search_service"]["name"] 
key = configuration["search_service"]["key"] 
index_name = configuration["search_service"]["index_name"]
index_schema = configuration["search_service"]["index_schema"]


# Storage service values
conn_str = configuration["storage"]["conn_str"] 
container = configuration["storage"]["container"] 

# Batch size
batch_size = int(configuration["batch_size"])



# Get the service name (short name) and admin API key from the environment
endpoint = "https://{}.search.windows.net/".format(service_name)



class CreateClient(object):
    def __init__(self, endpoint, key, index_name):
        self.endpoint = endpoint
        self.index_name = index_name
        self.key = key
        self.credentials = AzureKeyCredential(key)

    # Create a SearchClient
    # Use this to upload docs to the Index
    def create_search_client(self):
        return SearchClient(endpoint=self.endpoint,
                            index_name=self.index_name,
                            credential=self.credentials)

    # Create a SearchIndexClient
    # This is used to create, manage, and delete an index
    def create_admin_client(self):
        return SearchIndexClient(endpoint=endpoint,
                                 credential=self.credentials)
# Get Schema from File or URL
def get_schema_data(schema, url=False):
    if not url:
        with open(schema) as json_file:
            schema_data = json.load(json_file)
            return schema_data
    else:
            data_from_url = requests.get(schema)
            schema_data = json.loads(data_from_url.content)
            return schema_data
    


# Create Search Index from the schema
# If reading the schema from a URL, set url=True
def create_schema_from_json_and_upload(schema, index_name, admin_client, url=False):

    cors_options = CorsOptions(allowed_origins=["*"], max_age_in_seconds=60)
    scoring_profiles = []
    schema_data = get_schema_data(schema, url)

    index = SearchIndex(
                name=index_name,
                fields=schema_data['fields'],
                scoring_profiles=scoring_profiles,
                suggesters=schema_data['suggesters'], 
                cors_options=cors_options)

    try:
        upload_schema = admin_client.create_index(index)
        if upload_schema:
            print(f'Schema uploaded; Index created for {index_name}.')
        else:
            exit(0)
    except:
        print("Unexpected error:", sys.exc_info()[0])


# Batch your uploads to Azure Search
def batch_upload_json_data_to_index(json_file, client):
    batch_array = []
    count = 1
    batch_counter = 0
    sen = ""
    #f = open(json_file)
    hex_counter = 0
    for i in json_file:
        sen = sen + i

        if i == "\n":
            if "{\"hex\":" in sen:

                hash_id = hashlib.md5(sen.encode())
                count += 1
                # [:-2] needed to remove the ',' character at the end of each json line, -1 is \n, -2 is ','
                if sen[-2] == ',':
                    sen = json.loads(sen[:-2])
                elif sen[-2] == '}':
                    # reached end of file, no ',' present
                    sen = json.loads(sen[:-1])

                longitude = float(sen['lon']) if "lon" in sen else -179
                latitude = float(sen['lat']) if "lat" in sen else -89
                if longitude < -180:
                    longitude = -179
                if longitude > 80:
                    longitude = 79
                if latitude < -90:
                    latitude = -89
                if latitude > 90:
                    latitude = 89

                batch_array.append({
                    "id": str(hash_id.hexdigest()),
                    "hex": str(sen['hex']) if "hex" in sen else "no-hex-key",
                    "type": str(sen['type']) if "type" in sen else "no-flight-type",
                    "flight": str(sen['flight']) if "flight" in sen else "no-flight-name",
                    "r": str(sen['r']) if "r" in sen else "no-r",
                    "t": str(sen['t']) if "t" in sen else "no-t",
                    "alt_baro": int(sen['alt_baro']) if "alt_baro" in sen and str(sen['alt_baro']).isdigit() else 0 ,
                    "gs": float(sen['gs']) if "gs" in sen and str(sen["gs"]).isdigit() else 0,
                    "track": float(sen['track']) if "track" in sen else 0,
                    "lat": latitude,
                    "lon": longitude,
                    "Geo": {"type": "Point", "coordinates": [longitude, latitude]}     
                })
                sen = ""
            else:
                sen = ""
               

            # In this sample, we limit batches to 1000 records.
            # When the counter hits a number divisible by 1000, the batch is sent.
            if count != 0:
                if count % batch_size == 0:
                    client.upload_documents(documents=batch_array)

                    batch_counter += 1
                    print(f'Batch sent! - #{batch_counter}')
                    batch_array = []

    # This will catch any records left over, when not divisible by 1000
    if len(batch_array) > 0:
        client.upload_documents(documents=batch_array)
        batch_counter += 1
        print(f'Final batch sent! - #{batch_counter}')
    #f.close()

    print('Done!')

def create_index(index_schema, index_name, admin_client):
    print("Index creation is being executed")
    schema = create_schema_from_json_and_upload(
        index_schema,
        index_name,
        admin_client,
        url=False)


if __name__ == '__main__':
    if args.YEAR == "":
        raise ValueError('Please a valid YEAR value, e.g. 2021. YEAR cannot be empty!')
    year = "YEAR=" + str(args.YEAR)
    month = "MONTH=" + str(args.MONTH)
    day = "DAY=" + str(args.DAY)
    hour = "HOUR=" + str(args.HOUR)
    if args.MONTH == "":
        path = year
    if args.DAY == "" and args.MONTH != "":
        path = year + "/" + month
    if args.MONTH != "" and args.DAY != "":
        path = year + "/" + month + "/" + day
    if args.HOUR != "":
    	path = year + "/" + month + "/" + day + "/" + hour
    
    print(endpoint)
    print(key)


    start_client = CreateClient(endpoint, key, index_name)
    admin_client = start_client.create_admin_client()
    search_client = start_client.create_search_client()
    
    # run only once
    print(index_schema)
    print(index_name)

    #create_index(index_schema, index_name, admin_client)
  
   

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container)
    generator = container_client.list_blobs(name_starts_with=path)

    container_client = ContainerClient.from_connection_string(
            conn_str=conn_str, 
            container_name=container
            )
    for blob in generator:
        print("----- Processing ------: " + blob.name)
        
        blob_client = container_client.get_blob_client(blob.name)
        data = blob_client.download_blob()
        start_time = time.time()
        batch_upload = batch_upload_json_data_to_index(data.content_as_text(), search_client)
        print("--- %s seconds ---" % round(time.time() - start_time, 2))


