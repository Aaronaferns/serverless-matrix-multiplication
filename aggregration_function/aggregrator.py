import json
from google.cloud import storage
import functions_framework
from pymemcache.client import base
import numpy as np
import os
import datetime

memcached_client = base.Client(("10.0.1.3", 11211))
bucket_name = "output-matrix-bucket"


@functions_framework.http
def process_request(request):
    start_time = int(memcached_client.get('matrix_start_time'))
    
    try:
        # Parse the incoming JSON request
        request_json = request.get_json(silent=True)
        
        if not request_json:
            return json.dumps({"error": "Invalid JSON payload"}), 400
        
        # Extract values from the JSON payload
        K = int(request_json.get('K'))
        N = int(request_json.get('N'))
        
        # Initialize an output matrix of zeros with shape (N, K)
        output = np.zeros((N, K))
        
        # Iterate through the matrix and retrieve values from Memcached
        for i in range(N):
            for j in range(K):
                result = memcached_client.get(f"{i}_{j}")
                
                if result is None:
                    return json.dumps({"error": f"Key {i}_{j} not found in Memcached."}), 404
                else:
                    # Process the data and store it in the output matrix
                    print(f"Key {i}_{j} found in Memcached: {result}")
                    output[i, j] = float(result)
        end_time = int(datetime.datetime.utcnow().timestamp())
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('time_ellapsed.txt')
        blob.upload_from_string(f"{end_time-start_time}", content_type='text/plain')
        # Save the output matrix to a cloud storage bucket
        save_matrix_to_bucket(output,bucket_name,"output.npy")
        
        return "successful", 200
    
    except ValueError as ve:
        # Handle errors related to value conversion (e.g., invalid integers)
        print(f"ValueError occurred: {ve}")
        return json.dumps({"error": f"ValueError: {str(ve)}"}), 400
    
    except Exception as e:
        # Catch any other exceptions that may occur
        print(f"An error occurred: {e}")
        return json.dumps({"error": f"An unexpected error occurred: {str(e)}"}), 500
  



def save_matrix_to_bucket(matrix, bucket_name, blob_name):
    temp_file = "/tmp/output_matrix.npy"
    np.save(temp_file, matrix)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(temp_file)
    print(f"Matrix successfully uploaded to {bucket_name}/{blob_name}")
    os.remove(temp_file)
    