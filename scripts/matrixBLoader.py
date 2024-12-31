import numpy as np
import base64
# from pymemcache.client import base
import functions_framework
from google.cloud import storage
import os
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import concurrent.futures
import json



MAPPER_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/mapper"




@functions_framework.http
def loadMatrix(request):
    #send bucket name in request
    request_json = request.get_json(silent=True)
    if not request_json:
        return json.dumps({"error": "Invalid JSON payload"}), 400

    bucket_name = request_json.get('bucket_name')
    K = int(request_json.get('K'))
    N = int(request_json.get('N'))
    J = int(request_json.get('J'))

    # Log the event details
    print(f"Bucket: {bucket_name}")
   
    matrix = download_and_load_matrix(bucket_name, "matrix_B.npy")
    print("Matrix B loaded successfully:")

    # Determine chunk size based on the larger dimension
    max_dimension = max(matrix.shape)
    if max_dimension > 10:
        chunk_size = 10
    elif max_dimension == 1:
        chunk_size = 1
    else:
        chunk_size = 2

    # matrix_chunks = createChunks(matrix, chunk_size)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures =[]
        # for chunk, i_start, i_end, j_start, j_end in matrix_chunks:
        row_chunks = matrix.shape[0] // chunk_size + (matrix.shape[0] % chunk_size != 0)
        column_chunks = matrix.shape[1] // chunk_size + (matrix.shape[1] % chunk_size != 0)
        
        # Loop over row and column chunks to calculate indices
        for i in range(row_chunks):
            for j in range(column_chunks):
                # Calculate the start and end indices for rows and columns
                i_start = i * chunk_size
                i_end = min(i_start + chunk_size - 1, matrix.shape[0] - 1)  # Ensure we don't go out of bounds
                
                j_start = j * chunk_size
                j_end = min(j_start + chunk_size - 1, matrix.shape[1] - 1)

                
                #instead lets call mapper functions:
                chunk=matrix[i_start:i_end+1, j_start:j_end+1]
                futures.append(executor.submit(call_mapper_function,chunk, i_start, i_end, j_start, j_end,K=K,J=J,N=N,size=chunk_size))
        for future in futures:
            result = future.result()  # This will block until all mapper function calls are completed
            print(f"Mapper result: {result}")
        
    return f"Done MatB Loader",200 


#Helper functions

def call_mapper_function(chunk, i_start, i_end, j_start, j_end, N = None, K = None,J=None,size = None):
    """Cloud Function that invokes another Cloud Function."""
    name = f"B_{i_start}_{i_end}_{j_start}_{j_end}"
    matrix_bytes = chunk.astype(np.float64).tobytes()
    matrix_b64 = base64.b64encode(matrix_bytes).decode('utf-8')
    
    # Prepare data to send to the target function
    payload = {
        "chunk": matrix_b64,
        "chunk_name": name,
         "N":N,
         "K":K,
         "J":J,
         "size":size
    }
    
    # Authenticate using an Identity Token
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, MAPPER_FUNCTION_URL)
    
    # Make an authenticated POST request to the target function
    response = requests.post(
        MAPPER_FUNCTION_URL,
        json=payload,
        headers={"Authorization": f"Bearer {id_token}"}
    )
    
    # Return the response from the target function
    return f"Response from target function: {response.text}", response.status_code

def download_and_load_matrix(bucket_name, file_name):
    # Initialize the Cloud Storage client
    clientBucket = storage.Client()
    bucket = clientBucket.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Download the file to a local temporary location
    temp_file = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file)

    # Load the matrix from the .npy file
    matrix = np.load(temp_file)

    # Optionally delete the temporary file to free up space
 
    os.remove(temp_file)

    return matrix