import numpy as np
import functions_framework
from google.cloud import storage, pubsub_v1
import os
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import concurrent.futures
import redis
import datetime
from pymemcache.client import base
redis_client = redis.StrictRedis(host="10.0.1.67", port=6379, decode_responses=True)
memcached_client = base.Client(("10.0.1.3", 11211))

#Initialize 



MATA_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/mat-A-loader"
MATB_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/mat-B-loader"
AGGREGRATION_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/aggregrator"


# Initialize Pub/Sub client
publisher_client = pubsub_v1.PublisherClient()
subscriber_client = pubsub_v1.SubscriberClient()
project_id = 'aaron-fernandes-fall2024'
topic_name = 'matrix-multiplication-topic'
topic_path = publisher_client.topic_path(project_id,topic_name)
mapper_completion_topic_path = publisher_client.topic_path('aaron-fernandes-fall2024', 'mapper-completion-topic')
reducer_completion_topic_path = publisher_client.topic_path('aaron-fernandes-fall2024', 'reduce-completion-topic')
mapper_completion_subscription_id = "completion-subscription"
reducer_completion_subscription_id = "completion-subscription-reducer"

#Matrix file names
MATRIX_1 = "matrix_A.npy"
MATRIX_2 = "matrix_B.npy"





#MAIN FUNCTION

# Triggered by a change in input storage bucket
@functions_framework.cloud_event
def initMatrixMul(cloud_event):
    redis_client.flushall()
    memcached_client.flush_all()

    start_time = int(datetime.datetime.utcnow().timestamp())
    memcached_client.set('matrix_start_time', start_time)
    try:
        data = cloud_event.data

        bucket_name = data["bucket"]

        #both matrices have to be in bucket
        if not check_for_both_matrices(bucket_name):
            print("Both matrices are not present.")
            return "Missing matrices, initialization halted.",404


       
        N,J= getMatrixShape(bucket_name, MATRIX_1)
        _,K= getMatrixShape(bucket_name, MATRIX_2)  

        #initializes the completion subscriptions for mappers and reducers 
  
        create_subscription(mapper_completion_subscription_id,mapper_completion_topic_path)
  
        create_subscription(reducer_completion_subscription_id,reducer_completion_topic_path)
        

        #Call mappers concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_a = executor.submit(call_matrix_loader_async,N,K,J,bucket_name, MATA_FUNCTION_URL)
            future_b = executor.submit(call_matrix_loader_async,N,K,J,bucket_name, MATB_FUNCTION_URL)
            #wait for mappers to finish before calling reducers
            response_a = future_a.result()
            response_b = future_b.result()
        print(f"mapping tasks done: response from Loader A: {response_a}, response from B: {response_b}")

        return "Matrix multiplication successfully started.",

    except Exception as e:
        print(f"Error in initMatrixMul: {e}")
        return "An error occurred during matrix multiplication.", 500

            






#Matrix helper functions

def check_for_both_matrices(bucket_name):
    """Check if both matrices are present in the bucket."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Check if both matrix_A.npy and matrix_B.npy exist in the bucket
        blob_A = bucket.blob(MATRIX_1)
        blob_B = bucket.blob(MATRIX_2)
        
        if blob_A.exists() and blob_B.exists():
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking for matrices: {e}")
        return False    
    
def getMatrixShape(bucket_name, file_name):

    # Initialize the Cloud Storage client
    try:
        clientBucket = storage.Client()
        bucket = clientBucket.bucket(bucket_name)
        blob = bucket.blob(file_name)
        temp_file = f"/tmp/{file_name}"
        blob.download_to_filename(temp_file)
        matrix = np.load(temp_file)
        shape = matrix.shape
        os.remove(temp_file)
        return shape
    except Exception as e:
        print(f"Error getting matrix shape for {file_name}: {e}")
        return None
    
def create_subscription(subscription_id, topic_path):
    try:
        subscription_path = subscriber_client.subscription_path(project_id, subscription_id)
        subscription=subscriber_client.create_subscription(
                    request ={
                    "name":subscription_path,
                    "topic":topic_path,
                    "ack_deadline_seconds":600,
                    }
                )
        print(f"Subscription {subscription} created.")
    except Exception as e:
        print(f"Error creating subscription: {e}")



#CLOUD FUNCTION CALLER FUNCTIONS
def call_matrix_loader_async(N, K,J, bucket_name,url):

    try:
        payload = {"N": N, "K": K,"J":J ,"bucket_name":bucket_name}
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        
        # Make an authenticated POST request asynchronously
        response = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {id_token}"}
        )
        return f"{response.text}", response.status_code
    except Exception as e:
        print(f"Error calling matrix loader function: {e}")
        return None, 

def call_completion_tracker(subscriptionID,url,lenA=None,lenB=None):
    try:
        if lenA!=None and lenB!= None:
            payload = {"subscriptionID":subscriptionID, "lenA":lenA, "lenB":lenB}
        else: payload = {"subscriptionID":subscriptionID}
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        
        # Make an authenticated POST request asynchronously
        response = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {id_token}"}
        )
        return f"{response.text}", response.status_code
    except Exception as e:
        print(f"Error calling matrix loader function: {e}")
        return None, 500












