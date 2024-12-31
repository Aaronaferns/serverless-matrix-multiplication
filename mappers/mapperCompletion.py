import json
from google.cloud import pubsub_v1
import functions_framework
import redis
import concurrent.futures
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import base64

project_ID='aaron-fernandes-fall2024'
subscriber = pubsub_v1.SubscriberClient()

# Initialize Redis client
redis_client = redis.StrictRedis(host="10.0.1.67", port=6379, decode_responses=True)

REDUCER_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/reducer"

@functions_framework.cloud_event
def mapper_completed(event):
    """Triggered by a Pub/Sub message."""
    request_json = event.data
    # Parse subscription details from the message 
    try:
        pubsub_message = request_json.get('message', {}).get('data')
        if pubsub_message is None:
            raise ValueError("No data in Pub/Sub message")
        
        decoded_message = base64.b64decode(pubsub_message).decode('utf-8')
        parsed_data = json.loads(decoded_message)
        # Parse subscription details from the message payload
        N = int(parsed_data.get('N'))
        K = int(parsed_data.get('K'))
        J = int(parsed_data.get('J'))
        size = int(parsed_data.get('size'))
        message_id = parsed_data.get('messageID')
        print(f"Parsed mapper data - N: {N}, K: {K}, J: {J}, size: {size}, message_ID: {message_id}")
      
        if redis_client.setnx(message_id, 'processed_message'):
            # If the key was successfully set (i.e., it didn't exist before), set an expiration time
            redis_client.expire(message_id, 100)
            print(f"mapper completion message {message_id} has been processed and stored.")
        else:
            # If the key already exists, return a message indicating that it's already processed
            print(f"mapper completion message {message_id} has already been processed.")
            return "already processed mapper completion", 200
        
    except Exception as e:
        print(f"Error parsing message data: {e}")
        return json.dumps({"error": f"Failed to parse message: {e}"}), 500

    target_messages = count_chunks(N,J,size)+count_chunks(J,K,size)

    # Redis keys for storing message count and target messages
    count_key = f"count_mapper"
    target_key = f"mapper_target"

    # Initialize target messages in Redis if not already set
    if not redis_client.exists(target_key):
        redis_client.set(target_key, target_messages)

    # Increment message count atomically
    message_count = redis_client.incr(count_key)

    # Retrieve target messages to check completion
    stored_target_messages = int(redis_client.get(target_key))

    if message_count >= stored_target_messages:
        print(f"Received all mapper completion ,{message_count},{stored_target_messages},{target_messages}")

        #trigger calls to reducers
        with concurrent.futures.ThreadPoolExecutor() as executor:    
            if K>N: 
                col = 0
                span = N
            else :  
                col = 1
                span = K

            for x in range(span):
                key = x
                executor.submit(call_reducer_function_async, REDUCER_FUNCTION_URL, N,K,J, key,col)

    return json.dumps({"status": "Message processed", "count": message_count}), 200

#call reducer
def call_reducer_function_async(url,N, K,J,key,col):
    try: 
        print(f"in reducer caller:{key}")
        payload = {"N": N, "K": K,"J":J,"key":key, "col":col}
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, REDUCER_FUNCTION_URL)
        # Make an authenticated POST request to the reducer function
        response = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {id_token}"}
        )
        return f"Response from reducer function: {response.text}", response.status_code
    except Exception as e:
        print(f"Error calling reducer function for key ({key}): {e}")
        return None, 500
    
def count_chunks(rows, cols, chunk_size):
    
    row_chunks = rows // chunk_size + (rows % chunk_size != 0)   
    col_chunks = cols // chunk_size + (cols % chunk_size != 0)
    total_chunks = row_chunks * col_chunks
    return total_chunks