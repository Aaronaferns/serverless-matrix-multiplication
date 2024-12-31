import json
import redis
from google.cloud import pubsub_v1
import functions_framework
import google.auth.transport.requests
import google.oauth2.id_token
import requests
import base64
AGGREGRATION_FUNCTION_URL = "https://us-central1-aaron-fernandes-fall2024.cloudfunctions.net/aggregrator"


# Initialize Redis client
redis_client = redis.StrictRedis(host="10.0.1.67", port=6379, decode_responses=True)

# Project configuration
project_ID = 'aaron-fernandes-fall2024'
subscriber = pubsub_v1.SubscriberClient()

@functions_framework.cloud_event
def reducer_completed(event):
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
        message_id = parsed_data.get("messageID")

        print(f"Parsed reducer data - N: {N}, K: {K}")

        if redis_client.setnx(message_id, 'processed_message'):
            # If the key was successfully set (i.e., it didn't exist before), set an expiration time
            redis_client.expire(message_id, 100)
            print(f"reducer completion message {message_id} has been processed and stored.")
        else:
            # If the key already exists, return a message indicating that it's already processed
            print(f"reducer completion message {message_id} has already been processed.")
            return "already processed reducer completion", 200
    except Exception as e:
        print(f"Error parsing message data: {e}")
        return json.dumps({"error": f"Failed to parse message: {e}"}), 500
    
    target_messages = min(N,K)

    # Redis keys for storing message count and target messages
    count_key = f"count_reducer"
    target_key = f"target_reducer"

    # Initialize target messages in Redis if not already set
    if not redis_client.exists(target_key):
        redis_client.set(target_key, target_messages)

    # Increment message count atomically
    message_count = redis_client.incr(count_key)

    # Retrieve target messages to check completion
    stored_target_messages = int(redis_client.get(target_key))

    if message_count >= stored_target_messages:
        print(f"Received all reducer completion messages {message_count},{stored_target_messages}, {target_messages}")
        #After completion call aggregration funciton:
        print("calling aggregation function")
        call_aggregration_function(N,K)

    return json.dumps({"status": "Message processed", "count": message_count}), 200


def call_aggregration_function(N, K):
    try:
        payload = {"N": N, "K": K}
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, AGGREGRATION_FUNCTION_URL)
        # Make an authenticated POST request to the reducer function
        response = requests.post(
            AGGREGRATION_FUNCTION_URL,
            json=payload,
            headers={"Authorization": f"Bearer {id_token}"}
        )
        return f"Response from Aggregation function: {response.text}", response.status_code
    except Exception as e:
        print(f"Error calling aggregation function: {e}")
        return None, 500