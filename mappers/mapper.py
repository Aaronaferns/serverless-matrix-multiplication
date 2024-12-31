import json
import functions_framework
import numpy as np
from google.cloud import pubsub_v1
import base64
import redis

# Initialize Redis client
redis_client = redis.StrictRedis(host="10.0.1.67", port=6379, decode_responses=True)
# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
mapper_completion_topic_path = publisher.topic_path('aaron-fernandes-fall2024', 'mapper-completion-topic')

def store_in_redis(key, value):
    """
    Stores values in a Redis hash for a given key.
    """
    try:
        key = f"{key[0]}-{key[1]}"
        redis_client.rpush(key, json.dumps(value))
        print(f"storing key {key} in reduce")
    except:
        print("store in redis failed")

    
def publish_to_mapper_completion_pubsub(N,K,J,size,chunk_id):

   
    message_data = json.dumps({'N': N, "K":K, "J":J,"size":size,"messageID":chunk_id}).encode('utf-8')
    print(f"Publishing message with values N: {N}, K: {K}, J: {J}, size: {size}")
    # Publish message with 'key' as an attribute
    future = publisher.publish(mapper_completion_topic_path, data=message_data)
    future.result()  # Wait for the message to be published


@functions_framework.http
def mapper(request):
    """Cloud Function that processes a matrix chunk sent as a JSON request."""
    
    # Parse incoming JSON data
    request_json = request.get_json(silent=True)
    
    if not request_json:
        return json.dumps({"error": "Invalid JSON payload"}), 400
    
    # Extract Base64-encoded chunk and metadata
    matrix_b64 = request_json.get('chunk')
    chunk_name = request_json.get('chunk_name')
    chunkID=chunk_name
    size = int(request_json.get('size'))
    N = int(request_json.get("N"))
    J = int(request_json.get("J"))
    K = int(request_json.get("K"))
    
    if not matrix_b64 or not chunk_name:
        return json.dumps({"error": "Missing 'chunk' or 'chunk_name' in request"}), 400
    
    try:
        chunk_name=chunk_name.split("_")
        shape = (int(chunk_name[2])-int(chunk_name[1])+1, int(chunk_name[4])-int(chunk_name[3])+1)
        # Decode Base64 string back into bytes
        matrix_bytes = base64.b64decode(matrix_b64)
        
        # Convert bytes back into a NumPy array
        matrix_chunk = np.frombuffer(matrix_bytes, dtype=np.float64).reshape(shape)
        
        if chunk_name[0]=='A':
           
            
            for i in range(matrix_chunk.shape[0]):
                for j in range(matrix_chunk.shape[1]):
                    for k in range(K):
                        key = (i+int(chunk_name[1]),k)
                        value = ('A',j+int(chunk_name[3]),matrix_chunk[i,j])
                        store_in_redis(key, value)
            #publish to the mapper completion pubsub
           
        if chunk_name[0]=='B':
        
            
            for j in range(matrix_chunk.shape[0]):
                for k in range(matrix_chunk.shape[1]):
                    for i in range(N):
                        key = (i,k+int(chunk_name[3]))
                        value = ('B',j+int(chunk_name[1]),matrix_chunk[j,k])
                        store_in_redis(key, value)
            #publish to the mapper completion pubsub
        publish_to_mapper_completion_pubsub(N,K,J,size,chunkID)

        
        
        # Prepare response data
        response_data = {
            "successful": "yes"
        }
        return json.dumps(response_data), 200
    
    except Exception as e:
        # Handle any errors during processing
        print("Mapper failed",e)
        return json.dumps({"error": str(e)}), 500