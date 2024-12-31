import json
from google.cloud import pubsub_v1
import functions_framework
from pymemcache.client import base
import redis



#Pub/Sub 
publisher = pubsub_v1.PublisherClient()

memcached_host="10.0.1.3"
project_ID='aaron-fernandes-fall2024'
reducer_completion_topic_path = publisher.topic_path('aaron-fernandes-fall2024', 'reduce-completion-topic')


memcached_client = base.Client((memcached_host, 11211))
# Initialize Redis client
redis_client = redis.StrictRedis(host="10.0.1.67", port=6379, decode_responses=True)

# I want each reducer to listen to a specific key
@functions_framework.http
def reducer_function(request):
    #Parse incoming JSON data
    request_json = request.get_json(silent=True)
    key = int(request_json.get("key"))
    K=int(request_json.get("K"))
    N=int(request_json.get("N"))
    col = int(request_json.get("col"))


    if not request_json:
        return json.dumps({"error": "Invalid JSON payload"}), 400


    
    try:
        print(f"Reducer {key} processing data")
        #Process the data
        # Retrieve the entire list from Redis
        if col==0:
            for k in range(K):
                key1 = (key,k)
                stored_values = redis_client.lrange(f"{key1[0]}-{key1[1]}", 0, -1)  # Fetch all items
                deserialized_values = [json.loads(item) for item in stored_values]
                print(f"deserialized values for {key1}: {deserialized_values}")
                sum = 0
                A = {}
                B = {}
                for value in deserialized_values:
                    if value[0]=='A':
                        A[value[1]]=value[2]
                    elif value[0]=='B':
                        B[value[1]]=value[2]
                for j in A.keys():
                    sum+=A[j]*B[j]    
                memcached_client.set(f"{key1[0]}_{key1[1]}", sum)
        else:
            for i in range(N):
                key1 = (i,key)
                stored_values = redis_client.lrange(f"{key1[0]}-{key1[1]}", 0, -1)  # Fetch all items
                deserialized_values = [json.loads(item) for item in stored_values]
                print(f"deserialized values for {key1}: {deserialized_values}")
                sum = 0
                A = {}
                B = {}
                for value in deserialized_values:
                    if value[0]=='A':
                        A[value[1]]=value[2]
                    elif value[0]=='B':
                        B[value[1]]=value[2]
                for j in A.keys():
                    sum+=A[j]*B[j]    
                memcached_client.set(f"{key1[0]}_{key1[1]}", sum)
        
        
  

        # Save the partial result in memcache

        

        messageID=f"red_{key}"
        #publish_completion()
        publish_to_reducer_completion_pubsub(K,N,messageID)
        return f"Results for red {f"{key}"} stored in memcache"
        
    
    except Exception as e:
        raise f"Failed to  run reducer: {e}"
    
def publish_to_reducer_completion_pubsub(K,N,messageID):
   
    message_data = json.dumps({'completed': "yes", "K":K, "N":N,"messageID":messageID}).encode('utf-8')
    # Publish message with 'key' as an attribute
    future = publisher.publish(reducer_completion_topic_path, data=message_data)
    future.result()  # Wait for the message to be published