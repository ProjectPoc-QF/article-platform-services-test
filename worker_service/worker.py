import pika
import time
import json
import requests
import redis # Import the redis library
from bs4 import BeautifulSoup

def perform_analysis(url: str) -> dict:
    """Fetches a URL and performs a simple text analysis."""
    try:
        print(f"Fetching content from {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes

        # Use BeautifulSoup to parse HTML and get only the text
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        
        # Simple analysis
        word_count = len(text.split())
        character_count = len(text)

        result = {
            "word_count": word_count,
            "character_count": character_count,
        }
        print(f"Analysis complete: {result}")
        return result
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return {"error": str(e)}

def callback(ch, method, properties, body):
    """This function is called whenever a message is received."""
    print(" [x] Received message...")
    
    message = json.loads(body)
    job_id = message.get("job_id")
    url = message.get("url")

    if not job_id or not url:
        print(" [!] Invalid message format. Skipping.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Simulate a processing delay
    time.sleep(5) 
    
    analysis_result = perform_analysis(url)
    
    # TODO: In Step 4, we will replace this print statement
    # with code that saves the result to a database.
    print(f"--> Saving result for job {job_id} to database... (SIMULATED)")
    print(json.dumps(analysis_result, indent=2))
    
    # Acknowledge that the message has been processed successfully.
    # This removes it from the queue.
    print(" [x] Done. Acknowledging message.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# --- Main script execution ---
# Establish connection
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Declare the same queue as the publisher
channel.queue_declare(queue='article_analysis_jobs', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

# Tell RabbitMQ that this callback function should receive messages from our queue
channel.basic_consume(queue='article_analysis_jobs', on_message_callback=callback)

# Start listening for messages
channel.start_consuming()



# --- NEW: Connect to Redis ---
# This creates a connection pool, which is more efficient.
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def perform_analysis(url: str) -> dict:
    # ... (This function remains exactly the same) ...
    try:
        print(f"Fetching content from {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        word_count = len(text.split())
        character_count = len(text)
        result = {"word_count": word_count, "character_count": character_count}
        print(f"Analysis complete: {result}")
        return result
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return {"error": str(e)}

def callback(ch, method, properties, body):
    """This function is called whenever a message is received."""
    print(" [x] Received message...")
    
    message = json.loads(body)
    job_id = message.get("job_id")
    url = message.get("url")

    if not job_id or not url:
        print(" [!] Invalid message format. Skipping.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # --- REPLACEMENT FOR THE TODO ---
    # 1. Immediately save the initial state to Redis
    initial_record = {"status": "processing", "url": url, "analysis": None}
    redis_client.set(job_id, json.dumps(initial_record))
    print(f"--> Job {job_id} status set to 'processing' in Redis.")

    # 2. Perform the analysis (can take time)
    time.sleep(5) 
    analysis_result = perform_analysis(url)

    # 3. Update the record in Redis with the final result
    if "error" in analysis_result:
        final_record = {"status": "failed", "url": url, "analysis": analysis_result}
    else:
        final_record = {"status": "completed", "url": url, "analysis": analysis_result}
    
    redis_client.set(job_id, json.dumps(final_record))
    print(f"--> Final result for job {job_id} saved to Redis.")
    # --- END OF REPLACEMENT ---
    
    print(" [x] Done. Acknowledging message.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# ... (The main connection logic at the bottom remains the same) ...
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='article_analysis_jobs', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.basic_consume(queue='article_analysis_jobs', on_message_callback=callback)
channel.start_consuming()