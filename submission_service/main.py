import uuid
import json
import pika
import redis # Import the redis library
from fastapi import FastAPI, status, HTTPException # Import HTTPException
from pydantic import BaseModel, Field, HttpUrl

# ... (app initialization is the same) ...
app = FastAPI(
    title="Submission Service",
    description="Handles the submission of articles for analysis.",
)

# --- NEW: Connect to Redis ---
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# ... (Pydantic models are the same) ...
class ArticleSubmission(BaseModel):
    url: HttpUrl
class SubmissionAck(BaseModel):
    job_id: uuid.UUID = Field(..., description="The unique ID for the processing job.")
class ProcessingResult(BaseModel):
    job_id: uuid.UUID
    status: str
    url: HttpUrl
    analysis: dict | None = None

# ... (The POST endpoint submit_article_for_analysis remains exactly the same) ...
@app.post(
    "/articles",
    summary="Submit an article for analysis",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SubmissionAck,
)
def submit_article_for_analysis(submission: ArticleSubmission):
    # ... (no changes here) ...
    job_id = uuid.uuid4()
    message_body = {"job_id": str(job_id), "url": str(submission.url)}
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='article_analysis_jobs', durable=True)
    channel.basic_publish(
        exchange='', routing_key='article_analysis_jobs', body=json.dumps(message_body),
        properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
    )
    connection.close()
    print(f"--> Published job {job_id} to RabbitMQ for URL: {submission.url}")
    return SubmissionAck(job_id=job_id)

@app.get(
    "/articles/{jobId}",
    summary="Get the analysis result for an article",
    response_model=ProcessingResult,
)
def get_analysis_result(jobId: uuid.UUID):
    """
    Retrieves the job status and result from the Redis database.
    """
    # --- REPLACEMENT FOR THE TODO ---
    print(f"--> Checking status for job {jobId} in Redis...")
    
    result_json = redis_client.get(str(jobId))

    if result_json is None:
        # If the key doesn't exist, it means the worker hasn't even
        # started processing it yet, or the ID is invalid.
        # We return a 404 Not Found error.
        raise HTTPException(status_code=404, detail="Job ID not found.")

    # The result is stored as a JSON string, so we parse it.
    result_data = json.loads(result_json)
    
    return ProcessingResult(
        job_id=jobId,
        status=result_data.get("status"),
        url=result_data.get("url"),
        analysis=result_data.get("analysis")
    )
    # --- END OF REPLACEMENT ---