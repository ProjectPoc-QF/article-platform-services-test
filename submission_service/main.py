import uuid
from fastapi import FastAPI, status
from pydantic import BaseModel, Field, HttpUrl

# 1. Initialize the FastAPI application
app = FastAPI(
    title="Submission Service",
    description="Handles the submission of articles for analysis.",
)

# 2. Define Pydantic models to match the OpenAPI contract schemas
# These enforce data validation for requests and responses.

class ArticleSubmission(BaseModel):
    url: HttpUrl # Pydantic validates this is a valid URL

class SubmissionAck(BaseModel):
    job_id: uuid.UUID = Field(..., description="The unique ID for the processing job.")

class ProcessingResult(BaseModel):
    job_id: uuid.UUID
    status: str
    url: HttpUrl
    analysis: dict | None = None # Analysis can be null if pending


# 3. Implement the POST endpoint for submitting an article
@app.post(
    "/articles",
    summary="Submit an article for analysis",
    status_code=status.HTTP_202_ACCEPTED, # Set the success status code to 202
    response_model=SubmissionAck,
)
def submit_article_for_analysis(submission: ArticleSubmission):
    """
    Accepts an article URL, generates a unique job ID,
    and simulates publishing a job to the message queue.
    """
    # Generate a unique ID for this job
    job_id = uuid.uuid4()

    # TODO: In Step 3, we will replace this print statement
    # with code that sends a message to RabbitMQ.
    print(f"Received job {job_id} for URL: {submission.url}")
    print("--> Publishing job to message queue... (SIMULATED)")

    # Return the acknowledgement response to the client
    return SubmissionAck(job_id=job_id)


# 4. Implement a placeholder GET endpoint for retrieving results
@app.get(
    "/articles/{jobId}",
    summary="Get the analysis result for an article",
    response_model=ProcessingResult,
)
def get_analysis_result(jobId: uuid.UUID):
    """
    A placeholder endpoint. In a later step, this will query
    a database to get the actual job status and result.
    """
    # TODO: In Step 4, we will replace this with a real database lookup.
    print(f"--> Checking status for job {jobId}... (SIMULATED)")

    # For now, we return a hardcoded "pending" status.
    return ProcessingResult(
        job_id=jobId,
        status="pending",
        # We don't have the URL here yet, so we use a placeholder.
        # This shows a limitation we will fix later.
        url="http://example.com",
        analysis=None
    )