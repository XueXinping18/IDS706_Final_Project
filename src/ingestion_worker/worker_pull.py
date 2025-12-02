import asyncio
import os
import json
import logging
from google.cloud import pubsub_v1
from ingestion_worker.config import Config
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.infrastructure.vertex import VertexClient
from ingestion_worker.infrastructure.replicate import ReplicateClient
from ingestion_worker.domain.transcoding import TranscodingService
from ingestion_worker.domain.asr import ASRService
from ingestion_worker.domain.agentic.orchestrators import AgenticService
from ingestion_worker.domain.persistence import PersistenceService
from ingestion_worker.application.workflow import IngestVideoWorkflow
from ingestion_worker.types import PubSubMessage
from ingestion_worker.utils.logging import get_logger

logger = get_logger(__name__)

async def main():
    config = Config()
    
    # Initialize Infrastructure
    db = Database(config.database_url)
    await db.connect()
    
    gcs = GCSClient(config.project_id)
    vertex = VertexClient(config.project_id, config.location)
    replicate = ReplicateClient(config.replicate_api_token)
    
    # Initialize Domain Services
    transcoding_service = TranscodingService(gcs, config)
    asr_service = ASRService(replicate, gcs, config)
    agentic_service = AgenticService(vertex, config)
    persistence_service = PersistenceService(db)
    
    # Initialize Workflow
    workflow = IngestVideoWorkflow(
        config=config,
        db=db,
        gcs=gcs,
        transcoding_service=transcoding_service,
        asr_service=asr_service,
        agentic_service=agentic_service,
        persistence_service=persistence_service
    )
    
    # Pub/Sub Subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(config.project_id, config.pubsub_subscription_name)
    
    logger.info(f"Listening for messages on {subscription_path}...")

    def callback(message):
        try:
            logger.info(f"Received message: {message.data}")
            data = json.loads(message.data.decode("utf-8"))
            
            # Parse GCS notification
            # Expected format from GCS notification:
            # {
            #   "kind": "storage#object",
            #   "name": "sitcom_clips/S01E01_scene_01.mp4",
            #   "bucket": "lingoscaffold-videos-raw",
            #   ...
            # }
            
            object_name = data.get("name")
            bucket_name = data.get("bucket")
            
            if not object_name or not bucket_name:
                logger.warning("Invalid message format")
                message.ack()
                return

            # Extract video_uid (assuming filename is the UID for now, or generated)
            # In the smart_split.py, we upload as "sitcom_clips/{clip_name}"
            # We can use the filename as video_uid for simplicity in this assignment
            video_uid = os.path.splitext(os.path.basename(object_name))[0]
            
            pubsub_msg = PubSubMessage(
                video_uid=video_uid,
                object_name=object_name,
                bucket_name=bucket_name,
                etag=data.get("etag", "unknown")
            )
            
            # Run workflow in event loop
            asyncio.run_coroutine_threadsafe(workflow.process_message(pubsub_msg), loop)
            
            message.ack()
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            message.nack()

    # Create an event loop for async processing
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        await db.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
