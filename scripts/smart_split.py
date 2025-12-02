#!/usr/bin/env python3
"""
Smart Video Splitter
-------------------
Splits a long video into thematically coherent clips using AI.

Workflow:
1. Upload local video to GCS (temp).
2. Parse local transcript (SRT) OR Transcribe using WhisperX.
3. Analyze transcript with Gemini to find scene boundaries.
4. Split video using FFmpeg.
5. Upload clips to GCS (triggering the main Ingestion Worker).

Usage:
    python scripts/smart_split.py path/to/video.mp4 --transcript path/to/transcript.srt
"""
import asyncio
import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from scenedetect import detect, ContentDetector

from dotenv import load_dotenv

# Ensure src is in PYTHONPATH
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from ingestion_worker.config import Config
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.infrastructure.replicate import ReplicateClient
from ingestion_worker.infrastructure.vertex import VertexClient
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.utils.logging import setup_logging, get_logger
import uuid

logger = get_logger("smart_split")

class SmartSplitter:
    def __init__(self):
        load_dotenv()
        self.config = Config.from_env()
        setup_logging()
        
        self.gcs = GCSClient(self.config)
        self.replicate = ReplicateClient(self.config)
        self.vertex = VertexClient(self.config)
        
        self.temp_bucket = self.config.raw_bucket
        self.output_bucket = self.config.raw_bucket
        
        self.db = Database(self.config.db_url)



    async def run(self, video_path: str, transcript_path: Optional[str] = None, upload: bool = False):
        video_path = Path(video_path)
        if not video_path.exists():
            raise FileNotFoundError(f"Video not found: {video_path}")
            
        logger.info(f"🎬 Processing video: {video_path.name}")
        
        # Connect to DB
        await self.db.connect()
        try:
            # 1. Detect Visual Cuts (SceneDetect)
            visual_cuts = self._detect_visual_cuts(video_path)
            
            # 2. Get Transcript (Local or Remote)
            if transcript_path:
                transcript_path = Path(transcript_path)
                if not transcript_path.exists():
                    raise FileNotFoundError(f"Transcript not found: {transcript_path}")
                segments = self._parse_srt(transcript_path)
            else:
                gcs_uri = await self._upload_temp(video_path)
                segments = await self._transcribe(gcs_uri)
            
            # 3. Analyze (Gemini)
            semantic_scenes = await self._analyze_scenes(segments)
            
            # 4. Snap to Visual Cuts
            final_scenes = self._snap_to_cuts(semantic_scenes, visual_cuts)
            
            # 5. Split & Upload
            await self._split_and_upload(video_path, final_scenes, upload)
            
            logger.info("🎉 All Done!")
        finally:
            await self.db.close()

    def _detect_visual_cuts(self, video_path: Path) -> List[float]:
        """Detect visual scene changes using PySceneDetect"""
        logger.info("Detecting visual cuts (this may take a while)...")
        scene_list = detect(str(video_path), ContentDetector(threshold=27.0))
        # Return start times of new scenes (excluding 0.0)
        cuts = [scene[0].get_seconds() for scene in scene_list]
        logger.info(f"Detected {len(cuts)} visual cuts.")
        return cuts

    def _fmt_time(self, seconds: float) -> str:
        """Format seconds as MM:SS (seconds)"""
        m, s = divmod(seconds, 60)
        return f"{int(m):02d}:{s:04.1f} ({seconds:.1f}s)"

    def _snap_to_cuts(self, scenes: List[Dict], cuts: List[float], threshold: float = 5.0) -> List[Dict]:
        """Snap semantic scene boundaries to the nearest visual cut"""
        logger.info(f"Snapping {len(scenes)} scenes to {len(cuts)} visual cuts (threshold={threshold}s)...")
        
        # Visualization of alignment (User Request)
        logger.info("="*80)
        logger.info("ALIGNMENT VISUALIZATION (Semantic vs Visual)")
        logger.info("="*80)
        
        for i, scene in enumerate(scenes, 1):
            s_start, s_end = scene["start"], scene["end"]
            logger.info(f"🎬 Semantic #{i}: [{self._fmt_time(s_start)} - {self._fmt_time(s_end)}] '{scene['title']}'")
            
            # Find visual cuts strictly within the scene to show granularity
            inner_cuts = [c for c in cuts if s_start < c < s_end]
            
            # Find nearest boundary candidates
            nearest_start = min(cuts, key=lambda x: abs(x - s_start))
            nearest_end = min(cuts, key=lambda x: abs(x - s_end))
            
            # Log inner cuts (granularity)
            if inner_cuts:
                # Group them for cleaner display if too many
                cut_str = ", ".join([f"{c:.1f}s" for c in inner_cuts])
                logger.info(f"\t├── 🎞️  Inner Visual Cuts: {cut_str}")
            else:
                logger.info(f"\t├── 🎞️  (No visual cuts inside this scene)")
                
            # Log boundary alignment
            logger.info(f"\t├── 📍 Start Snap: {self._fmt_time(s_start)} -> {self._fmt_time(nearest_start)} (Diff: {nearest_start-s_start:+.1f}s)")
            logger.info(f"\t└── 📍 End Snap:   {self._fmt_time(s_end)} -> {self._fmt_time(nearest_end)} (Diff: {nearest_end-s_end:+.1f}s)")

        logger.info("="*80)

        snapped_scenes = []
        for scene in scenes:
            original_start = scene["start"]
            original_end = scene["end"]
            
            # Find nearest cut for start
            nearest_start = min(cuts, key=lambda x: abs(x - original_start))
            if abs(nearest_start - original_start) <= threshold:
                new_start = nearest_start
            else:
                new_start = original_start
                
            # Find nearest cut for end
            nearest_end = min(cuts, key=lambda x: abs(x - original_end))
            if abs(nearest_end - original_end) <= threshold:
                new_end = nearest_end
            else:
                new_end = original_end
            
            snapped_scenes.append({
                **scene,
                "start": new_start,
                "end": new_end
            })
            
        return snapped_scenes

    async def _upload_temp(self, local_path: Path) -> str:
        """Upload full video to GCS temp folder"""
        blob_name = f"temp_full_episodes/{local_path.name}"
        logger.info(f"Uploading to gs://{self.temp_bucket}/{blob_name}...")
        
        bucket = self.gcs.client.bucket(self.temp_bucket)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            blob.upload_from_filename(str(local_path))
            logger.info("Upload complete.")
        else:
            logger.info("File already exists, skipping upload.")
            
        return f"gs://{self.temp_bucket}/{blob_name}"

    def _parse_srt(self, srt_path: Path) -> List[Dict]:
        """Parse SRT file into segments"""
        logger.info(f"Parsing SRT file: {srt_path}")
        
        with open(srt_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        # Regex for SRT blocks
        # 1
        # 00:00:02,920 --> 00:00:04,629
        # Text
        pattern = re.compile(r'(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n((?:(?!\n\n).)*)', re.DOTALL)
        
        segments = []
        for match in pattern.finditer(content):
            start_str = match.group(2).replace(',', '.')
            end_str = match.group(3).replace(',', '.')
            text = match.group(4).replace('\n', ' ').strip()
            
            # Convert timestamp to seconds
            start = self._timestamp_to_seconds(start_str)
            end = self._timestamp_to_seconds(end_str)
            
            segments.append({
                "start": start,
                "end": end,
                "text": text
            })
            
        logger.info(f"Parsed {len(segments)} segments from SRT.")
        return segments

    def _timestamp_to_seconds(self, timestamp: str) -> float:
        """Convert HH:MM:SS.mmm to seconds"""
        h, m, s = timestamp.split(':')
        return int(h) * 3600 + int(m) * 60 + float(s)

    async def _transcribe(self, gcs_uri: str) -> List[Dict]:
        """Transcribe using WhisperX via Replicate"""
        logger.info("Transcribing with WhisperX...")
        bucket_name, object_name = self.gcs.parse_uri(gcs_uri)
        signed_url = self.gcs.generate_signed_url(
            bucket_name, object_name, method="GET", ttl_seconds=3600
        )
        prediction_id = await self.replicate.submit_whisperx(signed_url)
        result = await self.replicate.wait_for_prediction(prediction_id)
        output = result.get("output", {})
        segments = output.get("segments", [])
        logger.info(f"Transcription complete: {len(segments)} segments found.")
        return segments

    async def _analyze_scenes(self, segments: List[Dict]) -> List[Dict]:
        """Ask Gemini to identify scene boundaries"""
        logger.info("Analyzing scenes with Gemini...")
        
        transcript_text = "\n".join([
            f"[{s['start']:.1f} - {s['end']:.1f}] {s['text']}" 
            for s in segments
        ])
        
        if len(transcript_text) > 100000:
            logger.warning("Transcript too long, truncating...")
            transcript_text = transcript_text[:100000]

        prompt = """
        Analyze the following transcript of a sitcom episode. 
        **Goal**: Split the episode into coherent, standalone video clips that tell a complete mini-story.
        
        - You should group continuous conversations or related actions into a single scene, even if they move slightly (e.g., walking from kitchen to living room).
        - Split when there is a **hard cut** to a completely different storyline, set of characters, or significant time jump.
        
        Rules:
        - **Duration**: Scenes are typically 1-5 minutes. Merge very short fragments (<30s) into the adjacent scene.- **Merge Aggressively**: If two consecutive segments involve the same characters discussing the same topic, MERGE THEM, even if there is a small gap or minor location shift.
        - **Continuous Coverage**: Ensure coverage is continuous where possible, but skip intro/outro songs.
        """
        
        full_prompt = f"{prompt}\n\nTRANSCRIPT:\n{transcript_text}"
        
        async def dummy_handler(name, args): return {}
        
        # Define JSON Schema for Controlled Generation
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "scenes": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "start": {"type": "NUMBER"},
                            "end": {"type": "NUMBER"},
                            "title": {"type": "STRING"},
                            "summary": {"type": "STRING"}
                        },
                        "required": ["start", "end", "title", "summary"]
                    }
                }
            },
            "required": ["scenes"]
        }

        response = await self.vertex.call_with_tools(
            cached_content=None,
            prompt=full_prompt,
            tools=[], 
            tool_handler=dummy_handler,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": response_schema
            }
        )
        
        scenes = response.get("scenes", [])
        logger.info(f"Identified {len(scenes)} scenes.")
        return scenes

    async def _split_and_upload(self, original_video: Path, scenes: List[Dict], upload: bool):
        """Split video using FFmpeg and upload clips (Parallel & Resumable)"""
        logger.info("Splitting video and uploading clips...")
        
        output_dir = original_video.parent / "clips"
        output_dir.mkdir(exist_ok=True)
        
        # Concurrency Control
        semaphore = asyncio.Semaphore(4)  # Max 4 parallel ffmpeg/upload jobs
        
        # Phase 1: Generate all clips locally
        logger.info("Phase 1: Generating all clips locally...")
        
        async def generate_clip(idx: int, scene: Dict):
            async with semaphore:
                start = scene["start"]
                end = scene["end"]
                duration = end - start
                safe_title = "".join([c if c.isalnum() else "_" for c in scene["title"]])
                clip_name = f"{original_video.stem}_scene_{idx+1:02d}_{safe_title}.mp4"
                clip_path = output_dir / clip_name
                
                logger.info(f"Generating Scene {idx+1}: {scene['title']} ({start:.1f}s - {end:.1f}s)")
                
                if not clip_path.exists():
                    cmd = [
                        "ffmpeg", "-y",
                        "-ss", str(start),
                        "-i", str(original_video),
                        "-t", str(duration),
                        "-c:v", "libx264", "-c:a", "aac",
                        "-ac", "2",
                        "-preset", "fast",
                        str(clip_path)
                    ]
                    
                    try:
                        await asyncio.to_thread(
                            subprocess.run, 
                            cmd, 
                            check=True, 
                            stdout=subprocess.DEVNULL, 
                            stderr=subprocess.DEVNULL
                        )
                    except subprocess.CalledProcessError as e:
                        logger.error(f"FFmpeg failed for scene {idx+1}: {e}")
                        return None
                return clip_path

        # Run generation tasks
        gen_tasks = [generate_clip(i, scene) for i, scene in enumerate(scenes)]
        await asyncio.gather(*gen_tasks)
        logger.info("✓ Phase 1 Complete: All clips generated.")

        if not upload:
            logger.info("Upload skipped. Clips are saved in: " + str(output_dir))
            return

        # Phase 2: Upload all clips
        logger.info("Phase 2: Uploading clips to GCS...")
        
        async def upload_clip(idx: int, scene: Dict):
            async with semaphore:
                safe_title = "".join([c if c.isalnum() else "_" for c in scene["title"]])
                clip_name = f"{original_video.stem}_scene_{idx+1:02d}_{safe_title}.mp4"
                clip_path = output_dir / clip_name
                blob_name = f"sitcom_clips/{clip_name}"
                
                if not clip_path.exists():
                    logger.warning(f"Skipping upload for Scene {idx+1}: File not found")
                    return

                bucket = self.gcs.client.bucket(self.output_bucket)
                blob = bucket.blob(blob_name)
                
                if blob.exists():
                    logger.info(f"⏭️  Skipping Scene {idx+1}: Already exists in GCS")
                    return

                logger.info(f"Uploading Scene {idx+1}...")
                blob.metadata = {
                    "source_video": original_video.name,
                    "scene_title": scene["title"],
                    "scene_summary": scene["summary"]
                }
                
                await asyncio.to_thread(blob.upload_from_filename, str(clip_path))
                logger.info(f"🚀 Uploaded: gs://{self.output_bucket}/{blob_name}")

        # Run upload tasks
        upload_tasks = [upload_clip(i, scene) for i, scene in enumerate(scenes)]
        await asyncio.gather(*upload_tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Video Splitter")
    parser.add_argument("video_path", help="Path to local video file")
    parser.add_argument("--transcript", help="Path to local SRT transcript", required=False)
    parser.add_argument("--upload", action="store_true", help="Upload clips to GCS")
    args = parser.parse_args()
    
    splitter = SmartSplitter()
    asyncio.run(splitter.run(args.video_path, args.transcript, args.upload))
