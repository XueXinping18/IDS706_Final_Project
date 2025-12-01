import os
import polars as pl
import textstat
from pathlib import Path
from dotenv import load_dotenv
# Here we use Polaris to do EFL
# Ensure src is in PYTHONPATH
import sys
sys.path.append(str(Path(__file__).parent.parent))

from ingestion_worker.config import Config

def run_etl():
    load_dotenv()
    config = Config.from_env()
    
    db_url = config.db_url
    if not db_url:
        raise ValueError("DATABASE_URL not set")
        
    print(f"Connecting to database...")
    
    # 1. Load Data
    # We need segments to calculate text stats
    query = """
    SELECT 
        v.video_uid,
        s.text,
        s.t_start,
        s.t_end
    FROM segment s
    JOIN video v ON s.video_id = v.id
    """
    
    df = pl.read_database_uri(query, db_url, engine="connectorx")
    
    if df.is_empty():
        print("No data found in database.")
        return

    print(f"Loaded {len(df)} segments.")

    # 2. Aggregation per Video
    # We want to combine all text for a video to calculate overall stats
    
    # Group by video_uid
    video_stats = df.group_by("video_uid").agg([
        pl.col("text").str.concat(" ").alias("full_text"),
        pl.col("t_start").min().alias("start_time"),
        pl.col("t_end").max().alias("end_time"),
        pl.count("text").alias("segment_count")
    ])

    # 3. Feature Engineering
    # We will use map_elements to apply textstat functions
    # Note: map_elements is slower than native expressions but necessary for external libs
    
    def calculate_features(row):
        text = row["full_text"]
        duration_min = (row["end_time"] - row["start_time"]) / 60.0 if row["end_time"] > row["start_time"] else 1.0
        
        # Word count
        word_count = textstat.lexicon_count(text, removepunct=True)
        
        # WPM
        wpm = word_count / duration_min if duration_min > 0 else 0
        
        # Readability (Target Y)
        # Flesch-Kincaid Grade Level: Lower is easier, Higher is harder
        difficulty_score = textstat.flesch_kincaid_grade(text)
        
        # Complexity (Avg Sentence Length) - textstat handles this internally usually, 
        # but we can also just use the score as a proxy.
        
        return {
            "wpm": wpm,
            "word_count": word_count,
            "duration_min": duration_min,
            "difficulty_score": difficulty_score
        }

    # Apply calculation
    # Polars struct unpacking is a bit verbose, so we'll iterate or use map_rows
    # For simplicity and readability in this course project, let's convert to pandas for this row-wise op
    # then back to Polars or just save from Pandas.
    
    pdf = video_stats.to_pandas()
    
    features = pdf.apply(calculate_features, axis=1, result_type="expand")
    result_pdf = pdf.join(features)
    
    # Select final columns
    final_df = result_pdf[["video_uid", "wpm", "word_count", "duration_min", "difficulty_score"]]
    
    print("Calculated features:")
    print(final_df.head())
    
    # 4. Save to CSV
    output_path = Path("analytics_data.csv")
    final_df.to_csv(output_path, index=False)
    print(f"Saved training data to {output_path}")

if __name__ == "__main__":
    run_etl()
