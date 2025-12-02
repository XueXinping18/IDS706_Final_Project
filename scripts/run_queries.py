import asyncio
import json
import os
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.config import Config
from dotenv import load_dotenv

async def run_queries():
    load_dotenv()
    config = Config.from_env()
    db = Database(config.db_url)
    await db.connect()
    
    try:
        print("--- Executing Analytical Queries ---")
        
        # Query 1: Hard Clips (High WPM)
        # This relies only on video and segment tables, which exist.
        # Debug Video Table
        print("Checking Video Table Columns:")
        v_rows = await db.fetch_all("SELECT * FROM video LIMIT 1")
        if v_rows:
            print(dict(v_rows[0]))

        print("\nQuery 1: Identifying 'Hard' Clips (High Speech Rate)")
        # Lower threshold to 50 WPM to see if we get anything
        q1 = """
        SELECT 
            v.title, 
            COUNT(s.id) as total_segments,
            SUM(LENGTH(s.text)) / NULLIF(COALESCE(v.duration_seconds, 300) / 60.0, 0) as wpm
        FROM video v
        JOIN segment s ON v.id = s.video_id
        GROUP BY v.id, v.title, v.duration_seconds
        ORDER BY wpm DESC
        LIMIT 5;
        """
        rows = await db.fetch_all(q1)
        for r in rows:
            print(f"- {r['title']}: {r['wpm']:.2f} WPM ({r['total_segments']} segments)")

        # Query 2: Common Vocabulary/Concepts (Try joining with semantic.fine_unit)
        print("\nQuery 2: Top Knowledge Units (Joining with semantic.fine_unit)")
        try:
            q2 = """
            SELECT 
                f.label, 
                f.kind,
                COUNT(*) as frequency 
            FROM occurrence o
            JOIN semantic.fine_unit f ON o.fine_id = f.id
            GROUP BY f.label, f.kind
            ORDER BY frequency DESC
            LIMIT 10;
            """
            rows = await db.fetch_all(q2)
            if rows:
                for r in rows:
                    print(f"- {r['label']} ({r['kind']}): {r['frequency']}")
            else:
                print("No rows returned from JOIN query.")
        except Exception as e:
            print(f"Failed to query semantic.fine_unit: {e}")
            
            # Fallback to Evidence extraction if table missing
            print("Fallback: Extracting from Evidence JSON...")
            q_fallback = "SELECT evidence FROM occurrence LIMIT 1000"
            occurrences = await db.fetch_all(q_fallback)
            
            concepts = {}
            for row in occurrences:
                try:
                    evidence = json.loads(row['evidence'])
                    rationale = evidence.get('rationale', '')
                    words = rationale.split()
                    for w in words:
                        if len(w) > 4: 
                            concepts[w] = concepts.get(w, 0) + 1
                except:
                    continue
                    
            sorted_concepts = sorted(concepts.items(), key=lambda x: x[1], reverse=True)[:10]
            print("Top keywords in Rationale:")
            for word, count in sorted_concepts:
                print(f"- {word}: {count}")

    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(run_queries())
