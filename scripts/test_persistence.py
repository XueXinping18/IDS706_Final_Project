"""
测试 Persistence Service

运行：python test_persistence.py
"""

from dotenv import load_dotenv
load_dotenv()
import asyncio
from ingestion_worker.config import Config
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.domain.persistence import PersistenceService


async def test_persistence():
    """测试持久化服务"""

    print("\n[测试] Persistence Service")

    # 1. 初始化
    config = Config.from_env()
    db = Database(config.db_url)
    await db.connect()

    persistence = PersistenceService(db)

    # 2. 准备测试数据
    video_id = 1  # 假设已存在

    segments = [
        {
            "start": 0.0,
            "end": 3.5,
            "text": "This is a test segment",
            "lang": "en"
        },
        {
            "start": 3.5,
            "end": 7.0,
            "text": "Another test segment",
            "lang": "en"
        }
    ]

    annotations = [
        {
            "segment_index": 0,
            "fine_id": 101,  # 假设存在
            "span": {"start": 0, "end": 4},
            "rationale": "测试",
            "visual_comprehensibility": 0.8,
            "textual_comprehensibility": 0.7,
            "score": 0.9
        },
        {
            "segment_index": 0,
            "fine_id": 999999,  # 不存在的 fine_id（会被跳过）
            "span": {"start": 5, "end": 9},
            "rationale": "测试",
            "visual_comprehensibility": 0.6,
            "textual_comprehensibility": 0.5
        }
    ]

    # 3. 测试保存
    try:
        stats = await persistence.save_video_analysis(
            video_id=video_id,
            segments=segments,
            annotations=annotations,
            method="gemini_text",
            ontology_ver="gemini-2.0-20250110"
        )

        print(f"\n✓ 保存成功:")
        print(f"  Segments 插入: {stats['segments_inserted']}")
        print(f"  Occurrences 插入: {stats['occurrences_inserted']}")
        print(f"  Occurrences 跳过: {stats['occurrences_skipped']}")

        # 4. 测试更新状态
        await persistence.update_video_status(
            video_id=video_id,
            status="READY",
            hls_path="gs://bucket/video.m3u8",
            transcript_path="gs://bucket/transcript.json"
        )

        print(f"\n✓ 状态更新成功")

    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(test_persistence())