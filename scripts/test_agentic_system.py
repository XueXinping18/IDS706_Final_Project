"""
Agentic System 真实集成测试

测试策略：
- 不使用 Mock
- 使用真实的数据库、Gemini API、Lark Webhook
- 测试小数据集（1-3 个 segments）
- 验证端到端流程

前置条件：
1. 配置 .env 文件（DATABASE_URL, GEMINI_MODEL, ERROR_WEBHOOK_URL 等）
2. 数据库中有测试数据（semantic.fine_unit 表）
3. Lark Webhook 可用

运行方式：
    python test_agentic_real.py
"""

import asyncio
import sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()  # 这行很重要！

# 测试数据
TEST_VIDEO_UID = f"test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

TEST_SEGMENTS = [
    {
        "start": 0.0,
        "end": 3.5,
        "text": "I want to give up learning English"
    },
    {
        "start": 3.5,
        "end": 7.0,
        "text": "The cat is running very fast"
    },
    {
        "start": 7.0,
        "end": 10.5,
        "text": "She made a big mistake yesterday"
    }
]


async def test_database_connection():
    """测试 1: 数据库连接"""
    print("\n[测试 1] 数据库连接")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.database import Database

    config = Config.from_env()
    db = Database(config.db_url, pool_size=5)

    try:
        await db.connect()
        print("  ✓ 数据库连接成功")

        # 测试查询
        result = await db.fetch_one("SELECT COUNT(*) as count FROM semantic.fine_unit WHERE status = 'active'")
        count = result["count"]
        print(f"  ✓ 数据库中有 {count} 个 active semantic.fine_unit")

        if count == 0:
            print("  ⚠️  警告：数据库中没有 semantic.fine_unit，测试可能无法找到候选")

        await db.close()
        return True

    except Exception as e:
        print(f"  ✗ 数据库连接失败: {e}")
        return False


async def test_mcp_tools_real_query():
    """测试 2: MCP 工具真实查询"""
    print("\n[测试 2] MCP 工具真实查询")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.database import Database
    from ingestion_worker.domain.agentic.mcp_tools import MCPTools

    config = Config.from_env()
    db = Database(config.db_url)
    await db.connect()

    mcp = MCPTools(db)

    try:
        # 测试查询常见单词
        print("  - 查询单词 'run' (verb)...")
        result = await mcp.query_fine_units(
            lemma="run",
            kind="word_sense",
            pos="v",
            lang="en"
        )
        print(f"    找到 {len(result.candidates)} 个候选")
        print(f"    result.found = {result.found}")

        # 测试查询短语
        print("  - 查询短语 'give up'...")
        result2 = await mcp.query_fine_units(
            lemma="give up",
            kind="phrase_sense",
            lang="en"
        )
        print(f"    找到 {len(result2.candidates)} 个候选")
        print(f"    result.found = {result2.found}")

        # 测试查询不存在的词
        print("  - 查询不存在的词 'xyzabc123'...")
        result3 = await mcp.query_fine_units(
            lemma="xyzabc123",
            kind="word_sense",
            lang="en"
        )
        print(f"    找到 {len(result3.candidates)} 个候选")
        print(f"    result.found = {result3.found}")
        assert result3.found == False

        print("  ✓ MCP 工具查询正常")

        await db.close()
        return True

    except Exception as e:
        print(f"  ✗ MCP 查询失败: {e}")
        await db.close()
        return False


async def test_lark_real_notification():
    """测试 3: Lark 真实通知"""
    print("\n[测试 3] Lark 真实通知")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.lark import LarkClient, LarkMessageType

    config = Config.from_env()

    if not config.error_webhook_url:
        print("  ⚠️  跳过：未配置 ERROR_WEBHOOK_URL")
        return True

    lark = LarkClient(config)

    try:
        # 发送测试通知
        print(f"  - 发送测试通知到: {config.error_webhook_url[:50]}...")

        success = await lark.send_notification(
            message_type=LarkMessageType.INFO,
            title="测试通知",
            content={
                "测试ID": TEST_VIDEO_UID,
                "测试时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            metadata={
                "说明": "这是自动化测试发送的消息"
            }
        )

        if success:
            print("  ✓ Lark 通知发送成功（请检查飞书群）")
            return True
        else:
            print("  ✗ Lark 通知发送失败（返回 False）")
            return False

    except Exception as e:
        print(f"  ✗ Lark 通知失败: {e}")
        return False


async def test_vertex_simple_call():
    """测试 4: Vertex AI 简单调用"""
    print("\n[测试 4] Vertex AI 简单调用")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.vertex import VertexClient

    config = Config.from_env()
    vertex = VertexClient(config)

    try:
        # 创建纯文本缓存 (需要至少 1024 tokens)
        print("  - 创建纯文本缓存...")

        # 生成足够长的文本内容 (约1024+ tokens)
        long_text = """
        The quick brown fox jumps over the lazy dog. This is a test sentence.
        """ * 200  # 重复200次确保超过1024 tokens

        cached_content = await vertex.create_cached_content(
            video_uri=None,
            text_content=long_text,
            tools=None,  # No tools for this simple test
            ttl_seconds=600
        )

        print(f"  ✓ 缓存创建成功: {cached_content.name}")

        # 简单调用
        print("  - 发送简单查询...")

        async def dummy_handler(name, args):
            return {"result": "ok"}

        response = await vertex.call_with_tools(
            cached_content=cached_content,
            prompt="Say hello",
            tools=[],
            tool_handler=dummy_handler,
            generation_config=None
        )

        print(f"  ✓ Gemini 响应成功")

        return True

    except Exception as e:
        print(f"  ✗ Vertex AI 调用失败: {e}")
        return False


async def test_orchestrator_full_flow():
    """测试 5: Orchestrator 完整流程（真实 API）"""
    print("\n[测试 5] Orchestrator 完整流程（真实 API）")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.vertex import VertexClient
    from ingestion_worker.infrastructure.database import Database
    from ingestion_worker.infrastructure.lark import LarkClient
    from ingestion_worker.domain.agentic.orchestrators import AgenticOrchestrator

    config = Config.from_env()

    # 初始化所有组件
    print("  - 初始化组件...")
    vertex = VertexClient(config)
    db = Database(config.db_url)
    await db.connect()
    lark = LarkClient(config)

    orchestrator = AgenticOrchestrator(
        vertex=vertex,
        db=db,
        lark=lark,
        config=config
    )

    try:
        # 只测试第一个 segment（减少成本和时间）
        test_segments = [TEST_SEGMENTS[0]]

        print(f"  - 处理测试视频: {TEST_VIDEO_UID}")
        print(f"  - Segments 数量: {len(test_segments)}")
        print(f"  - 文本: '{test_segments[0]['text']}'")

        # 执行处理
        annotations, method, version = await orchestrator.process_video(
            video_uid=TEST_VIDEO_UID,
            video_uri=None,  # 纯文本模式
            segments=test_segments
        )

        # 验证结果
        print(f"\n  结果:")
        print(f"    Method: {method}")
        print(f"    Version: {version}")
        print(f"    Annotations: {len(annotations)}")

        if annotations:
            print(f"\n  示例 Annotation:")
            ann = annotations[0]
            print(f"    segment_index: {ann.get('segment_index')}")
            print(f"    fine_id: {ann.get('fine_id')}")
            print(f"    span: {ann.get('span')}")
            print(f"    rationale: {ann.get('rationale', '')[:50]}...")
            print(f"    visual_comprehensibility: {ann.get('visual_comprehensibility')}")
            print(f"    textual_comprehensibility: {ann.get('textual_comprehensibility')}")

        print("\n  ✓ Orchestrator 完整流程成功")
        print("  ✓ 请检查飞书群是否收到未找到通知（如有）")

        await db.close()
        return True

    except Exception as e:
        print(f"  ✗ Orchestrator 流程失败: {e}")
        import traceback
        traceback.print_exc()
        await db.close()
        return False


async def test_orchestrator_with_unknown_phrase():
    """测试 6: Orchestrator 处理未知短语（应该发送 Lark 通知）"""
    print("\n[测试 6] Orchestrator 处理未知短语（触发 Lark 通知）")

    from ingestion_worker.config import Config
    from ingestion_worker.infrastructure.vertex import VertexClient
    from ingestion_worker.infrastructure.database import Database
    from ingestion_worker.infrastructure.lark import LarkClient
    from ingestion_worker.domain.agentic.orchestrators import AgenticOrchestrator

    config = Config.from_env()

    if not config.error_webhook_url:
        print("  ⚠️  跳过：未配置 ERROR_WEBHOOK_URL")
        return True

    # 初始化组件
    vertex = VertexClient(config)
    db = Database(config.db_url)
    await db.connect()
    lark = LarkClient(config)

    orchestrator = AgenticOrchestrator(
        vertex=vertex,
        db=db,
        lark=lark,
        config=config
    )

    try:
        # 构造包含生僻短语的 segment
        unknown_phrase_segment = {
            "start": 0.0,
            "end": 5.0,
            "text": "I want to xyzabc123test this unknown phrase"
        }

        print(f"  - 处理包含未知短语的 segment")
        print(f"  - 文本: '{unknown_phrase_segment['text']}'")
        print(f"  - 期望: Gemini 会尝试查询 'xyzabc123test'，查不到，触发 Lark 通知")

        annotations, method, version = await orchestrator.process_video(
            video_uid=f"{TEST_VIDEO_UID}-unknown",
            video_uri=None,
            segments=[unknown_phrase_segment]
        )

        print(f"\n  ✓ 处理完成")
        print(f"  ✓ 请检查飞书群是否收到'短语未匹配'或'单词未匹配'通知")

        await db.close()
        return True

    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
        await db.close()
        return False


async def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("Agentic System 真实集成测试")
    print("=" * 70)
    print(f"测试 ID: {TEST_VIDEO_UID}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    tests = [
        ("数据库连接", test_database_connection),
        ("MCP 真实查询", test_mcp_tools_real_query),
        ("Lark 真实通知", test_lark_real_notification),
        ("Vertex AI 简单调用", test_vertex_simple_call),
        ("Orchestrator 完整流程", test_orchestrator_full_flow),
        ("Orchestrator 未知短语（触发通知）", test_orchestrator_with_unknown_phrase),
    ]

    results = []

    for name, test_func in tests:
        try:
            success = await test_func()
            results.append((name, success))
        except Exception as e:
            print(f"  ✗ 测试崩溃: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))

        # 测试间隔（避免 API 限流）
        await asyncio.sleep(1)

    # 打印汇总
    print("\n" + "=" * 70)
    print("测试结果汇总")
    print("=" * 70)

    passed = sum(1 for _, success in results if success)
    failed = len(results) - passed

    for name, success in results:
        status = "✓ 通过" if success else "✗ 失败"
        print(f"  {status}: {name}")

    print(f"\n总计: {passed} 通过, {failed} 失败")
    print("=" * 70)

    return failed == 0


if __name__ == "__main__":
    # 检查环境变量
    import os

    required_vars = [
        "DATABASE_URL",
        "GCP_PROJECT",
        "GCP_REGION",
        "GEMINI_MODEL"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print(f"错误：缺少环境变量: {', '.join(missing)}")
        print("请配置 .env 文件")
        sys.exit(1)

    # 运行测试
    success = asyncio.run(run_all_tests())

    # 退出码
    sys.exit(0 if success else 1)