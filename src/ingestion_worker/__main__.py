"""
Ingestion Worker 主入口（Push 模式）

启动 FastAPI 服务，接收 Pub/Sub Push 请求
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from ingestion_worker.config import Config
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.infrastructure.lark import LarkClient
from ingestion_worker.infrastructure.vertex import VertexClient
from ingestion_worker.infrastructure.transcoder import TranscoderClient
from ingestion_worker.infrastructure.replicate import ReplicateClient
from ingestion_worker.domain.agentic.orchestrators import AgenticOrchestrator
from ingestion_worker.domain.persistence import PersistenceService
from ingestion_worker.application.workflow import IngestVideoWorkflow
from ingestion_worker.api.webhooks import router
from ingestion_worker.utils.logging import setup_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    # Startup
    logger.info("🚀 Ingestion Worker 启动中...")

    # 1. 加载配置
    config = Config.from_env()
    config.validate()
    setup_logging()

    # 2. 初始化基础设施
    db = Database(config.db_url)
    await db.connect()

    gcs = GCSClient(config)
    lark = LarkClient(config)
    vertex = VertexClient(config)
    transcoder = TranscoderClient(config)
    replicate = ReplicateClient(config)

    # 3. 初始化 Agentic
    agentic = AgenticOrchestrator(vertex, db, lark, config)

    # 4. 初始化持久化服务
    persistence = PersistenceService(db)

    # 5. 初始化 Workflow
    workflow = IngestVideoWorkflow(
        config=config,
        db=db,
        gcs=gcs,
        lark=lark,
        transcoder=transcoder,
        replicate=replicate,
        agentic=agentic,
        persistence=persistence,
    )

    # 6. 注入到 App State（供 Dependency Injection 使用）
    app.state.workflow = workflow

    logger.info("✓ Ingestion Worker 启动完成")

    yield  # Application runs here

    # Shutdown
    logger.info("正在关闭 Ingestion Worker...")
    if hasattr(app.state, 'workflow'):
        await app.state.workflow.db.close()


# Create FastAPI app with lifespan
app = FastAPI(title="Ingestion Worker", lifespan=lifespan)

# 注册路由
app.include_router(router)


# 健康检查端点
@app.get("/health")
async def health():
    return {"status": "ok"}


# 测试数据库连接端点
@app.get("/test/db")
async def test_db():
    """Test database connection"""
    if not hasattr(app.state, 'workflow'):
        return {"error": "Workflow not initialized"}

    try:
        result = await app.state.workflow.db.fetch_one(
            "SELECT COUNT(*) as count FROM semantic.fine_unit"
        )
        return {"status": "ok", "fine_units": result["count"]}
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)