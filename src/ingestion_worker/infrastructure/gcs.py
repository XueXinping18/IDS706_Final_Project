"""
职责：
- 生成 Signed URL（GET/PUT）
- 读取对象内容（文本/JSON）
- 检查对象是否存在

依赖：google.cloud.storage (同步) + aiofiles（如需异步读写本地）

对外接口：
- generate_signed_url(bucket, object_name, method, ttl, content_type) -> str
- read_text(uri: str) -> str
- read_json(uri: str) -> dict
- exists(uri: str) -> bool

注意：
- google-cloud-storage 是同步库，但生成签名 URL 很快，可在 async 函数中直接调用
- 读取大文件时用 aiohttp 读 Signed URL 更好（避免阻塞）
"""
import json
from datetime import timedelta
from typing import Optional
from google.cloud import storage
import aiohttp

from ingestion_worker.config import Config
from ingestion_worker.utils.logging import get_logger


class GCSError(Exception):
    """GCS 操作错误"""
    pass


class GCSClient:
    """Google Cloud Storage 客户端"""

    def __init__(self, config: Config):
        """
        初始化 GCS 客户端

        Args:
            config: 系统配置
        """
        self.config = config
        self.logger = get_logger(__name__)

        # 创建同步客户端（用于生成签名 URL，操作很快不会阻塞）
        try:
            self.client = storage.Client(project=config.gcp_project)
            self.logger.info(f"✓ GCS 客户端初始化成功 (project: {config.gcp_project})")
        except Exception as e:
            self.logger.error(f"✗ GCS 客户端初始化失败: {e}")
            raise GCSError(f"Failed to initialize GCS client: {e}") from e

    def generate_signed_url(
            self,
            bucket: str,
            object_name: str,
            method: str = "GET",
            ttl_seconds: Optional[int] = None,
            content_type: Optional[str] = None,
    ) -> str:
        """
        生成 Signed URL

        Args:
            bucket: Bucket 名称
            object_name: 对象路径（相对于 bucket root）
            method: HTTP 方法 (GET, PUT, POST, DELETE)
            ttl_seconds: 有效期（秒），默认使用配置值
            content_type: Content-Type（PUT 时建议指定）

        Returns:
            Signed URL 字符串

        Raises:
            GCSError: 生成失败
        """
        if ttl_seconds is None:
            ttl_seconds = self.config.signed_url_ttl_seconds

        try:
            bucket_obj = self.client.bucket(bucket)
            blob = bucket_obj.blob(object_name)

            # 生成签名 URL
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=ttl_seconds),
                method=method,
                content_type=content_type,
            )

            self.logger.debug(f"生成 Signed URL: {method} gs://{bucket}/{object_name}")
            return url

        except Exception as e:
            self.logger.error(f"生成 Signed URL 失败: {e}")
            raise GCSError(f"Failed to generate signed URL: {e}") from e

    async def read_text(self, uri: str) -> str:
        """
        读取对象内容（文本）

        Args:
            uri: GCS URI (gs://bucket/path/to/object)

        Returns:
            对象内容（字符串）

        Raises:
            GCSError: 读取失败
        """
        if not uri.startswith("gs://"):
            raise GCSError(f"Invalid GCS URI: {uri}")

        # 解析 URI
        parts = uri[5:].split("/", 1)
        if len(parts) != 2:
            raise GCSError(f"Invalid GCS URI format: {uri}")

        bucket, object_name = parts

        try:
            # 生成临时 Signed URL
            signed_url = self.generate_signed_url(bucket, object_name, method="GET", ttl_seconds=300)

            # 用 aiohttp 异步下载
            async with aiohttp.ClientSession() as session:
                async with session.get(signed_url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 404:
                        raise GCSError(f"Object not found: {uri}")
                    elif resp.status != 200:
                        raise GCSError(f"Failed to read object: HTTP {resp.status}")

                    content = await resp.text()
                    self.logger.debug(f"读取对象成功: {uri} ({len(content)} 字符)")
                    return content

        except aiohttp.ClientError as e:
            self.logger.error(f"读取对象失败: {e}")
            raise GCSError(f"Failed to read object: {e}") from e
        except Exception as e:
            self.logger.error(f"读取对象失败: {e}")
            raise GCSError(f"Failed to read object: {e}") from e

    async def read_json(self, uri: str) -> dict:
        """
        读取对象内容（JSON）

        Args:
            uri: GCS URI (gs://bucket/path/to/object.json)

        Returns:
            解析后的 JSON 对象

        Raises:
            GCSError: 读取或解析失败
        """
        try:
            content = await self.read_text(uri)
            data = json.loads(content)
            self.logger.debug(f"解析 JSON 成功: {uri}")
            return data
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 解析失败: {e}")
            raise GCSError(f"Invalid JSON content: {e}") from e

    async def exists(self, uri: str) -> bool:
        """
        检查对象是否存在

        Args:
            uri: GCS URI (gs://bucket/path/to/object)

        Returns:
            True 如果对象存在，否则 False
        """
        if not uri.startswith("gs://"):
            raise GCSError(f"Invalid GCS URI: {uri}")

        parts = uri[5:].split("/", 1)
        if len(parts) != 2:
            raise GCSError(f"Invalid GCS URI format: {uri}")

        bucket, object_name = parts

        try:
            bucket_obj = self.client.bucket(bucket)
            blob = bucket_obj.blob(object_name)

            # 使用 HEAD 请求检查（不下载内容）
            exists = blob.exists()
            self.logger.debug(f"对象存在检查: {uri} -> {exists}")
            return exists

        except Exception as e:
            self.logger.error(f"检查对象存在性失败: {e}")
            # 检查失败默认返回 False（而不是抛异常）
            return False

    def parse_uri(self, uri: str) -> tuple[str, str]:
        """
        解析 GCS URI

        Args:
            uri: GCS URI (gs://bucket/path/to/object)

        Returns:
            (bucket, object_name) 元组

        Raises:
            GCSError: URI 格式无效
        """
        if not uri.startswith("gs://"):
            raise GCSError(f"Invalid GCS URI (must start with gs://): {uri}")

        parts = uri[5:].split("/", 1)
        if len(parts) != 2:
            raise GCSError(f"Invalid GCS URI format: {uri}")

        return parts[0], parts[1]