"""
职责：
- 提交 Replicate 预测任务
- 轮询任务状态直到完成

依赖：aiohttp

对外接口：
- async def submit_prediction(model, input_data) -> str (prediction_id)
- async def wait_for_prediction(prediction_id, max_wait_seconds) -> dict

实现：
- 使用 Replicate HTTP API（非官方 SDK，因为官方是同步的）
- 指数退避轮询
"""
import asyncio
import aiohttp
from typing import Optional, Any
from datetime import datetime

from ingestion_worker.config import Config
from ingestion_worker.utils.logging import get_logger


class ReplicateError(Exception):
    """Replicate API 错误"""
    pass


class ReplicateClient:
    """Replicate API 客户端（异步）"""

    # WhisperX 模型
    DEFAULT_MODEL = "victor-upmeet/whisperx:84d2ad2d6194fe98a17d2b60bef1c7f910c46b2f6fd38996ca457afd9c8abfcb"

    def __init__(self, config: Config):
        """
        初始化 Replicate 客户端

        Args:
            config: 系统配置
        """
        self.config = config
        self.api_token = config.replicate_api_token
        self.base_url = "https://api.replicate.com/v1"
        self.logger = get_logger(__name__)

        if not self.api_token or self.api_token == "r8_test":
            self.logger.warning("⚠️  使用测试 token，API 调用将失败")

    async def submit_prediction(
            self,
            model: str,
            input_data: dict[str, Any],
            webhook: Optional[str] = None,
    ) -> str:
        """
        提交预测任务

        Args:
            model: 模型标识符（如 "owner/name:version"）
            input_data: 输入参数
            webhook: 可选的 webhook URL（任务完成时回调）

        Returns:
            prediction_id: 预测任务 ID

        Raises:
            ReplicateError: 提交失败
        """
        url = f"{self.base_url}/predictions"
        headers = {
            "Authorization": f"Token {self.api_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "version": model.split(":")[-1] if ":" in model else model,
            "input": input_data,
        }

        if webhook:
            payload["webhook"] = webhook
            payload["webhook_events_filter"] = ["completed"]

        try:
            async with aiohttp.ClientSession() as session:
                self.logger.info(f"提交 Replicate 任务: {model}")
                self.logger.debug(f"输入参数: {input_data}")

                async with session.post(
                        url,
                        headers=headers,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 401:
                        raise ReplicateError("Replicate API token 无效")
                    elif resp.status == 402:
                        raise ReplicateError("Replicate 账户余额不足")
                    elif resp.status != 201:
                        error_text = await resp.text()
                        raise ReplicateError(f"提交任务失败: HTTP {resp.status}, {error_text}")

                    data = await resp.json()
                    prediction_id = data["id"]

                    self.logger.info(f"✓ 任务已提交: {prediction_id}")
                    return prediction_id

        except aiohttp.ClientError as e:
            self.logger.error(f"网络错误: {e}")
            raise ReplicateError(f"Network error: {e}") from e
        except Exception as e:
            self.logger.error(f"提交任务失败: {e}")
            raise ReplicateError(f"Failed to submit prediction: {e}") from e

    async def get_prediction(self, prediction_id: str) -> dict[str, Any]:
        """
        获取预测任务状态

        Args:
            prediction_id: 预测任务 ID

        Returns:
            预测任务详情

        Raises:
            ReplicateError: 查询失败
        """
        url = f"{self.base_url}/predictions/{prediction_id}"
        headers = {
            "Authorization": f"Token {self.api_token}",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 404:
                        raise ReplicateError(f"预测任务不存在: {prediction_id}")
                    elif resp.status != 200:
                        error_text = await resp.text()
                        raise ReplicateError(f"查询任务失败: HTTP {resp.status}, {error_text}")

                    data = await resp.json()
                    return data

        except aiohttp.ClientError as e:
            self.logger.error(f"网络错误: {e}")
            raise ReplicateError(f"Network error: {e}") from e
        except Exception as e:
            self.logger.error(f"查询任务失败: {e}")
            raise ReplicateError(f"Failed to get prediction: {e}") from e

    async def wait_for_prediction(
            self,
            prediction_id: str,
            max_wait_seconds: int = 1800,  # 30 分钟
            poll_interval_seconds: int = 5,
    ) -> dict[str, Any]:
        """
        轮询等待预测任务完成

        Args:
            prediction_id: 预测任务 ID
            max_wait_seconds: 最大等待时间（秒）
            poll_interval_seconds: 轮询间隔（秒）

        Returns:
            完成的预测任务详情

        Raises:
            ReplicateError: 任务失败或超时
        """
        start_time = datetime.now()
        poll_count = 0

        self.logger.info(f"等待任务完成: {prediction_id} (最多 {max_wait_seconds}s)")

        while True:
            poll_count += 1
            elapsed = (datetime.now() - start_time).total_seconds()

            if elapsed > max_wait_seconds:
                raise ReplicateError(f"任务超时 (>{max_wait_seconds}s): {prediction_id}")

            # 查询状态
            prediction = await self.get_prediction(prediction_id)
            status = prediction.get("status")

            self.logger.debug(f"轮询 #{poll_count}: status={status}, elapsed={elapsed:.1f}s")

            if status == "succeeded":
                self.logger.info(f"✓ 任务完成: {prediction_id} (耗时 {elapsed:.1f}s)")
                return prediction

            elif status == "failed":
                error = prediction.get("error", "Unknown error")
                self.logger.error(f"✗ 任务失败: {error}")
                raise ReplicateError(f"Prediction failed: {error}")

            elif status == "canceled":
                raise ReplicateError("任务已取消")

            elif status in ("starting", "processing"):
                # 任务进行中，继续等待
                await asyncio.sleep(poll_interval_seconds)

            else:
                # 未知状态
                self.logger.warning(f"未知状态: {status}")
                await asyncio.sleep(poll_interval_seconds)

    async def submit_whisperx(
            self,
            audio_url: str,
            language: str = "en",
            align_output: bool = True,
    ) -> str:
        """
        提交 WhisperX 任务（便捷方法）

        Args:
            audio_url: 音频文件 URL（Signed GET URL）
            language: 语言代码（如 'en', 'zh'）
            align_output: 是否对齐输出

        Returns:
            prediction_id

        Raises:
            ReplicateError: 提交失败
        """
        input_data = {
            "audio_file": audio_url,
            "language": language,
            "align_output": align_output,
        }

        # 提交任务
        prediction_id = await self.submit_prediction(
            model=self.DEFAULT_MODEL,
            input_data=input_data,
        )

        return prediction_id