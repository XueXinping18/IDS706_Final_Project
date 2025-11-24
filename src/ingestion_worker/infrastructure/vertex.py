"""
职责：
- 调用 Gemini API（支持 Function Calling）
- 处理 function_call/function_response 循环

依赖：google-cloud-aiplatform
"""
import asyncio
import json
import re
import os
from typing import Callable, Awaitable, Any, Optional
from datetime import timedelta

from vertexai.generative_models import (
    GenerativeModel,
    Part,
    Content,
    Tool,
    GenerationConfig,
    FunctionDeclaration,
    SafetySetting,
    HarmCategory,
    HarmBlockThreshold
)
from vertexai.preview.caching import CachedContent
from google.api_core import exceptions as gcp_exceptions

from ingestion_worker.config import Config
from ingestion_worker.utils.logging import get_logger


class VertexError(Exception):
    """Vertex AI 调用错误"""
    pass


class VertexClient:
    """Gemini API 客户端（纯技术封装）"""

    def __init__(self, config: Config):
        """
        初始化 Vertex AI 客户端

        Args:
            config: 系统配置
        """
        self.config = config
        self.logger = get_logger(__name__)

        try:
            # 初始化 Vertex AI
            import vertexai
            vertexai.init(
                project=config.gcp_project,
                location=config.gcp_region
            )
            self.logger.info(
                f"✓ Vertex AI 初始化成功 "
                f"(project: {config.gcp_project}, region: {config.gcp_region})"
            )
        except Exception as e:
            self.logger.error(f"✗ Vertex AI 初始化失败: {e}")
            raise VertexError(f"Failed to initialize Vertex AI: {e}") from e

    async def create_cached_content(
        self,
        video_uri: Optional[str],
        text_content: str,
        system_instruction: Optional[str] = None,  # [保持] 接收系统指令
        tools: Optional[list[Tool]] = None,
        ttl_seconds: Optional[int] = None
    ) -> CachedContent:
        """
        创建缓存内容（包含视频、文本和tools）
        """
        if ttl_seconds is None:
            ttl_seconds = self.config.gemini_cache_ttl_seconds

        try:
            contents = []

            # 添加视频（如果有）
            if video_uri:
                self.logger.info(f"添加视频到缓存: {video_uri}")
                contents.append(
                    Part.from_uri(video_uri, mime_type="video/mp4")
                )

            # 添加文本内容
            contents.append(Part.from_text(text_content))

            # 在 asyncio 中运行同步代码
            cached_content = await asyncio.to_thread(
                CachedContent.create,
                model_name=self.config.gemini_model,
                contents=[Content(role="user", parts=contents)],
                tools=tools,  # Include tools in cache
                system_instruction=system_instruction,  # [保持] 注入系统指令到 Cache
                ttl=timedelta(seconds=ttl_seconds)
            )

            self.logger.info(
                f"✓ 缓存已创建: {cached_content.name}, TTL={ttl_seconds}s"
            )

            return cached_content

        except gcp_exceptions.InvalidArgument as e:
            self.logger.error(f"参数无效: {e}")
            raise VertexError(f"Invalid argument: {e}") from e
        except gcp_exceptions.ResourceExhausted as e:
            self.logger.error(f"配额耗尽: {e}")
            raise VertexError(f"Quota exceeded: {e}") from e
        except Exception as e:
            self.logger.error(f"创建缓存失败: {e}")
            raise VertexError(f"Failed to create cached content: {e}") from e

    async def call_with_tools(
        self,
        cached_content: Optional[CachedContent],
        prompt: str,
        tools: list[Tool],
        tool_handler: Callable[[str, dict], Awaitable[dict]],
        system_instruction: Optional[str] = None,  # [保持] 用于无缓存模式
        generation_config: Optional[dict] = None,
        trace_context: Optional[dict] = None  # [新增] 追踪上下文
    ) -> dict:
        """
        调用 Gemini 并处理 Function Calling 循环

        Args:
            trace_context: 追踪上下文 {segment_index, segment_text, annotator_kind, video_uid}
        """
        try:
            config = GenerationConfig(
                temperature=0.0,
                max_output_tokens=8192,
                **(generation_config or {})
            )

            # 创建模型
            if cached_content:
                # 缓存模式：system_instruction 和 tools 已经在 cached_content 中
                model = GenerativeModel.from_cached_content(cached_content)
            else:
                # 无缓存模式：显式传入 system_instruction 和 tools
                # Configure safety settings to be permissive
                safety_settings = [
                    SafetySetting(
                        category=HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                        threshold=HarmBlockThreshold.BLOCK_NONE,
                    ),
                    SafetySetting(
                        category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                        threshold=HarmBlockThreshold.BLOCK_NONE,
                    ),
                    SafetySetting(
                        category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                        threshold=HarmBlockThreshold.BLOCK_NONE,
                    ),
                    SafetySetting(
                        category=HarmCategory.HARM_CATEGORY_HARASSMENT,
                        threshold=HarmBlockThreshold.BLOCK_NONE,
                    ),
                ]

                # 实例化模型
                model = GenerativeModel(
                    model_name=self.config.gemini_model,
                    system_instruction=system_instruction,
                    tools=tools,
                    safety_settings=safety_settings
                )

            chat = model.start_chat(response_validation=False)

            # 提取追踪信息
            ctx = trace_context or {}
            video_uid = ctx.get("video_uid", "N/A")
            seg_idx = ctx.get("segment_index", "N/A")
            seg_text = ctx.get("segment_text", "N/A")[:200]  # 截取前200字符
            annotator = ctx.get("annotator_kind", "N/A")
            trace_id = f"[{video_uid}|Seg#{seg_idx}|{annotator}]"

            # ========== 位置 1: 第一次请求前 ==========
            self.logger.info("=" * 80)
            self.logger.info(f"📤 {trace_id} Gemini 请求")
            self.logger.info(f"   Segment: \"{seg_text}\"")
            self.logger.info(f"   Prompt 长度: {len(prompt)} 字符 | 缓存: {cached_content.name if cached_content else 'No cache'}")
            self.logger.info("=" * 80)
            self.logger.debug(f"{trace_id} 完整 Prompt:\n{prompt}")

            # 第一次调用
            # 如果使用 cached_content，tools 已在 cache 中，不能再传
            # 如果不用 cache，传 tools (无缓存模式下 tools 已在 model 初始化时传入)
            response = await asyncio.to_thread(
                chat.send_message,
                prompt,
                generation_config=config
            )

            # ========== 位置 1: 第一次响应后 ==========
            self.logger.info(f"📥 {trace_id} Gemini 第1次响应")
            self.logger.info(f"   Segment: \"{seg_text}\"")
            if response.candidates:
                self.logger.info(f"   Finish reason: {response.candidates[0].finish_reason}")
            self.logger.debug(f"{trace_id} 完整响应对象: {response}")

            # 处理 function calling 循环
            max_iterations = 10  # 防止无限循环
            iteration = 0

            # 记录 fine_id → lemma 的映射（用于最终决策时显示）
            fine_id_to_lemma = {}

            while iteration < max_iterations:
                iteration += 1

                # 检查是否有 function call
                function_calls = self._extract_function_calls(response)

                # ========== 位置 2: 提取 function_calls 后 ==========
                if not function_calls:
                    # 没有 function call，返回最终结果
                    final_result = self._parse_response(response, ctx)
                    self.logger.info("=" * 80)
                    self.logger.info(f"✅ {trace_id} LLM 最终决策")
                    self.logger.info(f"   Segment: \"{seg_text}\"")
                    self.logger.info(f"   标注数量: {len(final_result.get('annotations', []))}")
                    self.logger.info("=" * 80)

                    # 记录最终选择的annotations（显示 lemma 而非 span）
                    for ann_idx, ann in enumerate(final_result.get('annotations', [])):
                        fine_id = ann.get('fine_id')
                        lemma = fine_id_to_lemma.get(fine_id, "未知")
                        self.logger.info(
                            f"   [{ann_idx+1}] \"{lemma}\" → fine_id={fine_id} | "
                            f"rationale: {ann.get('rationale', '')[:120]}"
                        )
                    return final_result

                self.logger.info("=" * 80)
                self.logger.info(f"🔍 {trace_id} LLM 调用工具（第{iteration}轮）")
                self.logger.info(f"   Segment: \"{seg_text}\"")
                self.logger.info(f"   查询数量: {len(function_calls)}")
                for idx, fc in enumerate(function_calls):
                    self.logger.info(f"   [{idx+1}] {fc.name}({dict(fc.args)})")
                self.logger.info("=" * 80)

                # 执行所有 function calls
                function_responses = []
                for idx, fc in enumerate(function_calls):
                    name = fc.name
                    args = dict(fc.args)

                    self.logger.debug(f"调用工具: {name}({args})")

                    try:
                        # ========== 位置 3: 执行工具调用 ==========
                        result = await tool_handler(name, args)

                        # 提取 lemma 并建立 fine_id → lemma 映射
                        if isinstance(result, dict) and "lemma" in result and "candidates" in result:
                            lemma = result["lemma"]
                            candidates = result["candidates"]
                            for cand in candidates:
                                if isinstance(cand, dict) and "fine_id" in cand:
                                    fine_id_to_lemma[cand["fine_id"]] = lemma

                        self.logger.info(f"  ✓ 工具调用成功 [{idx+1}]: {name}")
                        function_responses.append(
                            Part.from_function_response(
                                name=name,
                                response={"result": result}
                            )
                        )
                    except Exception as e:
                        self.logger.error(f"  ✗ 工具调用失败 [{idx+1}]: {name}, 错误: {e}")
                        function_responses.append(
                            Part.from_function_response(
                                name=name,
                                response={"error": str(e)}
                            )
                        )

                # ========== 位置 4: 发送 function_responses 前 ==========
                self.logger.info(f"📤 {trace_id} 发送工具结果给 LLM（第{iteration}轮）")
                self.logger.info(f"   Segment: \"{seg_text}\"")

                # 发送 function responses
                response = await asyncio.to_thread(
                    chat.send_message,
                    function_responses,
                    generation_config=config
                )

                # ========== 位置 4: 收到 Gemini 响应后 ==========
                self.logger.info(f"📥 {trace_id} LLM 第{iteration+1}次响应（处理工具结果后）")
                self.logger.info(f"   Segment: \"{seg_text}\"")
                if response.candidates:
                    self.logger.info(f"   Finish reason: {response.candidates[0].finish_reason}")

            # 达到最大迭代次数
            self.logger.warning(
                f"Function calling 达到最大迭代次数 ({max_iterations})"
            )
            return self._parse_response(response, ctx)

        except gcp_exceptions.DeadlineExceeded as e:
            self.logger.error(f"请求超时: {e}")
            raise VertexError(f"Request timeout: {e}") from e
        except gcp_exceptions.ResourceExhausted as e:
            self.logger.error(f"配额耗尽: {e}")
            raise VertexError(f"Quota exceeded: {e}") from e
        except Exception as e:
            self.logger.error(f"Gemini 调用失败: {e}")
            raise VertexError(f"Failed to call Gemini: {e}") from e

    def _extract_function_calls(self, response) -> list:
        """
        从响应中提取 function calls

        Args:
            response: GenerateContentResponse

        Returns:
            FunctionCall 列表
        """
        function_calls = []

        for candidate in response.candidates:
            for part in candidate.content.parts:
                if hasattr(part, 'function_call') and part.function_call:
                    function_calls.append(part.function_call)

        return function_calls

    def _parse_response(self, response, trace_context: dict = None) -> dict:
        """
        解析 Gemini 响应为 dict

        Args:
            response: GenerateContentResponse

        Returns:
            解析后的 JSON 对象

        Raises:
            VertexError: 解析失败
        """
        try:
            # 获取第一个候选的文本
            if not response.candidates:
                self.logger.error("Gemini 响应中没有 candidates")
                raise VertexError("No candidates in response")

            candidate = response.candidates[0]

            # 记录候选的详细信息（用于诊断）
            self.logger.debug(f"Candidate finish_reason: {candidate.finish_reason}")
            self.logger.debug(f"Candidate content: {candidate.content}")

            if not candidate.content.parts:
                # ⚠️ Gemini 返回空内容 - 视为该 segment 无需标注
                ctx = trace_context or {}
                self.logger.warning("=" * 80)
                self.logger.warning("⚠️ GEMINI 返回空响应（视为无需标注）")
                self.logger.warning(f"📍 Segment #{ctx.get('segment_index', 'N/A')}: \"{ctx.get('segment_text', 'N/A')}\"")
                self.logger.warning(f"📍 Annotator: {ctx.get('annotator_kind', 'N/A')}")
                self.logger.warning(f"📍 finish_reason: {candidate.finish_reason}")
                self.logger.warning("=" * 80)

                # 返回空的 annotations 数组，而不是抛出错误
                return {"annotations": []}

            text = candidate.content.parts[0].text
            
            # Clean up markdown code blocks if present
            if "```" in text:
                # Extract content between first ```json (or just ```) and last ```
                match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
                if match:
                    text = match.group(1)
                else:
                    # Fallback: just strip backticks
                    text = text.replace("```json", "").replace("```", "").strip()

            # Ensure we have a valid JSON object structure
            text = text.strip()
            
            # Debug: Write raw response to file
            try:
                with open("gemini_debug_response.txt", "w", encoding="utf-8") as f:
                    f.write(text)
                self.logger.info(f"Raw response saved to: {os.path.abspath('gemini_debug_response.txt')}")
            except Exception as e:
                self.logger.error(f"Failed to save debug response: {e}")

            if not (text.startswith("{") and text.endswith("}")):
                 # Try to find the first { and last }
                 start = text.find("{")
                 end = text.rfind("}")
                 if start != -1 and end != -1:
                     text = text[start:end+1]

            # 尝试解析 JSON
            data = json.loads(text)

            return data

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 解析失败: {e}")
            if 'text' in locals():
                self.logger.error(f"响应文本（前500字符）: {text[:500]}")
            raise VertexError(f"Invalid JSON response: {e}") from e
        except Exception as e:
            self.logger.error(f"响应解析失败: {e}")
            raise VertexError(f"Failed to parse response: {e}") from e