"""
Agentic Workflow 编排器

职责：
- 创建 Cached Content
- 并发处理 segments
- 降级策略（多模态 → 纯文本）
- 聚合结果
- 决定何时发送通知

依赖：
- VertexClient (Infrastructure)
- Database (Infrastructure)
- LarkClient (Infrastructure)
- MCPTools (Domain)
- Annotators (Domain)
"""
import asyncio
from typing import Optional

from vertexai.preview.caching import CachedContent

from ingestion_worker.config import Config
from ingestion_worker.infrastructure.vertex import VertexClient, VertexError
from ingestion_worker.infrastructure.database import Database
from ingestion_worker.infrastructure.lark import LarkClient
from ingestion_worker.domain.agentic.mcp_tools import MCPTools
from ingestion_worker.domain.agentic.annotators.base import BaseAnnotator
from ingestion_worker.domain.agentic.annotators.word import WordAnnotator
from ingestion_worker.domain.agentic.annotators.phrase import PhraseAnnotator
from ingestion_worker.utils.logging import get_logger


class AgenticOrchestrator:
    """Agentic 工作流编排器"""

    # [新增] 集中定义的系统指令，确保模型"牢记"工具使用规则
    SYSTEM_INSTRUCTION = """
你是视频内容分析专家。你将看到完整的视频和字幕（或仅字幕）。

任务：
逐个处理 Segment，识别其中的单词和短语，并**必须调用 query_fine_units 工具**查询数据库中的候选，注意查询数据库时往往需要单词短语的原型。
根据视频画面和文本上下文进行消歧，并评估 comprehensibility。

核心原则：
1. **短语优先于单词**：先识别短语（如 "give up"），再识别单词。
2. **必须调用工具**：不要凭空生成 fine_id，必须通过 query_fine_units 获取候选列表。
3. **候选为空则跳过**：如果没有查询到候选，不输出该词的 annotation。
4. **评分要客观**：从语言学习者角度考虑，高分表示更适合学习。
5. **JSON 输出**：严格遵循指定的 JSON Schema 输出结果。

记住：
- 每次只处理一个 segment。
- Span 是相对于该 segment 文本的字符偏移。
"""

    def __init__(
        self,
        vertex: VertexClient,
        db: Database,
        lark: LarkClient,
        config: Config
    ):
        """
        初始化编排器

        Args:
            vertex: Vertex AI 客户端
            db: 数据库客户端
            lark: Lark 通知客户端
            config: 系统配置
        """
        self.vertex = vertex
        self.lark = lark
        self.config = config
        self.logger = get_logger(__name__)

        # 初始化 MCP 工具
        self.mcp = MCPTools(db, config.gemini_model)

        # 初始化标注器
        self.word_annotator = WordAnnotator()
        self.phrase_annotator = PhraseAnnotator()

    async def process_video(
        self,
        video_uid: str,
        video_uri: Optional[str],
        segments: list[dict]
    ) -> tuple[list[dict], str, str]:
        """
        处理整个视频（主入口）

        Args:
            video_uid: 视频唯一标识
            video_uri: GCS 视频 URI（或 None 表示纯文本模式）
            segments: WhisperX 的 segments 列表

        Returns:
            (annotations, method, ontology_ver) 元组
            - annotations: 标注列表
            - method: 使用的方法（'gemini_video' | 'gemini_text' | 'gemini_nocache'）
            - ontology_ver: 本体版本

        Raises:
            VertexError: Gemini API 完全不可用时
        """
        self.logger.info(
            f"开始处理视频: {video_uid}, "
            f"segments={len(segments)}, "
            f"has_video={video_uri is not None}"
        )

        # 1. 创建缓存内容（带降级）
        cached_content, method = await self._create_cached_content_with_fallback(
            video_uri, segments
        )

        # 2. 并发处理所有 segments
        annotations = await self._process_segments_concurrent(
            cached_content, segments, video_uid
        )

        # 3. 返回结果（使用当前 Gemini 模型版本作为 ontology_ver）
        ontology_ver = self.config.gemini_model

        self.logger.info(
            f"✓ 视频处理完成: {video_uid}, "
            f"annotations={len(annotations)}, "
            f"method={method}, "
            f"ontology_ver={ontology_ver}"
        )

        return annotations, method, ontology_ver

    async def _create_cached_content_with_fallback(
        self,
        video_uri: Optional[str],
        segments: list[dict]
    ) -> tuple[Optional[CachedContent], str]:
        """
        创建缓存内容（带降级策略）

        降级顺序：
        1. 多模态缓存（视频 + 文本）
        2. 纯文本缓存（仅文本）
        3. 无缓存模式

        Args:
            video_uri: GCS 视频 URI（或 None）
            segments: Segments 列表

        Returns:
            (cached_content, method) 元组
        """
        # 尝试 1: 多模态缓存
        if video_uri:
            try:
                self.logger.info("尝试创建多模态缓存...")
                cached_content = await self._create_cached_content(
                    video_uri, segments, multimodal=True
                )
                return cached_content, "gemini_video"
            except VertexError as e:
                self.logger.warning(f"多模态缓存创建失败: {e}")
                # 继续降级

        # 尝试 2: 纯文本缓存
        try:
            self.logger.info("创建纯文本缓存...")
            cached_content = await self._create_cached_content(
                None, segments, multimodal=False
            )
            return cached_content, "gemini_text"
        except VertexError as e:
            self.logger.error(f"纯文本缓存也失败: {e}")
            # 继续降级

        # 尝试 3: 无缓存模式
        self.logger.warning("降级到无缓存模式")
        return None, "gemini_nocache"

    async def _create_cached_content(
        self,
        video_uri: Optional[str],
        segments: list[dict],
        multimodal: bool
    ) -> CachedContent:
        """
        创建缓存内容（包含视频、文本和tools）

        Args:
            video_uri: GCS 视频 URI（或 None）
            segments: Segments 列表
            multimodal: 是否多模态

        Returns:
            CachedContent 对象

        Raises:
            VertexError: 创建失败
        """
        # 拼接所有 segments
        full_transcript = "\n\n".join([
            f"Segment #{i} ({s['start']:.1f}s - {s['end']:.1f}s):\n{s['text']}"
            for i, s in enumerate(segments)
        ])

        # 获取工具定义（需要包含在缓存中）
        tools = self.mcp.get_tool_definitions()

        # 创建缓存（包含 tools 和 system_instruction）
        return await self.vertex.create_cached_content(
            video_uri=video_uri if multimodal else None,
            text_content=full_transcript,
            system_instruction=self.SYSTEM_INSTRUCTION,  # [新增] 传入系统指令
            tools=tools,  # 包含工具定义
            ttl_seconds=self.config.gemini_cache_ttl_seconds
        )

    async def _process_segments_concurrent(
        self,
        cached_content: Optional[CachedContent],
        segments: list[dict],
        video_uid: str
    ) -> list[dict]:
        """
        并发处理所有 segments

        Args:
            cached_content: 缓存内容（或 None）
            segments: Segments 列表
            video_uid: 视频 UID

        Returns:
            所有 annotations 的聚合列表
        """
        semaphore = asyncio.Semaphore(self.config.gemini_max_concurrency)

        async def process_one(idx: int, segment: dict):
            """处理单个 segment（先短语后单词）"""
            async with semaphore:
                try:
                    all_anns = []

                    # 1. 处理短语
                    phrase_anns = await self._process_segment(
                        cached_content=cached_content,
                        segment=segment,
                        segment_index=idx,
                        annotator=self.phrase_annotator,
                        video_uid=video_uid
                    )
                    all_anns.extend(phrase_anns)

                    # 2. 处理单词
                    word_anns = await self._process_segment(
                        cached_content=cached_content,
                        segment=segment,
                        segment_index=idx,
                        annotator=self.word_annotator,
                        video_uid=video_uid
                    )
                    all_anns.extend(word_anns)

                    return all_anns

                except Exception as e:
                    self.logger.error(f"Segment {idx} 处理失败: {e}")

                    # 发送错误通知
                    await self.lark.send_error(
                        error_type="Segment 处理失败",
                        error_message=str(e),
                        context={
                            "视频 UID": video_uid,
                            "Segment #": idx,
                            "Segment 文本": segment.get("text", "")[:100]
                        }
                    )

                    return []

        # 创建所有任务
        tasks = [
            process_one(idx, seg)
            for idx, seg in enumerate(segments)
        ]

        # 并发执行
        results = await asyncio.gather(*tasks)

        # 合并结果
        all_annotations = []
        for batch in results:
            all_annotations.extend(batch)

        self.logger.info(
            f"并发处理完成: {len(segments)} segments, "
            f"{len(all_annotations)} annotations"
        )

        return all_annotations

    async def _process_segment(
        self,
        cached_content: Optional[CachedContent],
        segment: dict,
        segment_index: int,
        annotator: BaseAnnotator,
        video_uid: str
    ) -> list[dict]:
        """
        处理单个 segment（使用指定的标注器）

        Args:
            cached_content: 缓存内容（或 None）
            segment: Segment 数据
            segment_index: Segment 索引
            annotator: 标注器实例
            video_uid: 视频 UID

        Returns:
            该 segment 的 annotations 列表
        """
        # [修改] 移除了原有的 system_instruction 定义，现在已经在 Cache 里了

        # 构建具体的任务指令
        task_instruction = f"""
现在请处理 Segment #{segment_index}：
时间: {segment['start']:.1f}s - {segment['end']:.1f}s
文本: {segment['text']}

请使用 **{annotator.get_kind()}** 模式进行分析。
"""

        # 构建完整 prompt（任务指令 + 标注器特定 Prompt）
        prompt = task_instruction + "\n\n" + annotator.build_prompt(segment, segment_index)

        # 获取工具定义
        tools = self.mcp.get_tool_definitions()

        # 创建 trace_id
        trace_id = f"[{video_uid}|Seg#{segment_index}|{annotator.get_kind()}]"

        # 创建 tool_handler（带通知逻辑和详细日志）
        async def tool_handler_with_notification(function_name: str, args: dict) -> dict:
            """
            MCP 工具调用处理器（带通知逻辑）

            职责：
            1. 调用 MCP 查询
            2. 判断是否需要通知
            3. 返回候选列表给 Gemini
            """
            if function_name == "query_fine_units":
                # 记录查询参数
                lemma = args.get("lemma", "N/A")
                kind = args.get("kind", "N/A")
                pos = args.get("pos", "N/A")

                # 查询数据库
                result = await self.mcp.query_fine_units(**args)

                # 记录查询结果
                if result.found:
                    self.logger.info(
                        f"      ✅ 查询 \"{lemma}\" (kind={kind}, pos={pos}) → 找到 {len(result.candidates)} 个候选:"
                    )
                    # 记录每个候选的详情
                    for cand_idx, cand in enumerate(result.candidates):
                        fine_id = cand.get("fine_id", "N/A")
                        definition = cand.get("definition", "N/A")[:120]  # 截取定义
                        self.logger.info(
                            f"         [{cand_idx+1}] fine_id={fine_id} | def: \"{definition}\""
                        )
                else:
                    self.logger.warning(
                        f"      ❌ 查询 \"{lemma}\" (kind={kind}, pos={pos}) → 未找到候选"
                    )
                    # 发送通知
                    await self._handle_not_found(
                        query_params=result.query_params,
                        video_uid=video_uid,
                        segment_index=segment_index,
                        segment=segment
                    )

                # 返回候选列表给 Gemini（同时返回 lemma 用于日志映射）
                return {
                    "candidates": result.candidates,
                    "lemma": lemma  # 用于 vertex.py 建立 fine_id → lemma 映射
                }

            elif function_name == "create_fine_unit":
                # 创建新的 fine_unit
                lemma = args.get("lemma", "N/A")
                kind = args.get("kind", "N/A")
                pos = args.get("pos", "N/A")
                definition = args.get("definition", "N/A")

                self.logger.info(
                    f"      🏗️ Gemini 尝试创建 fine_unit: \"{lemma}\" (kind={kind}, pos={pos})"
                )
                self.logger.info(f"         📝 定义: {definition}")

                # 调用 MCP 创建
                result = await self.mcp.create_fine_unit(
                    lemma=lemma,
                    kind=kind,
                    pos=pos,
                    definition=definition,
                    lang=args.get("lang", "en"),
                    video_uid=video_uid
                )

                self.logger.info(
                    f"      💎 Fine unit 创建结果: fine_id={result['fine_id']}, "
                    f"status={result['status']}, note={result['note']}"
                )

                # 返回创建的候选（格式与 query_fine_units 保持一致）
                return {
                    "candidates": [{
                        "fine_id": result["fine_id"],
                        "label": result["lemma"],
                        "pos": result["pos"],
                        "definition": result["def"]
                    }],
                    "lemma": lemma
                }

            else:
                raise ValueError(f"Unknown function: {function_name}")

        # 调用 Gemini
        try:
            response = await self.vertex.call_with_tools(
                cached_content=cached_content,
                prompt=prompt,
                tools=tools,
                tool_handler=tool_handler_with_notification,
                # [新增] 仅当没有 Cache 时，手动传入 system_instruction
                system_instruction=self.SYSTEM_INSTRUCTION if not cached_content else None,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": annotator.get_output_schema()
                },
                # [新增] 传入追踪上下文（仅用于日志，LLM看不到）
                trace_context={
                    "video_uid": video_uid,
                    "segment_index": segment_index,
                    "segment_text": segment["text"],
                    "annotator_kind": annotator.get_kind()
                }
            )

            annotations = response.get("annotations", [])

            # 验证和过滤
            valid_anns = []
            for ann in annotations:
                if annotator.validate_annotation(ann, segment):
                    valid_anns.append(ann)
                else:
                    self.logger.warning(
                        f"Segment {segment_index} 的 annotation 验证失败: {ann}"
                    )

            self.logger.debug(
                f"Segment {segment_index} ({annotator.get_kind()}): "
                f"{len(valid_anns)}/{len(annotations)} annotations 有效"
            )

            return valid_anns

        except VertexError as e:
            self.logger.error(
                f"Segment {segment_index} ({annotator.get_kind()}) "
                f"Gemini 调用失败: {e}"
            )

            # 发送错误通知
            await self.lark.send_error(
                error_type="Gemini API 调用失败",
                error_message=str(e),
                context={
                    "视频 UID": video_uid,
                    "Segment #": segment_index,
                    "标注器": annotator.get_kind()
                }
            )

            return []

    async def _handle_not_found(
        self,
        query_params: dict,
        video_uid: str,
        segment_index: int,
        segment: dict
    ):
        """
        处理未找到的情况（决定是否通知）

        业务规则：
        - 短语未找到 → 一定通知
        - 单词未找到 → 一定通知
        （只要 LLM 尝试查询了，就说明它认为重要，应该通知）

        Args:
            query_params: 查询参数 {lemma, kind, pos, lang}
            video_uid: 视频 UID
            segment_index: Segment 索引
            segment: Segment 数据
        """
        kind = query_params["kind"]
        lemma = query_params["lemma"]
        lang = query_params["lang"]

        if kind == "phrase_sense":
            # 短语未找到 → 通知
            self.logger.warning(
                f"❌ 短语未匹配: '{lemma}' | "
                f"video={video_uid} | segment={segment_index}"
            )

            await self.lark.send_phrase_not_found(
                phrase=lemma,
                lang=lang,
                video_uid=video_uid,
                segment_index=segment_index,
                segment_text=segment["text"],
                timestamp_range=f"{segment['start']:.1f}s - {segment['end']:.1f}s"
            )

        elif kind == "word_sense":
            # 单词未找到 → 通知（LLM 尝试查询了，说明它认为重要）
            self.logger.warning(
                f"❌ 单词未匹配: '{lemma}' (pos={query_params.get('pos')}) | "
                f"video={video_uid} | segment={segment_index}"
            )

            await self.lark.send_word_not_found(
                word=lemma,
                pos=query_params.get("pos"),
                lang=lang,
                video_uid=video_uid,
                segment_index=segment_index,
                segment_text=segment["text"]
            )

        else:
            # 未知类型
            self.logger.warning(f"未知的 kind: {kind}")