"""
短语标注器（phrase_sense）

职责：
- 构建短语标注的 prompt（重点关注多词组合）
- 定义短语的 comprehensibility 评分规则
- 验证标注结果
"""
from ingestion_worker.domain.agentic.annotators.base import BaseAnnotator
from ingestion_worker.utils.logging import get_logger


class PhraseAnnotator(BaseAnnotator):
    """短语标注器"""

    # 业务配置
    COMMON_PHRASE_TYPES = [
        "phrasal verbs (如 give up, run out of)",
        "collocations (如 heavy rain, strong wind)",
        "idioms (如 piece of cake)"
    ]

    def __init__(self):
        self.logger = get_logger(__name__)

    def get_kind(self) -> str:
        """返回标注类型"""
        return "phrase_sense"

    def build_prompt(self, segment: dict, segment_index: int) -> str:
        """
        构建短语标注的 prompt

        Args:
            segment: Segment 数据
            segment_index: Segment 索引

        Returns:
            Prompt 字符串
        """
        phrase_types = "\n".join([f"  - {t}" for t in self.COMMON_PHRASE_TYPES])

        return f"""
专注处理 Segment #{segment_index}：

时间: {segment['start']:.1f}s - {segment['end']:.1f}s
文本: {segment['text']}

任务：识别该 segment 中的**短语**，并标注含义。

常见短语类型：
{phrase_types}

工作流程：
1. 识别短语（如 "give up", "run out of", "heavy rain"）
2. **调用 query_fine_units 工具**获取候选列表
   - 示例：query_fine_units(lemma="give up", kind="phrase_sense")
   - 工具会返回候选列表，每个候选包含 fine_id 和定义
3. 从工具返回的候选中选择最合适的 fine_id
   - **fine_id 必须是工具返回的候选之一，不能自己编造**
   - 如果工具返回空列表，跳过该短语，不输出 annotation
4. 评估两个 comprehensibility 分数（0.0-1.0）

Comprehensibility 评分标准（关注短语的整体含义）：

**visual_comprehensibility**: 画面展示短语的整体含义
- 1.0: 画面直接展示短语含义（说 "give up" 时看到放弃的动作）
- 0.8: 画面清晰展示相关场景
- 0.6: 画面提供线索（说 "run out of" 时看到空瓶子）
- 0.4: 画面弱相关
- 0.2: 画面很弱相关
- 0.0: 画面无关

**textual_comprehensibility**: 上下文对短语整体含义的提示
- 1.0: 上下文明确解释短语
- 0.8: 上下文提供丰富线索
- 0.6: 可从上下文推断短语含义
- 0.4: 提供基本线索
- 0.2: 上下文弱相关
- 0.0: 上下文不足

输出格式：
{{
  "annotations": [
    {{
      "segment_index": {segment_index},
      "fine_id": 23456,
      "span": {{"start": 5, "end": 12}},
      "rationale": "表示放弃，视频中看到人停止尝试",
      "visual_comprehensibility": 0.85,
      "textual_comprehensibility": 0.7
    }}
  ]
}}

重要规则：
- 只标注 segment #{segment_index}，segment_index 必须是 {segment_index}
- **短语优先**：优先识别短语，而非单独的单词
  - 如 "give up" 应标注为短语，而非单独的 "give"
  - 如 "run out of" 应标注为短语，而非 "run"
- 候选为空 → 记录但跳过（不输出 annotation）
- 评分要客观，从语言学习者角度考虑
- Span 覆盖整个短语（如 "give up" 的 span 应包含两个词）
"""

    def validate_annotation(self, ann: dict, segment: dict) -> bool:
        """
        验证短语标注（逻辑同单词标注器）

        Args:
            ann: Annotation 数据
            segment: Segment 数据

        Returns:
            True 如果有效，否则 False
        """
        # 必需字段检查
        required = [
            "segment_index", "fine_id", "span", "rationale",
            "visual_comprehensibility", "textual_comprehensibility"
        ]
        if not all(k in ann for k in required):
            self.logger.warning(f"Annotation 缺少必需字段: {ann}")
            return False

        # 类型检查
        if not isinstance(ann["segment_index"], int):
            self.logger.warning(f"segment_index 必须是整数: {ann['segment_index']}")
            return False

        if not isinstance(ann["fine_id"], int):
            self.logger.warning(f"fine_id 必须是整数: {ann['fine_id']}")
            return False

        # 评分范围检查
        v_comp = ann["visual_comprehensibility"]
        t_comp = ann["textual_comprehensibility"]

        if not isinstance(v_comp, (int, float)) or not (0.0 <= v_comp <= 1.0):
            self.logger.warning(f"visual_comprehensibility 无效: {v_comp}")
            return False

        if not isinstance(t_comp, (int, float)) or not (0.0 <= t_comp <= 1.0):
            self.logger.warning(f"textual_comprehensibility 无效: {t_comp}")
            return False

        # Span 检查
        span = ann["span"]
        if not isinstance(span, dict) or "start" not in span or "end" not in span:
            self.logger.warning(f"Span 格式无效: {span}")
            return False

        start = span["start"]
        end = span["end"]
        text_len = len(segment["text"])

        if not isinstance(start, int) or not isinstance(end, int):
            self.logger.warning(f"Span start/end 必须是整数: {span}")
            return False

        if not (0 <= start < end <= text_len):
            self.logger.warning(
                f"Span 超出范围: start={start}, end={end}, text_len={text_len}"
            )
            return False

        # Rationale 检查
        if not isinstance(ann["rationale"], str) or len(ann["rationale"]) == 0:
            self.logger.warning(f"Rationale 无效: {ann['rationale']}")
            return False

        return True

    def get_output_schema(self) -> dict:
        """
        返回输出 schema（同单词标注器）

        Returns:
            JSON schema dict
        """
        return {
            "type": "object",
            "properties": {
                "annotations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "segment_index": {
                                "type": "integer",
                                "description": "Segment 索引"
                            },
                            "fine_id": {
                                "type": "integer",
                                "description": "知识点 ID"
                            },
                            "span": {
                                "type": "object",
                                "properties": {
                                    "start": {
                                        "type": "integer",
                                        "description": "起始字符偏移"
                                    },
                                    "end": {
                                        "type": "integer",
                                        "description": "结束字符偏移"
                                    }
                                },
                                "required": ["start", "end"]
                            },
                            "rationale": {
                                "type": "string",
                                "description": "选择该义项的理由"
                            },
                            "visual_comprehensibility": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                                "description": "视频画面的提示强度（0.0-1.0）"
                            },
                            "textual_comprehensibility": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                                "description": "文本上下文的提示强度（0.0-1.0）"
                            }
                        },
                        "required": [
                            "segment_index",
                            "fine_id",
                            "span",
                            "rationale",
                            "visual_comprehensibility",
                            "textual_comprehensibility"
                        ]
                    }
                }
            },
            "required": ["annotations"]
        }