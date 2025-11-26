"""
MCP 工具定义和实现

职责:
- 定义 query_fine_units 的 schema
- 定义 create_fine_unit 的 schema
- 给LLM提供能查询数据库表的工具，使用vertaxAI原生的FunctionDeclaration而非专门的 MCP Server
- 实现数据库查询逻辑（单词、短语、语法）
- 实现创建 fine_unit 的业务逻辑（Gemini生成的条目）
- 返回候选列表
"""
from typing import Optional
import hashlib
import json
from datetime import datetime

from vertexai.generative_models import Tool, FunctionDeclaration

from ingestion_worker.infrastructure.database import Database
from ingestion_worker.utils.logging import get_logger

# POS 映射：完整名称 → 数据库单字符缩写
POS_TO_DB = {
    "n": "n",
    "v": "v",
    "a": "a",
    "r": "r",
    "prep": "p",
    "conj": "c",
    "pron": "m",  # m for pronoun
    "det": "d",
    "interj": "i",
    "N/A": None
}

# 反向映射：数据库缩写 → 完整名称（用于查询结果）
DB_TO_POS = {v: k for k, v in POS_TO_DB.items() if v is not None}

class MCPQueryResult:
    """MCP 查询结果（带元信息）"""
    def __init__(
        self,
        candidates: list[dict],
        found: bool,
        query_params: dict
    ):
        self.candidates = candidates
        self.found = found  # 是否找到候选
        self.query_params = query_params  # 查询参数（用于通知）


class MCPTools:
    """MCP 工具集"""

    def __init__(self, db: Database, gemini_model: str):
        """
        初始化 MCP 工具

        Args:
            db: 数据库客户端
            gemini_model: Gemini 模型名称（用于 external_key）
        """
        self.db = db
        self.gemini_model = gemini_model
        self.logger = get_logger(__name__)

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """
        返回 Function Calling 的工具定义

        Returns:
            Gemini Tool 列表
        """
        query_fine_units = FunctionDeclaration(
            name="query_fine_units",
            description="查询词或短语的知识点候选列表（用于消歧）",
            parameters={
                "type": "object",
                "properties": {
                    "lemma": {
                        "type": "string",
                        "description": "词的原型或短语（如 'run' 或 'run out of'）"
                    },
                    "pos": {
                        "type": "string",
                        "description": "词性：n(名词)/v(动词)/a(形容词)/r(副词)/prep(介词)/conj(连词)/pron(代词)/det(限定词)/interj(感叹词)",
                        "enum": ["n", "v", "a", "r", "prep", "conj", "pron", "det", "interj"]
                    },
                    "kind": {
                        "type": "string",
                        "description": "类型：word_sense(单词义项)/phrase_sense(短语义项)",
                        "enum": ["word_sense", "phrase_sense"]
                    },
                    "lang": {
                        "type": "string",
                        "description": "语言代码",
                        "default": "en"
                    }
                },
                "required": ["lemma", "kind"]
            }
        )

        create_fine_unit = FunctionDeclaration(
            name="create_fine_unit",
            description="""
创建新的知识点（仅当 query_fine_units 返回空且该词/短语足够通用时）

判断标准（general enough）：
✅ 常见短语动词 (give up, run out of)
✅ 固定搭配 (heavy rain, strong wind)
✅ 习语 (piece of cake)
✅ 常见语块 (a lot of, in order to)
✅ 常见单词的常用义项
✅ 基础介词、连词、代词等

❌ 随意组合的词 (happy banana, running table)
❌ 专有名词 (New York, Python)
❌ 高度专业术语

只有确认通用时才调用此工具。
""",
            parameters={
                "type": "object",
                "properties": {
                    "lemma": {
                        "type": "string",
                        "description": "词的原型或短语（如 'run' 或 'give up'）"
                    },
                    "kind": {
                        "type": "string",
                        "description": "类型：word_sense(单词义项)/phrase_sense(短语义项)",
                        "enum": ["word_sense", "phrase_sense"]
                    },
                    "pos": {
                        "type": "string",
                        "description": "词性：n/v/a/r/prep/conj/pron/det/interj，短语可用 N/A",
                        "enum": ["n", "v", "a", "r", "prep", "conj", "pron", "det", "interj", "N/A"]
                    },
                    "definition": {
                        "type": "string",
                        "description": "简洁、清晰的英文定义"
                    },
                    "lang": {
                        "type": "string",
                        "description": "语言代码",
                        "default": "en"
                    }
                },
                "required": ["lemma", "kind", "pos", "definition"]
            }
        )

        return [Tool(function_declarations=[query_fine_units, create_fine_unit])]

    async def query_fine_units(
            self,
            lemma: str,
            kind: str,
            pos: Optional[str] = None,
            lang: str = "en"
    ) -> MCPQueryResult:
        """
        查询 fine_unit 候选

        Args:
            lemma: 词的原型或短语
            kind: 类型（word_sense / phrase_sense）
            pos: 词性（可选，仅用于 word_sense）
            lang: 语言代码

        Returns:
            MCPQueryResult（包含候选列表和元信息）
        """
        query_params = {
            "lemma": lemma,
            "kind": kind,
            "pos": pos,
            "lang": lang
        }

        if kind == "word_sense":
            candidates = await self._query_word_senses(lemma, pos, lang)
        elif kind == "phrase_sense":
            candidates = await self._query_phrase_senses(lemma, lang)
        else:
            self.logger.warning(f"未知的 kind: {kind}")
            candidates = []

        # 返回结果 + 元信息
        return MCPQueryResult(
            candidates=candidates,
            found=len(candidates) > 0,
            query_params=query_params
        )

    async def _query_word_senses(
            self,
            lemma: str,
            pos: Optional[str],
            lang: str
    ) -> list[dict]:
        """查询单词义项"""

        query = """
                SELECT id, label, pos, def
                FROM semantic.fine_unit
                WHERE kind = 'word_sense'
                  AND LOWER(label) = LOWER($1)
                  AND lang = $2
                  AND status = 'active' \
                """
        params = [lemma, lang]

        if pos:
            # 将完整词性名称转换为数据库单字符缩写
            db_pos = POS_TO_DB.get(pos, pos)
            query += " AND pos = $3"
            params.append(db_pos)

        query += " ORDER BY id LIMIT 50"

        try:
            rows = await self.db.fetch_all(query, *params)

            candidates = [
                {
                    "fine_id": row["id"],
                    "label": row["label"],
                    "pos": row["pos"],
                    "definition": row["def"]
                }
                for row in rows
            ]

            self.logger.debug(
                f"查询单词 '{lemma}' (pos={pos}): {len(candidates)} 个候选"
            )

            return candidates

        except Exception as e:
            self.logger.error(f"查询单词义项失败: {e}")
            return []

    async def _query_phrase_senses(
            self,
            phrase: str,
            lang: str
    ) -> list[dict]:
        """查询短语义项"""

        query = """
                SELECT id, label, def
                FROM semantic.fine_unit
                WHERE kind = 'phrase_sense'
                  AND LOWER(label) = LOWER($1)
                  AND lang = $2
                  AND status = 'active'
                """

        try:
            rows = await self.db.fetch_all(query, phrase, lang)

            candidates = [
                {
                    "fine_id": row["id"],
                    "label": row["label"],
                    "definition": row["def"]
                }
                for row in rows
            ]

            self.logger.debug(
                f"查询短语 '{phrase}': {len(candidates)} 个候选"
            )

            return candidates

        except Exception as e:
            self.logger.error(f"查询短语义项失败: {e}")
            return []

    async def create_fine_unit(
            self,
            lemma: str,
            kind: str,
            pos: str,
            definition: str,
            lang: str = "en",
            video_uid: Optional[str] = None
    ) -> dict:
        """
        创建新的 fine_unit（Gemini 生成）

        Args:
            lemma: 词的原型或短语
            kind: 类型（word_sense / phrase_sense）
            pos: 词性
            definition: 定义
            lang: 语言代码
            video_uid: 视频 UID（用于追溯来源）

        Returns:
            创建结果 {
                "fine_id": int,
                "lemma": str,
                "pos": str,
                "def": str,
                "status": "pending",
                "note": str
            }
        """
        # 1. 生成 external_key
        def_hash = hashlib.md5(definition.encode()).hexdigest()[:8]
        external_key = f"{self.gemini_model}:{lemma}:def_{def_hash}"

        # 2. 检查是否已存在
        try:
            existing = await self.db.fetch_one(
                """
                SELECT id, label, pos, def, status
                FROM semantic.fine_unit
                WHERE external_key = $1
                """,
                external_key
            )

            if existing:
                self.logger.info(
                    f"🔄 Fine unit 已存在: {lemma} (fine_id={existing['id']}, status={existing['status']})"
                )
                return {
                    "fine_id": existing["id"],
                    "lemma": existing["label"],
                    "pos": existing["pos"],
                    "def": existing["def"],
                    "status": existing["status"],
                    "note": "Already exists in database"
                }

            # 3. 将完整词性转换为数据库单字符缩写
            db_pos = POS_TO_DB.get(pos, pos)

            # 4. 构建 meta（保存完整词性名称）
            meta = {
                "source": self.gemini_model,
                "lemma_name": lemma,
                "pos": pos,  # meta 中保存完整名称
                "definition": definition,
                "created_by": "gemini_agentic",
                "created_at_timestamp": datetime.utcnow().isoformat(),
                "video_uid": video_uid
            }

            # 5. 写入数据库（pos 使用单字符缩写）
            row = await self.db.fetch_one(
                """
                INSERT INTO semantic.fine_unit (kind, label, lang, pos, def, meta, status, external_key)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id, label, pos, def, status
                """,
                kind,
                lemma,
                lang,
                db_pos,  # 使用数据库缩写
                definition,
                json.dumps(meta),
                "pending",  # 需要人工审核
                external_key
            )

            self.logger.info(
                f"💎 创建新 fine_unit: {lemma} (fine_id={row['id']}, status=pending, external_key={external_key})"
            )

            return {
                "fine_id": row["id"],
                "lemma": row["label"],
                "pos": row["pos"] or "N/A",
                "def": row["def"],
                "status": "pending",
                "note": "Created by Gemini, pending manual review"
            }

        except Exception as e:
            self.logger.error(f"创建 fine_unit 失败: {e}")
            raise