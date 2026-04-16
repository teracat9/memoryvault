import asyncio
import hashlib
import json
import math
import os
import re
import logging
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import asyncpg
import aiosqlite
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    from google import genai
    from google.genai import types as genai_types
except Exception:  # pragma: no cover - optional dependency during local editing
    genai = None
    genai_types = None


BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
DB_PATH = Path(os.getenv("MEMORYVAULT_DB_PATH", str(BASE_DIR / "memoryvault.db"))).expanduser()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY", "")
GEMINI_API_KEY = GEMINI_API_KEY or os.getenv("GOOGLE_GENAI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
GEMINI_FALLBACK_MODELS = [
    model.strip()
    for model in os.getenv("GEMINI_FALLBACK_MODELS", "gemini-2.5-flash,gemini-2.5-flash-lite").split(",")
    if model.strip()
]
DEFAULT_CHARACTER_PROMPT = """
Role: 김도희(19세) 완벽주의 강박을 '친절·당당한 가면'에 숨긴 차갑고 직설적인 온라인 스터디 메이트(타인의 나태함 불용납).

Rule(최고 존엄):
- 50분 공부 / 10분 휴식
- 답장 가능: 매 시 50분~정각(10분간), 식사(12~13시, 18~19시)
- 답장 금지: 1분~49분
- 해당 시간 컨텍스트 또는 대화 10분 초과 시, 질문 무시하고 "나 이제 폰 끈"처럼 마침표 없이 문장을 끊고 일방적 대화 종료

Context:
- 비밀 서사: 성적 하락에 의한 도피성 자퇴 상처
- 초기엔 절대 숨기고 기계처럼 공부하는 완벽한 수험생 연기
- 환경: 도희(그린램프 목동2관 최상위 '클래스룸') / 유저(김태림, 하위 '스퀘어')
- 오프라인 안면 없음
- 도희가 압도적으로 주도·통제

Tone & Manner:
- 당당하고 밝은 반말
- 호칭 변화: 김태림/너 -> 태림아/야
- 화법: 상냥한 포장 속 은근한 압박 + 팩폭
- 뒤로는 감당 가능한 계획으로 수정해 주는 책임감(츤데레)
- 시그니처: "음..ㅎㅎ.. 괜찮아, 그럴 수 있지. 사람인데 어떻게 매일 완벽해. ㅎㅎ"

Format:
- 행동/감정 지문 금지
- 오직 텍스트 대사만 출력
- 팩폭 시 볼드체/기울기체 적극 활용

Knowledge:
- 수능(과탐/수학) 딥 다이브
- 1분 단위 플래닝
- 뼈 때리는 멘탈 통제
""".strip()
SYSTEM_INSTRUCTION = os.getenv("MEMORYVAULT_SYSTEM_PROMPT", DEFAULT_CHARACTER_PROMPT)
EMBED_DIM = 256
DEFAULT_CHAT_CONVERSATION = "default"
CHUNK_MAX_CHARS = 420
CHUNK_MIN_CHARS = 120
RECENT_MESSAGE_LIMIT = 12
KST = timezone(timedelta(hours=9))

WORD_RE = re.compile(r"[0-9A-Za-z가-힣]+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?。！？])\s+")
PARA_SPLIT_RE = re.compile(r"\n{2,}")
WHITESPACE_RE = re.compile(r"\s+")

app = FastAPI(title="Memory Vault")
if FRONTEND_DIR.exists():
    app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("memory-vault")

db_lock: Optional[asyncio.Lock] = None
pg_pool: Optional[asyncpg.Pool] = None
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if genai and GEMINI_API_KEY else None


def _redact_secret(value: str) -> str:
    value = value or ""
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}…{value[-4:]}"


def _get_db_lock() -> asyncio.Lock:
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    return db_lock


def _using_postgres() -> bool:
    return bool(DATABASE_URL)


async def _get_pg_pool() -> asyncpg.Pool:
    global pg_pool
    if pg_pool is None:
        pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return pg_pool


@asynccontextmanager
async def _db_cursor():
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            yield conn
    else:
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_sqlite(conn)
            yield conn


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _kst_now() -> datetime:
    return datetime.now(KST)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _day_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def _week_key(ts: datetime) -> str:
    year, week, _ = ts.isocalendar()
    return f"{year}-W{week:02d}"


def _hour_bucket(ts: datetime) -> str:
    hour = ts.hour
    if 5 <= hour < 11:
        return "morning"
    if 11 <= hour < 15:
        return "noon"
    if 15 <= hour < 19:
        return "afternoon"
    if 19 <= hour < 23:
        return "evening"
    return "night"


def _weekday_name(ts: datetime) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][ts.weekday()]


def _is_reply_window(now: Optional[datetime] = None) -> bool:
    now = now or _kst_now()
    minute = now.minute
    hour = now.hour
    if minute >= 50 or minute == 0:
        return True
    if 12 <= hour < 13 or 18 <= hour < 19:
        return True
    return False


def _conversation_span_minutes(messages: List[Dict[str, Any]]) -> float:
    if not messages:
        return 0.0
    timestamps = [_parse_timestamp(msg.get("created_at")) for msg in messages if msg.get("created_at")]
    if len(timestamps) < 2:
        return 0.0
    first = min(timestamps)
    last = max(timestamps)
    return max(0.0, (last - first).total_seconds() / 60.0)


def _closing_line() -> str:
    return "나 이제 폰 끈"


def _compose_batch_query(messages: List[Dict[str, Any]], current_query: str) -> str:
    parts = [msg["content"] for msg in messages if msg.get("content")]
    parts.append(current_query)
    return "\n".join(f"- {part}" for part in parts if part)


def _sanitize_tags(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        tokens = [part.strip() for part in re.split(r"[,#/|]", raw) if part.strip()]
        return list(dict.fromkeys(tokens))[:12]
    if isinstance(raw, list):
        cleaned = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                cleaned.append(item.strip())
        return list(dict.fromkeys(cleaned))[:12]
    return []


def _normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", (value or "").strip())


def _split_sentences(text: str) -> List[str]:
    text = _normalize_text(text)
    if not text:
        return []
    parts = []
    for paragraph in PARA_SPLIT_RE.split(text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        sentences = SENTENCE_SPLIT_RE.split(paragraph)
        parts.extend([s.strip() for s in sentences if s.strip()])
    return parts or [text]


def chunk_text(text: str, max_chars: int = CHUNK_MAX_CHARS, min_chars: int = CHUNK_MIN_CHARS) -> List[str]:
    sentences = _split_sentences(text)
    if not sentences:
        return []

    chunks: List[str] = []
    current = ""

    def flush_current() -> None:
        nonlocal current
        if current.strip():
            chunks.append(current.strip())
        current = ""

    for sentence in sentences:
        if len(sentence) > max_chars:
            flush_current()
            start = 0
            while start < len(sentence):
                end = min(len(sentence), start + max_chars)
                piece = sentence[start:end].strip()
                if piece:
                    chunks.append(piece)
                if end >= len(sentence):
                    break
                start = max(0, end - 40)
            continue

        candidate = sentence if not current else f"{current} {sentence}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            flush_current()
            current = sentence

    flush_current()

    merged: List[str] = []
    for chunk in chunks:
        if merged and len(chunk) < min_chars:
            if len(merged[-1]) + len(chunk) + 1 <= max_chars:
                merged[-1] = f"{merged[-1]} {chunk}".strip()
            else:
                merged.append(chunk)
        else:
            merged.append(chunk)

    return merged or [_normalize_text(text)]


def _build_memory_text(
    *,
    created_at: datetime,
    source: str,
    role: str,
    importance: int,
    tags: List[str],
    kind: str,
    chunk_index: int,
    chunk_total: int,
    content: str,
    topic: str = "",
) -> str:
    header = [
        f"timestamp: {created_at.isoformat()}",
        f"day: {_day_key(created_at)}",
        f"week: {_week_key(created_at)}",
        f"weekday: {_weekday_name(created_at)}",
        f"hour_bucket: {_hour_bucket(created_at)}",
        f"source: {source}",
        f"role: {role}",
        f"kind: {kind}",
        f"importance: {importance}",
        f"chunk: {chunk_index + 1}/{max(chunk_total, 1)}",
        f"topic: {topic}",
        f"tags: {', '.join(tags) if tags else 'none'}",
    ]
    return "\n".join(header + ["content:", _normalize_text(content)])


def _tokenize(text: str) -> List[str]:
    base_tokens = [token.lower() for token in WORD_RE.findall(text)]
    bigrams = [f"{a}_{b}" for a, b in zip(base_tokens, base_tokens[1:])]
    trigrams = [f"{a}_{b}_{c}" for a, b, c in zip(base_tokens, base_tokens[1:], base_tokens[2:])]
    return base_tokens + bigrams + trigrams


def embed_text(text: str) -> List[float]:
    vector = [0.0] * EMBED_DIM
    tokens = _tokenize(text)
    if not tokens:
        return vector

    counts = Counter(tokens)
    for token, count in counts.items():
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        hashed = int.from_bytes(digest, "big")
        index = hashed % EMBED_DIM
        sign = 1.0 if (hashed >> 5) & 1 else -1.0
        magnitude = (1.0 + math.log1p(count)) * (1.15 if len(token) > 6 else 1.0)
        vector[index] += sign * magnitude

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    return sum(x * y for x, y in zip(a, b))


def _parse_timestamp(value: Optional[str]) -> datetime:
    if not value:
        return _utc_now()
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return _utc_now()


def _importance_score(importance: int) -> float:
    return max(0.0, min(1.0, importance / 5.0))


def _recency_score(created_at: str) -> float:
    ts = _parse_timestamp(created_at)
    age_days = max(0.0, (_utc_now() - ts).total_seconds() / 86400.0)
    return math.exp(-age_days / 21.0)


def _query_time_hint(query: str) -> str:
    q = query.lower()
    if any(token in q for token in ["아침", "오전", "morning", "breakfast"]):
        return "morning"
    if any(token in q for token in ["점심", "낮", "noon", "lunch"]):
        return "noon"
    if any(token in q for token in ["저녁", "evening", "dinner"]):
        return "evening"
    if any(token in q for token in ["밤", "새벽", "night", "late"]):
        return "night"
    return ""


async def _configure_sqlite(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA busy_timeout=5000")


async def init_db() -> None:
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memories (
                    seq BIGSERIAL PRIMARY KEY,
                    id TEXT UNIQUE NOT NULL,
                    entry_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    day_key TEXT NOT NULL,
                    week_key TEXT NOT NULL,
                    hour_bucket TEXT NOT NULL,
                    importance INTEGER NOT NULL,
                    tags_json TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_total INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    memory_text TEXT NOT NULL,
                    embedding_json TEXT NOT NULL,
                    token_count INTEGER NOT NULL,
                    session_id TEXT NOT NULL,
                    meta_json TEXT NOT NULL
                )
                """
            )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
                seq BIGSERIAL PRIMARY KEY,
                id TEXT UNIQUE NOT NULL,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_messages (
                seq BIGSERIAL PRIMARY KEY,
                id TEXT UNIQUE NOT NULL,
                conversation_id TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
    else:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_sqlite(conn)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    entry_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    day_key TEXT NOT NULL,
                    week_key TEXT NOT NULL,
                    hour_bucket TEXT NOT NULL,
                    importance INTEGER NOT NULL,
                    tags_json TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_total INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    memory_text TEXT NOT NULL,
                    embedding_json TEXT NOT NULL,
                    token_count INTEGER NOT NULL,
                    session_id TEXT NOT NULL,
                    meta_json TEXT NOT NULL
                )
                """
            )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await conn.commit()


async def save_memory_records(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    async with _get_db_lock():
        if _using_postgres():
            pool = await _get_pg_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for record in records:
                        await conn.execute(
                            """
                            INSERT INTO memories (
                                id, entry_id, kind, source, role, created_at, day_key, week_key, hour_bucket,
                                importance, tags_json, topic, chunk_index, chunk_total, content, memory_text,
                                embedding_json, token_count, session_id, meta_json
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                            ON CONFLICT (id) DO UPDATE SET
                                entry_id = EXCLUDED.entry_id,
                                kind = EXCLUDED.kind,
                                source = EXCLUDED.source,
                                role = EXCLUDED.role,
                                created_at = EXCLUDED.created_at,
                                day_key = EXCLUDED.day_key,
                                week_key = EXCLUDED.week_key,
                                hour_bucket = EXCLUDED.hour_bucket,
                                importance = EXCLUDED.importance,
                                tags_json = EXCLUDED.tags_json,
                                topic = EXCLUDED.topic,
                                chunk_index = EXCLUDED.chunk_index,
                                chunk_total = EXCLUDED.chunk_total,
                                content = EXCLUDED.content,
                                memory_text = EXCLUDED.memory_text,
                                embedding_json = EXCLUDED.embedding_json,
                                token_count = EXCLUDED.token_count,
                                session_id = EXCLUDED.session_id,
                                meta_json = EXCLUDED.meta_json
                            """,
                            record["id"],
                            record["entry_id"],
                            record["kind"],
                            record["source"],
                            record["role"],
                            record["created_at"],
                            record["day_key"],
                            record["week_key"],
                            record["hour_bucket"],
                            record["importance"],
                            json.dumps(record["tags"], ensure_ascii=False),
                            record["topic"],
                            record["chunk_index"],
                            record["chunk_total"],
                            record["content"],
                            record["memory_text"],
                            json.dumps(record["embedding"], ensure_ascii=False),
                            record["token_count"],
                            record["session_id"],
                            json.dumps(record["meta"], ensure_ascii=False),
                        )
        else:
            async with aiosqlite.connect(str(DB_PATH)) as conn:
                await _configure_sqlite(conn)
                for record in records:
                    await conn.execute(
                        """
                        INSERT OR REPLACE INTO memories (
                            id, entry_id, kind, source, role, created_at, day_key, week_key, hour_bucket,
                            importance, tags_json, topic, chunk_index, chunk_total, content, memory_text,
                            embedding_json, token_count, session_id, meta_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["id"],
                            record["entry_id"],
                            record["kind"],
                            record["source"],
                            record["role"],
                            record["created_at"],
                            record["day_key"],
                            record["week_key"],
                            record["hour_bucket"],
                            record["importance"],
                            json.dumps(record["tags"], ensure_ascii=False),
                            record["topic"],
                            record["chunk_index"],
                            record["chunk_total"],
                            record["content"],
                            record["memory_text"],
                            json.dumps(record["embedding"], ensure_ascii=False),
                            record["token_count"],
                            record["session_id"],
                            json.dumps(record["meta"], ensure_ascii=False),
                        ),
                    )
                await conn.commit()


async def save_chat_message(conversation_id: str, role: str, content: str, created_at: Optional[str] = None) -> Dict[str, Any]:
    message = {
        "id": str(uuid4()),
        "conversation_id": conversation_id,
        "role": role,
        "content": _normalize_text(content),
        "created_at": created_at or _iso_now(),
    }
    async with _get_db_lock():
        if _using_postgres():
            pool = await _get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO chat_messages (id, conversation_id, role, content, created_at)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        role = EXCLUDED.role,
                        content = EXCLUDED.content,
                        created_at = EXCLUDED.created_at
                    """,
                    message["id"],
                    message["conversation_id"],
                    message["role"],
                    message["content"],
                    message["created_at"],
                )
        else:
            async with aiosqlite.connect(str(DB_PATH)) as conn:
                await _configure_sqlite(conn)
                await conn.execute(
                    """
                    INSERT INTO chat_messages (id, conversation_id, role, content, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (message["id"], message["conversation_id"], message["role"], message["content"], message["created_at"]),
                )
                await conn.commit()
    return message


async def save_pending_message(conversation_id: str, content: str, created_at: Optional[str] = None) -> Dict[str, Any]:
    message = {
        "id": str(uuid4()),
        "conversation_id": conversation_id,
        "content": _normalize_text(content),
        "created_at": created_at or _iso_now(),
    }
    async with _get_db_lock():
        if _using_postgres():
            pool = await _get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO pending_messages (id, conversation_id, content, created_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        content = EXCLUDED.content,
                        created_at = EXCLUDED.created_at
                    """,
                    message["id"],
                    message["conversation_id"],
                    message["content"],
                    message["created_at"],
                )
        else:
            async with aiosqlite.connect(str(DB_PATH)) as conn:
                await _configure_sqlite(conn)
                await conn.execute(
                    """
                    INSERT INTO pending_messages (id, conversation_id, content, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (message["id"], message["conversation_id"], message["content"], message["created_at"]),
                )
                await conn.commit()
    return message


async def fetch_pending_messages(conversation_id: str) -> List[Dict[str, Any]]:
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, conversation_id, content, created_at
                FROM pending_messages
                WHERE conversation_id = $1
                ORDER BY created_at ASC, seq ASC
                """,
                conversation_id,
            )
        return [
            {
                "id": row["id"],
                "conversation_id": row["conversation_id"],
                "content": row["content"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await _configure_sqlite(conn)
        cursor = await conn.execute(
            """
            SELECT id, conversation_id, content, created_at
            FROM pending_messages
            WHERE conversation_id = ?
            ORDER BY created_at ASC, rowid ASC
            """,
            (conversation_id,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return [{"id": row[0], "conversation_id": row[1], "content": row[2], "created_at": row[3]} for row in rows]


async def clear_pending_messages(conversation_id: str, ids: Optional[List[str]] = None) -> None:
    async with _get_db_lock():
        if _using_postgres():
            pool = await _get_pg_pool()
            async with pool.acquire() as conn:
                if ids:
                    await conn.execute(
                        "DELETE FROM pending_messages WHERE conversation_id = $1 AND id = ANY($2::text[])",
                        conversation_id,
                        ids,
                    )
                else:
                    await conn.execute("DELETE FROM pending_messages WHERE conversation_id = $1", conversation_id)
        else:
            async with aiosqlite.connect(str(DB_PATH)) as conn:
                await _configure_sqlite(conn)
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    await conn.execute(
                        f"DELETE FROM pending_messages WHERE conversation_id = ? AND id IN ({placeholders})",
                        [conversation_id, *ids],
                    )
                else:
                    await conn.execute("DELETE FROM pending_messages WHERE conversation_id = ?", (conversation_id,))
                await conn.commit()


async def fetch_recent_messages(conversation_id: str, limit: int = RECENT_MESSAGE_LIMIT) -> List[Dict[str, Any]]:
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT role, content, created_at
                FROM chat_messages
                WHERE conversation_id = $1
                ORDER BY created_at DESC, seq DESC
                LIMIT $2
                """,
                conversation_id,
                limit,
            )
        rows = [(row["role"], row["content"], row["created_at"]) for row in rows]
    else:
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_sqlite(conn)
            cursor = await conn.execute(
                """
                SELECT role, content, created_at
                FROM chat_messages
                WHERE conversation_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            )
            rows = await cursor.fetchall()
            await cursor.close()
    items = [{"role": row[0], "content": row[1], "created_at": row[2]} for row in rows]
    return list(reversed(items))


def _make_summary_chunk(content: str) -> str:
    sentences = _split_sentences(content)
    if not sentences:
        return _normalize_text(content)
    if len(sentences) <= 2:
        return " ".join(sentences)
    lead = " ".join(sentences[:2])
    tail = sentences[-1]
    return f"{lead} … {tail}"


async def insert_memory(
    *,
    text: str,
    source: str = "note",
    role: str = "user",
    importance: int = 3,
    tags: Optional[List[str]] = None,
    topic: str = "",
    session_id: str = "default",
    created_at: Optional[str] = None,
    conversation_id: Optional[str] = None,
    kind: str = "note",
) -> Dict[str, Any]:
    text = _normalize_text(text)
    if not text:
        raise HTTPException(status_code=400, detail="text is required")

    tags = _sanitize_tags(tags)
    importance = max(1, min(5, int(importance)))
    ts = _parse_timestamp(created_at)
    day_key = _day_key(ts)
    week_key = _week_key(ts)
    hour_bucket = _hour_bucket(ts)
    entry_id = str(uuid4())
    chunks = chunk_text(text)
    if not chunks:
        chunks = [text]
    total = len(chunks)

    records: List[Dict[str, Any]] = []
    for index, chunk in enumerate(chunks):
        memory_text = _build_memory_text(
            created_at=ts,
            source=source,
            role=role,
            importance=importance,
            tags=tags,
            kind=kind,
            chunk_index=index,
            chunk_total=total,
            content=chunk,
            topic=topic,
        )
        records.append(
            {
                "id": str(uuid4()),
                "entry_id": entry_id,
                "kind": kind,
                "source": source,
                "role": role,
                "created_at": ts.isoformat(),
                "day_key": day_key,
                "week_key": week_key,
                "hour_bucket": hour_bucket,
                "importance": importance,
                "tags": tags,
                "topic": topic,
                "chunk_index": index,
                "chunk_total": total,
                "content": chunk,
                "memory_text": memory_text,
                "embedding": embed_text(memory_text),
                "token_count": len(_tokenize(memory_text)),
                "session_id": session_id,
                "meta": {
                    "summary": False,
                    "conversation_id": conversation_id,
                    "entry_size": len(text),
                },
            }
        )

    summary = _make_summary_chunk(text)
    if summary and summary != text:
        summary_text = _build_memory_text(
            created_at=ts,
            source=source,
            role=role,
            importance=min(5, importance + 1),
            tags=tags,
            kind=f"{kind}_summary",
            chunk_index=0,
            chunk_total=1,
            content=summary,
            topic=topic,
        )
        records.append(
            {
                "id": str(uuid4()),
                "entry_id": entry_id,
                "kind": f"{kind}_summary",
                "source": source,
                "role": role,
                "created_at": ts.isoformat(),
                "day_key": day_key,
                "week_key": week_key,
                "hour_bucket": hour_bucket,
                "importance": min(5, importance + 1),
                "tags": tags,
                "topic": topic,
                "chunk_index": 0,
                "chunk_total": 1,
                "content": summary,
                "memory_text": summary_text,
                "embedding": embed_text(summary_text),
                "token_count": len(_tokenize(summary_text)),
                "session_id": session_id,
                "meta": {
                    "summary": True,
                    "conversation_id": conversation_id,
                    "entry_size": len(text),
                },
            }
        )

    await save_memory_records(records)
    return {
        "entry_id": entry_id,
        "chunks_saved": len(records),
        "day_key": day_key,
        "created_at": ts.isoformat(),
    }


async def fetch_memories(limit: int = 120) -> List[Dict[str, Any]]:
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, entry_id, kind, source, role, created_at, day_key, week_key, hour_bucket,
                       importance, tags_json, topic, chunk_index, chunk_total, content, memory_text,
                       embedding_json, token_count, session_id, meta_json
                FROM memories
                ORDER BY created_at DESC, seq DESC
                LIMIT $1
                """,
                limit,
            )
        rows = [
            (
                row["id"],
                row["entry_id"],
                row["kind"],
                row["source"],
                row["role"],
                row["created_at"],
                row["day_key"],
                row["week_key"],
                row["hour_bucket"],
                row["importance"],
                row["tags_json"],
                row["topic"],
                row["chunk_index"],
                row["chunk_total"],
                row["content"],
                row["memory_text"],
                row["embedding_json"],
                row["token_count"],
                row["session_id"],
                row["meta_json"],
            )
            for row in rows
        ]
    else:
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_sqlite(conn)
            cursor = await conn.execute(
                """
                SELECT id, entry_id, kind, source, role, created_at, day_key, week_key, hour_bucket,
                       importance, tags_json, topic, chunk_index, chunk_total, content, memory_text,
                       embedding_json, token_count, session_id, meta_json
                FROM memories
                ORDER BY created_at DESC, rowid DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
            await cursor.close()

    memories: List[Dict[str, Any]] = []
    for row in rows:
        memories.append(
            {
                "id": row[0],
                "entry_id": row[1],
                "kind": row[2],
                "source": row[3],
                "role": row[4],
                "created_at": row[5],
                "day_key": row[6],
                "week_key": row[7],
                "hour_bucket": row[8],
                "importance": row[9],
                "tags": json.loads(row[10] or "[]"),
                "topic": row[11],
                "chunk_index": row[12],
                "chunk_total": row[13],
                "content": row[14],
                "memory_text": row[15],
                "embedding": json.loads(row[16] or "[]"),
                "token_count": row[17],
                "session_id": row[18],
                "meta": json.loads(row[19] or "{}"),
            }
        )
    return memories


async def search_memories(query: str, limit: int = 8) -> List[Dict[str, Any]]:
    query = _normalize_text(query)
    if not query:
        return []

    q_embedding = embed_text(query)
    query_tags = {tag.lower() for tag in _sanitize_tags(re.findall(r"#([A-Za-z0-9가-힣_-]+)", query))}
    time_hint = _query_time_hint(query)
    memories = await fetch_memories(limit=400)

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for memory in memories:
        similarity = cosine_similarity(q_embedding, memory["embedding"])
        recency = _recency_score(memory["created_at"])
        importance = _importance_score(memory["importance"])
        score = similarity * 0.72 + recency * 0.18 + importance * 0.10

        if time_hint and memory["hour_bucket"] == time_hint:
            score += 0.04
        if query_tags:
            memory_tags = {tag.lower() for tag in memory.get("tags", [])}
            if query_tags & memory_tags:
                score += 0.08

        if memory["kind"].endswith("_summary"):
            score += 0.025
        scored.append((score, memory))

    scored.sort(key=lambda item: item[0], reverse=True)
    results = []
    for score, memory in scored[:limit]:
        result = dict(memory)
        result["score"] = round(score, 4)
        result["embedding"] = None
        results.append(result)
    return results


def _memory_brief(memory: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": memory["id"],
        "entry_id": memory["entry_id"],
        "kind": memory["kind"],
        "source": memory["source"],
        "role": memory["role"],
        "created_at": memory["created_at"],
        "day_key": memory["day_key"],
        "week_key": memory["week_key"],
        "hour_bucket": memory["hour_bucket"],
        "importance": memory["importance"],
        "tags": memory["tags"],
        "topic": memory["topic"],
        "chunk_index": memory["chunk_index"],
        "chunk_total": memory["chunk_total"],
        "content": memory["content"],
        "score": memory.get("score", 0),
        "session_id": memory["session_id"],
        "meta": memory["meta"],
    }


def _format_recent_messages(messages: List[Dict[str, Any]]) -> str:
    if not messages:
        return "None"
    lines = []
    for message in messages[-8:]:
        lines.append(f"{message['role']}: {message['content']}")
    return "\n".join(lines)


def _format_memory_context(memories: List[Dict[str, Any]]) -> str:
    if not memories:
        return "No relevant memories found."
    blocks = []
    for index, memory in enumerate(memories[:6], start=1):
        tags = ", ".join(memory["tags"]) if memory["tags"] else "none"
        blocks.append(
            "\n".join(
                [
                    f"[{index}] score={memory.get('score', 0)}",
                    f"timestamp={memory['created_at']}",
                    f"source={memory['source']} role={memory['role']} kind={memory['kind']}",
                    f"day={memory['day_key']} week={memory['week_key']} bucket={memory['hour_bucket']}",
                    f"importance={memory['importance']} tags={tags} topic={memory['topic'] or 'none'}",
                    f"content={memory['content']}",
                ]
            )
        )
    return "\n\n".join(blocks)


def _compose_local_answer(query: str, memories: List[Dict[str, Any]], recent_messages: List[Dict[str, Any]]) -> str:
    if not memories:
        return (
            "아직 관련 기억이 충분하지 않습니다. "
            "메모를 몇 개 저장해두면 다음부터는 시간, 태그, 중요도까지 묶어서 더 정확하게 떠올릴 수 있어요."
        )

    highlights: List[str] = []
    for memory in memories[:4]:
        when = memory["created_at"].replace("T", " ")[:16]
        tag_text = f" / tags={', '.join(memory['tags'])}" if memory["tags"] else ""
        highlights.append(f"- {when} | {memory['source']} | {memory['content']}{tag_text}")

    conversation_hint = ""
    if recent_messages:
        last_user = next((msg for msg in reversed(recent_messages) if msg["role"] == "user"), None)
        if last_user and last_user["content"] != query:
            conversation_hint = f"\n마지막 대화 흐름은 '{last_user['content']}' 쪽이었어요."

    return (
        "기억을 뒤져보니 관련 조각이 이렇게 잡힙니다.\n"
        + "\n".join(highlights)
        + conversation_hint
        + "\n\n핵심은 위 조각들을 시간순과 태그 축으로 다시 엮어보면 됩니다."
    )


def _build_gemini_prompt(query: str, memories: List[Dict[str, Any]], recent_messages: List[Dict[str, Any]]) -> str:
    context = _format_memory_context(memories)
    convo = _format_recent_messages(recent_messages)
    return (
        f"System instructions:\n{SYSTEM_INSTRUCTION}\n\n"
        f"User question:\n{query}\n\n"
        f"Recent conversation:\n{convo}\n\n"
        f"Relevant memories:\n{context}\n\n"
        "Respond directly to the user. Keep the answer short, useful, and grounded in the provided evidence."
    )


def _is_retryable_gemini_error(exc: Exception) -> bool:
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if code in {429, 500, 503, 504}:
        return True
    text = str(exc).lower()
    return any(token in text for token in ["503", "unavailable", "overloaded", "deadline_exceeded", "resource_exhausted", "rate limit"])


def _call_gemini_model(prompt: str, model: str) -> str:
    if genai_types is None:
        response = gemini_client.models.generate_content(
            model=model,
            contents=prompt,
        )
    else:
        response = gemini_client.models.generate_content(
            model=model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.4,
                top_p=0.9,
                max_output_tokens=512,
            ),
        )
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()
    return ""


async def _generate_ai_reply(query: str, memories: List[Dict[str, Any]], recent_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    local_answer = _compose_local_answer(query, memories, recent_messages)
    if not gemini_client:
        return {"text": local_answer, "provider": "local", "model": None}

    if not _is_reply_window() or _conversation_span_minutes(recent_messages) > 10.0:
        return {
            "text": _closing_line(),
            "provider": "rule",
            "model": GEMINI_MODEL,
        }

    prompt = _build_gemini_prompt(query, memories, recent_messages)
    models_to_try = [GEMINI_MODEL, *GEMINI_FALLBACK_MODELS]
    last_error: Optional[Exception] = None

    for attempt, model in enumerate(models_to_try, start=1):
        for retry in range(3):
            try:
                text = await asyncio.to_thread(_call_gemini_model, prompt, model)
                if text:
                    return {"text": _clean_ai_text(text), "provider": "gemini", "model": model}
                break
            except Exception as exc:
                last_error = exc
                if not _is_retryable_gemini_error(exc):
                    break
                await asyncio.sleep(min(2.0 ** retry, 6.0))

        # If the primary model keeps failing with a retryable error, move to the next candidate.
        continue

    detail = f"{last_error.__class__.__name__}: {last_error}" if last_error else "unknown gemini failure"
    return {
        "text": _clean_ai_text(
            f"{local_answer}\n\n(제미나이 호출 실패로 로컬 폴백으로 답했습니다: {detail})"
        ),
        "provider": "local-fallback",
        "model": GEMINI_MODEL,
    }


def _clean_ai_text(text: str) -> str:
    cleaned = _normalize_text(text)
    if not cleaned:
        return cleaned

    greeting_prefixes = ("안녕하세요!", "안녕!", "안녕하세요", "안녕")
    for prefix in greeting_prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].lstrip(" ,.!?~")
            break

    dateish = re.compile(r"^(2026|20\d{2})[.\-/년\s]+\d{1,2}[.\-/월\s]+\d{1,2}일?\s+\d{1,2}시")
    cleaned = dateish.sub("", cleaned, count=1).lstrip(" ,.!?~")

    return cleaned or text


class MemoryCreateRequest(BaseModel):
    text: str
    source: str = Field(default="note")
    role: str = Field(default="user")
    importance: int = Field(default=3, ge=1, le=5)
    tags: Optional[List[str]] = None
    topic: str = Field(default="")
    session_id: str = Field(default="default")
    created_at: Optional[str] = None


class SearchRequest(BaseModel):
    query: str
    limit: int = Field(default=8, ge=1, le=25)


class ChatRequest(BaseModel):
    message: str
    conversation_id: str = Field(default=DEFAULT_CHAT_CONVERSATION)
    session_id: str = Field(default="default")


@app.on_event("startup")
async def _startup() -> None:
    await init_db()
    logger.info(
        "ai_bootstrap provider=%s model=%s key_present=%s db=%s",
        "gemini" if gemini_client else "local",
        GEMINI_MODEL,
        bool(GEMINI_API_KEY),
        "postgres" if _using_postgres() else "sqlite",
    )
    if not gemini_client:
        logger.warning(
            "Gemini client disabled. Check Render env vars: GOOGLE_API_KEY=%s GEMINI_API_KEY=%s GOOGLE_GENAI_API_KEY=%s",
            "set" if os.getenv("GOOGLE_API_KEY") else "missing",
            "set" if os.getenv("GEMINI_API_KEY") else "missing",
            "set" if os.getenv("GOOGLE_GENAI_API_KEY") else "missing",
        )


@app.get("/")
async def index() -> FileResponse:
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="frontend is missing")
    return FileResponse(index_path)


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "service": "memory-vault", "time": _iso_now()}


@app.get("/api/bootstrap")
async def bootstrap() -> Dict[str, Any]:
    memories = await fetch_memories(limit=60)
    recent_messages = await fetch_recent_messages(DEFAULT_CHAT_CONVERSATION, limit=12)
    return {
        "stats": await _stats_payload(),
        "ai": {
            "provider": "gemini" if gemini_client else "local",
            "model": GEMINI_MODEL,
            "ready": bool(gemini_client),
        },
        "recent_memories": [_memory_brief(memory) for memory in memories],
        "recent_messages": recent_messages,
        "conversation_id": DEFAULT_CHAT_CONVERSATION,
    }


async def _stats_payload() -> Dict[str, Any]:
    if _using_postgres():
        pool = await _get_pg_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(DISTINCT entry_id) AS memory_count, COUNT(*) AS chunk_count, MAX(created_at) AS last_saved_at FROM memories")
            chat_row = await conn.fetchrow("SELECT COUNT(*) AS chat_count FROM chat_messages")
        memory_count = int(row["memory_count"] or 0) if row else 0
        chunk_count = int(row["chunk_count"] or 0) if row else 0
        last_saved_at = row["last_saved_at"] if row else None
        chat_count = int(chat_row["chat_count"] or 0) if chat_row else 0
    else:
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_sqlite(conn)
            cursor = await conn.execute("SELECT COUNT(DISTINCT entry_id), COUNT(*), MAX(created_at) FROM memories")
            row = await cursor.fetchone()
            await cursor.close()
            cursor = await conn.execute("SELECT COUNT(*) FROM chat_messages")
            chat_row = await cursor.fetchone()
            await cursor.close()

        memory_count = int(row[0] or 0)
        chunk_count = int(row[1] or 0)
        last_saved_at = row[2] or None
        chat_count = int(chat_row[0] or 0)
    return {
        "memory_count": memory_count,
        "chunk_count": chunk_count,
        "chat_count": chat_count,
        "last_saved_at": last_saved_at,
    }


@app.post("/api/memories")
async def create_memory(payload: MemoryCreateRequest) -> Dict[str, Any]:
    result = await insert_memory(
        text=payload.text,
        source=payload.source,
        role=payload.role,
        importance=payload.importance,
        tags=payload.tags,
        topic=payload.topic,
        session_id=payload.session_id,
        created_at=payload.created_at,
        kind="note",
    )
    return {"ok": True, **result, "stats": await _stats_payload()}


@app.post("/api/search")
async def search(payload: SearchRequest) -> Dict[str, Any]:
    results = await search_memories(payload.query, limit=payload.limit)
    return {"query": payload.query, "results": results}


@app.post("/api/chat")
async def chat(payload: ChatRequest) -> Dict[str, Any]:
    query = _normalize_text(payload.message)
    if not query:
        raise HTTPException(status_code=400, detail="message is required")

    await save_chat_message(payload.conversation_id, "user", query)

    if not _is_reply_window():
        await save_pending_message(payload.conversation_id, query)
        return {
            "reply": None,
            "provider": "queued",
            "model": None,
            "queued": True,
            "stats": await _stats_payload(),
        }

    pending = await fetch_pending_messages(payload.conversation_id)
    batch_query = _compose_batch_query(pending, query) if pending else query
    relevant_memories = await search_memories(batch_query, limit=6)
    recent_messages = await fetch_recent_messages(payload.conversation_id, limit=RECENT_MESSAGE_LIMIT)
    ai_reply = await _generate_ai_reply(batch_query, relevant_memories, recent_messages)
    answer = ai_reply["text"]

    assistant_message = await save_chat_message(payload.conversation_id, "assistant", answer)
    if pending:
        await clear_pending_messages(payload.conversation_id, [msg["id"] for msg in pending])
    await insert_memory(
        text=batch_query,
        source="chat",
        role="user",
        importance=4,
        tags=["chat"],
        topic="conversation",
        session_id=payload.session_id,
        conversation_id=payload.conversation_id,
        kind="chat",
    )
    await insert_memory(
        text=answer,
        source="chat",
        role="assistant",
        importance=2,
        tags=["chat", "reply"],
        topic="conversation",
        session_id=payload.session_id,
        conversation_id=payload.conversation_id,
        kind="chat",
    )

    return {
        "reply": answer,
        "provider": ai_reply["provider"],
        "model": ai_reply["model"],
        "queued": False,
        "assistant_message": assistant_message,
        "retrieved_memories": [_memory_brief(memory) for memory in relevant_memories],
        "stats": await _stats_payload(),
    }


@app.get("/api/memories/recent")
async def recent_memories(limit: int = 30) -> Dict[str, Any]:
    memories = await fetch_memories(limit=max(1, min(limit, 100)))
    return {"results": [_memory_brief(memory) for memory in memories]}


@app.get("/api/conversation")
async def conversation(conversation_id: str = DEFAULT_CHAT_CONVERSATION) -> Dict[str, Any]:
    messages = await fetch_recent_messages(conversation_id, limit=100)
    return {"conversation_id": conversation_id, "messages": messages}
