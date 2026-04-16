import asyncio
import hashlib
import json
import math
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import aiosqlite
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
DB_PATH = BASE_DIR / "memoryvault.db"
EMBED_DIM = 256
DEFAULT_CHAT_CONVERSATION = "default"
CHUNK_MAX_CHARS = 420
CHUNK_MIN_CHARS = 120
RECENT_MESSAGE_LIMIT = 12

WORD_RE = re.compile(r"[0-9A-Za-z가-힣]+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?。！？])\s+")
PARA_SPLIT_RE = re.compile(r"\n{2,}")
WHITESPACE_RE = re.compile(r"\s+")

app = FastAPI(title="Memory Vault")
if FRONTEND_DIR.exists():
    app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")

db_lock: Optional[asyncio.Lock] = None


def _get_db_lock() -> asyncio.Lock:
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    return db_lock


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


async def _configure_db(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA busy_timeout=5000")


async def init_db() -> None:
    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await _configure_db(conn)
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
        await conn.commit()


async def save_memory_records(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    async with _get_db_lock():
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_db(conn)
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
        async with aiosqlite.connect(str(DB_PATH)) as conn:
            await _configure_db(conn)
            await conn.execute(
                """
                INSERT INTO chat_messages (id, conversation_id, role, content, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (message["id"], message["conversation_id"], message["role"], message["content"], message["created_at"]),
            )
            await conn.commit()
    return message


async def fetch_recent_messages(conversation_id: str, limit: int = RECENT_MESSAGE_LIMIT) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await _configure_db(conn)
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
    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await _configure_db(conn)
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
        "recent_memories": [_memory_brief(memory) for memory in memories],
        "recent_messages": recent_messages,
        "conversation_id": DEFAULT_CHAT_CONVERSATION,
    }


async def _stats_payload() -> Dict[str, Any]:
    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await _configure_db(conn)
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

    relevant_memories = await search_memories(query, limit=6)
    recent_messages = await fetch_recent_messages(payload.conversation_id, limit=RECENT_MESSAGE_LIMIT)
    answer = _compose_local_answer(query, relevant_memories, recent_messages)

    assistant_message = await save_chat_message(payload.conversation_id, "assistant", answer)
    await insert_memory(
        text=query,
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
