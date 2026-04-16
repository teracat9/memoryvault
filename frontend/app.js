const state = {
  bootstrap: null,
  conversationId: 'default',
};

const $ = (id) => document.getElementById(id);

const els = {
  statMemories: $('statMemories'),
  statChunks: $('statChunks'),
  statChats: $('statChats'),
  statLastSaved: $('statLastSaved'),
  memoryText: $('memoryText'),
  memoryTags: $('memoryTags'),
  memorySource: $('memorySource'),
  memoryTopic: $('memoryTopic'),
  memoryImportance: $('memoryImportance'),
  importanceValue: $('importanceValue'),
  captureStatus: $('captureStatus'),
  saveMemory: $('saveMemory'),
  saveDemo: $('saveDemo'),
  searchQuery: $('searchQuery'),
  runSearch: $('runSearch'),
  searchResults: $('searchResults'),
  chatLog: $('chatLog'),
  chatInput: $('chatInput'),
  sendChat: $('sendChat'),
  ledgerList: $('ledgerList'),
};

function formatTime(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '-';
  return d.toLocaleString('ko-KR', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function parseTags(raw) {
  return String(raw || '')
    .split(/[,#\/|]/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function renderStats(stats) {
  els.statMemories.textContent = String(stats?.memory_count ?? 0);
  els.statChunks.textContent = String(stats?.chunk_count ?? 0);
  els.statChats.textContent = String(stats?.chat_count ?? 0);
  els.statLastSaved.textContent = formatTime(stats?.last_saved_at);
}

function renderLedger(memories) {
  if (!memories?.length) {
    els.ledgerList.innerHTML = '<div class="empty">아직 저장된 기억이 없습니다.</div>';
    return;
  }

  els.ledgerList.innerHTML = memories.map((memory) => {
    const tags = (memory.tags || []).map((tag) => `<span class="chip">#${escapeHtml(tag)}</span>`).join('');
    return `
      <article class="ledger-item">
        <div class="ledger-top">
          <span>${escapeHtml(memory.day_key)} · ${escapeHtml(memory.hour_bucket)} · ${escapeHtml(memory.kind)}</span>
          <span class="badge">I${memory.importance}</span>
        </div>
        <p>${escapeHtml(memory.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(memory.source)}</span>
          <span class="chip">${escapeHtml(memory.role)}</span>
          ${tags}
        </div>
      </article>
    `;
  }).join('');
}

function renderSearchResults(results) {
  if (!results?.length) {
    els.searchResults.innerHTML = '<div class="empty">검색 결과가 없습니다. 다른 표현으로 다시 물어보세요.</div>';
    return;
  }

  els.searchResults.innerHTML = results.map((memory) => {
    const tags = (memory.tags || []).map((tag) => `<span class="chip">#${escapeHtml(tag)}</span>`).join('');
    return `
      <article class="memory-card">
        <div class="memory-top">
          <span>${escapeHtml(memory.created_at)} · ${escapeHtml(memory.source)} · ${escapeHtml(memory.kind)}</span>
          <span class="confidence">score ${memory.score}</span>
        </div>
        <p>${escapeHtml(memory.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(memory.day_key)}</span>
          <span class="chip">${escapeHtml(memory.week_key)}</span>
          <span class="chip">${escapeHtml(memory.hour_bucket)}</span>
          ${tags}
        </div>
      </article>
    `;
  }).join('');
}

function renderChat(messages, extraReply) {
  const parts = [];
  (messages || []).forEach((message) => {
    parts.push(`
      <article class="chat-item ${escapeHtml(message.role)}">
        <div class="role">${escapeHtml(message.role)}</div>
        <p>${escapeHtml(message.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(formatTime(message.created_at))}</span>
        </div>
      </article>
    `);
  });
  if (extraReply) {
    parts.push(`
      <article class="reply-card">
        <div class="role">assistant</div>
        <p>${escapeHtml(extraReply)}</p>
      </article>
    `);
  }
  if (!parts.length) {
    parts.push('<div class="empty">아직 대화가 없습니다.</div>');
  }
  els.chatLog.innerHTML = parts.join('');
}

async function loadBootstrap() {
  const res = await fetch('/api/bootstrap');
  if (!res.ok) {
    throw new Error('bootstrap failed');
  }
  state.bootstrap = await res.json();
  state.conversationId = state.bootstrap.conversation_id || 'default';
  renderStats(state.bootstrap.stats);
  renderLedger(state.bootstrap.recent_memories);
  renderChat(state.bootstrap.recent_messages);
}

async function saveMemory() {
  const text = els.memoryText.value.trim();
  if (!text) {
    els.captureStatus.textContent = '메모 내용을 먼저 입력하세요.';
    return;
  }

  els.captureStatus.textContent = '저장 중...';
  const payload = {
    text,
    tags: parseTags(els.memoryTags.value),
    source: els.memorySource.value,
    topic: els.memoryTopic.value.trim(),
    importance: Number(els.memoryImportance.value || 3),
    session_id: state.conversationId,
  };

  const res = await fetch('/api/memories', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    els.captureStatus.textContent = '저장 실패';
    return;
  }

  const data = await res.json();
  els.captureStatus.textContent = `저장 완료. 청크 ${data.chunks_saved}개가 벡터 메모리에 들어갔습니다.`;
  els.memoryText.value = '';
  await loadBootstrap();
}

async function runSearch() {
  const query = els.searchQuery.value.trim();
  if (!query) {
    renderSearchResults([]);
    return;
  }

  const res = await fetch('/api/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, limit: 8 }),
  });
  const data = await res.json();
  renderSearchResults(data.results || []);
}

async function sendChat() {
  const message = els.chatInput.value.trim();
  if (!message) return;

  els.chatInput.value = '';
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      message,
      conversation_id: state.conversationId,
      session_id: state.conversationId,
    }),
  });

  if (!res.ok) return;
  const data = await res.json();
  renderStats(data.stats);
  renderChat(
    [
      ...(state.bootstrap?.recent_messages || []),
      { role: 'user', content: message, created_at: new Date().toISOString() },
      { role: 'assistant', content: data.reply, created_at: new Date().toISOString() },
    ],
    data.reply,
  );
  renderSearchResults(data.retrieved_memories || []);
  await loadBootstrap();
}

function bindEvents() {
  els.memoryImportance.addEventListener('input', () => {
    els.importanceValue.textContent = String(els.memoryImportance.value);
  });
  els.saveMemory.addEventListener('click', saveMemory);
  els.runSearch.addEventListener('click', runSearch);
  els.sendChat.addEventListener('click', sendChat);
  els.searchQuery.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      runSearch();
    }
  });
  els.chatInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      sendChat();
    }
  });
  els.saveDemo.addEventListener('click', () => {
    els.memoryText.value = [
      '2026-04-16 저녁에 기억 엔진 앱을 만들면서, 시간/태그/중요도를 같이 저장하는 설계를 정했다.',
      '벡터 검색은 청크 단위로 돌리고, 요약 청크도 같이 넣어서 긴 글을 더 잘 떠올리게 했다.',
      '대화는 최근 메시지와 관련 기억을 함께 보고 답하도록 구성했다.',
    ].join(' ');
    els.memoryTags.value = 'memory, vector-db, recall';
    els.memorySource.value = 'journal';
    els.memoryTopic.value = 'product';
    els.memoryImportance.value = '5';
    els.importanceValue.textContent = '5';
  });
}

async function boot() {
  bindEvents();
  await loadBootstrap();
}

boot().catch((error) => {
  console.error(error);
  els.captureStatus.textContent = '초기화에 실패했습니다. 백엔드를 확인하세요.';
});
