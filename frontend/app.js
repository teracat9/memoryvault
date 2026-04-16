const state = {
  bootstrap: null,
  conversationId: 'default',
  activeView: 'chat',
};

const $ = (id) => document.getElementById(id);

const els = {
  statMemories: $('statMemories'),
  statChunks: $('statChunks'),
  statChats: $('statChats'),
  aiStatus: $('aiStatus'),
  aiModel: $('aiModel'),
  aiDot: $('aiDot'),
  providerChip: $('providerChip'),
  lastSavedChip: $('lastSavedChip'),
  snapshotProvider: $('snapshotProvider'),
  snapshotModel: $('snapshotModel'),
  snapshotSaved: $('snapshotSaved'),
  snapshotReady: $('snapshotReady'),
  memoryMix: $('memoryMix'),
  recentFeed: $('recentFeed'),
  stageTitle: $('stageTitle'),
  stageSubtitle: $('stageSubtitle'),
  memoryText: $('memoryText'),
  memoryTags: $('memoryTags'),
  memorySource: $('memorySource'),
  memoryTopic: $('memoryTopic'),
  memoryImportance: $('memoryImportance'),
  memoryClass: $('memoryClass'),
  importanceValue: $('importanceValue'),
  captureStatus: $('captureStatus'),
  saveMemory: $('saveMemory'),
  saveDemo: $('saveDemo'),
  focusChat: $('focusChat'),
  refreshAll: $('refreshAll'),
  searchQuery: $('searchQuery'),
  runSearch: $('runSearch'),
  searchResults: $('searchResults'),
  chatLog: $('chatLog'),
  chatInput: $('chatInput'),
  sendChat: $('sendChat'),
  ledgerList: $('ledgerList'),
  pendingBadge: $('pendingBadge'),
};

const viewMeta = {
  chat: {
    title: '기억 기반 대화',
    subtitle: 'Gemini가 관련 기억을 찾아서 바로 답합니다.',
  },
  capture: {
    title: '새 기억 저장',
    subtitle: '시간, 태그, 중요도, 토픽을 청크와 함께 저장합니다.',
  },
  search: {
    title: '벡터 검색',
    subtitle: '질문 문장을 그대로 넣어도 의미 기반으로 회상합니다.',
  },
  ledger: {
    title: '최근 저장된 청크',
    subtitle: '메모를 타임라인처럼 훑어보며 다시 꺼냅니다.',
  },
};

const memoryClassLabels = {
  short_term: '단기기억',
  long_term: '장기기억',
  info: '정보기억',
  relationship: '관계기억',
};

function memoryClassLabel(value) {
  return memoryClassLabels[String(value || 'info')] || '정보기억';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

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

function parseTags(raw) {
  return String(raw || '')
    .split(/[,#\/|]/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function updateView(view) {
  state.activeView = view;
  document.querySelectorAll('.dock-tab').forEach((button) => {
    button.classList.toggle('is-active', button.dataset.view === view);
  });
  document.querySelectorAll('[data-view-panel]').forEach((panel) => {
    panel.classList.toggle('is-active', panel.dataset.viewPanel === view);
  });
  const meta = viewMeta[view] || viewMeta.chat;
  els.stageTitle.textContent = meta.title;
  els.stageSubtitle.textContent = meta.subtitle;
}

function renderStats(stats) {
  els.statMemories.textContent = String(stats?.memory_count ?? 0);
  els.statChunks.textContent = String(stats?.chunk_count ?? 0);
  els.statChats.textContent = String(stats?.chat_count ?? 0);
  const lastSaved = formatTime(stats?.last_saved_at);
  els.lastSavedChip.textContent = `last saved ${lastSaved}`;
  els.snapshotSaved.textContent = lastSaved;

  const counts = stats?.memory_class_counts || {};
  if (els.memoryMix) {
    const ordered = ['short_term', 'long_term', 'info', 'relationship'];
    els.memoryMix.innerHTML = ordered.map((key) => {
      const value = Number(counts[key] || 0);
      return `
        <div class="mix-chip">
          <span>${escapeHtml(memoryClassLabel(key))}</span>
          <strong>${value}</strong>
        </div>
      `;
    }).join('');
  }
}

function renderPendingBadge(count) {
  const hasPending = Number(count || 0) > 0;
  if (!els.pendingBadge) return;
  els.pendingBadge.hidden = !hasPending;
  els.pendingBadge.textContent = hasPending ? '1' : '';
}

function mergeConversation(recentMessages = [], pendingMessages = [], extraMessages = []) {
  const messages = [
    ...recentMessages.map((message) => ({ ...message, pending: false })),
    ...pendingMessages.map((message) => ({ ...message, role: 'user', pending: true })),
    ...extraMessages,
  ];
  const seen = new Set();
  const deduped = [];
  for (const message of messages) {
    const signature = `${message.role || 'user'}|${message.content || ''}|${message.created_at || ''}`;
    if (seen.has(signature)) continue;
    seen.add(signature);
    deduped.push(message);
  }
  deduped.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  return deduped;
}

function renderAI(ai) {
  const provider = ai?.provider || 'local';
  const model = ai?.model || '-';
  const ready = Boolean(ai?.ready);
  els.aiStatus.textContent = ready ? 'online' : 'offline';
  els.aiModel.textContent = model;
  els.providerChip.textContent = provider;
  els.snapshotProvider.textContent = provider;
  els.snapshotModel.textContent = model;
  els.snapshotReady.textContent = ready ? 'Yes' : 'No';
  els.aiDot.classList.toggle('is-live', ready);
}

function renderRecentFeed(memories) {
  if (!memories?.length) {
    els.recentFeed.innerHTML = '<div class="empty">최근 기억이 여기에 표시됩니다.</div>';
    return;
  }

  els.recentFeed.innerHTML = memories.slice(0, 6).map((memory) => {
    const tags = (memory.tags || []).slice(0, 3).map((tag) => `<span class="chip">#${escapeHtml(tag)}</span>`).join('');
    return `
      <article class="pulse-item">
        <div class="chat-top">
          <span>${escapeHtml(memory.day_key)} · ${escapeHtml(memory.hour_bucket)}</span>
          <span class="chip">${escapeHtml(memoryClassLabel(memory.memory_class))}</span>
        </div>
        <p>${escapeHtml(memory.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(memory.source)}</span>
          <span class="chip">${escapeHtml(memory.kind)}</span>
          <span class="chip">I${memory.importance}</span>
          ${tags}
        </div>
      </article>
    `;
  }).join('');
}

function renderLedger(memories) {
  if (!memories?.length) {
    els.ledgerList.innerHTML = '<div class="empty">아직 저장된 기억이 없습니다.</div>';
    return;
  }

  els.ledgerList.innerHTML = memories.slice(0, 18).map((memory) => {
    const tags = (memory.tags || []).map((tag) => `<span class="chip">#${escapeHtml(tag)}</span>`).join('');
    return `
      <article class="ledger-item">
        <div class="ledger-top">
          <span>${escapeHtml(memory.created_at)} · ${escapeHtml(memory.kind)}</span>
          <span class="chip">score ${Number(memory.score || 0).toFixed(3)}</span>
        </div>
        <p>${escapeHtml(memory.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(memoryClassLabel(memory.memory_class))}</span>
          <span class="chip">${escapeHtml(memory.day_key)}</span>
          <span class="chip">${escapeHtml(memory.week_key)}</span>
          <span class="chip">${escapeHtml(memory.hour_bucket)}</span>
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
          <span class="confidence">score ${Number(memory.score || 0).toFixed(3)}</span>
        </div>
        <p>${escapeHtml(memory.content)}</p>
        <div class="meta">
          <span class="chip">${escapeHtml(memoryClassLabel(memory.memory_class))}</span>
          <span class="chip">${escapeHtml(memory.day_key)}</span>
          <span class="chip">${escapeHtml(memory.week_key)}</span>
          <span class="chip">${escapeHtml(memory.hour_bucket)}</span>
          ${tags}
        </div>
      </article>
    `;
  }).join('');
}

function renderChat(messages) {
  if (!messages?.length) {
    els.chatLog.innerHTML = '<div class="empty">아직 대화가 없습니다.</div>';
    return;
  }

  els.chatLog.innerHTML = messages.map((message) => {
    const mine = message.role === 'user';
    const label = mine ? '김태림' : '도희';
    return `
      <article class="bubble-row ${mine ? 'mine' : 'other'}">
        <div class="bubble-avatar">${mine ? 'T' : 'D'}</div>
        <div class="bubble-stack">
          <div class="bubble-name">${label}</div>
          <div class="bubble ${mine ? 'mine' : 'other'} ${message.pending ? 'pending' : ''}">${escapeHtml(message.content)}</div>
          <div class="bubble-time">${escapeHtml(formatTime(message.created_at))}</div>
        </div>
      </article>
    `;
  }).join('');

  requestAnimationFrame(() => {
    els.chatLog.scrollTop = els.chatLog.scrollHeight;
  });
}

async function loadBootstrap() {
  const res = await fetch('/api/bootstrap');
  if (!res.ok) {
    throw new Error('bootstrap failed');
  }
  state.bootstrap = await res.json();
  state.conversationId = state.bootstrap.conversation_id || 'default';
  renderStats(state.bootstrap.stats);
  renderAI(state.bootstrap.ai);
  renderPendingBadge(state.bootstrap.pending_count);
  renderRecentFeed(state.bootstrap.recent_memories);
  renderLedger(state.bootstrap.recent_memories);
  renderChat(mergeConversation(state.bootstrap.recent_messages, state.bootstrap.pending_messages || []));
}

function applySample() {
  els.memoryText.value = [
    '2026-04-16 저녁에 기억 엔진 앱을 만들면서, 시간/태그/중요도를 같이 저장하는 설계를 정했다.',
    '벡터 검색은 청크 단위로 돌리고, 요약 청크도 같이 넣어서 긴 글을 더 잘 떠올리게 했다.',
    '대화는 최근 메시지와 관련 기억을 함께 보고 답하도록 구성했다.',
  ].join(' ');
  els.memoryTags.value = 'memory, vector-db, design';
  els.memorySource.value = 'journal';
  els.memoryTopic.value = 'product';
  els.memoryImportance.value = '5';
  els.importanceValue.textContent = '5';
  if (els.memoryClass) {
    els.memoryClass.value = 'info';
  }
  updateView('capture');
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
    memory_class: els.memoryClass?.value || '',
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
  updateView('ledger');
}

async function runSearch(queryOverride = null) {
  const query = (queryOverride ?? els.searchQuery.value).trim();
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
  updateView('search');
}

async function sendChat() {
  const message = els.chatInput.value.trim();
  if (!message) return;

  const optimistic = {
    role: 'user',
    content: message,
    created_at: new Date().toISOString(),
    pending: true,
  };
  renderChat(mergeConversation(
    state.bootstrap?.recent_messages || [],
    state.bootstrap?.pending_messages || [],
    [optimistic],
  ));
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
  renderPendingBadge(data.pending_count);
  if (data.queued) {
    els.captureStatus.textContent = '지금은 답장 시간이 아니라 메시지만 저장했어. 다음 답장 시간에 한꺼번에 볼게.';
    await loadBootstrap();
    updateView('chat');
    return;
  }

  els.providerChip.textContent = data.provider || els.providerChip.textContent;
  els.snapshotProvider.textContent = data.provider || els.snapshotProvider.textContent;
  els.snapshotModel.textContent = data.model || els.snapshotModel.textContent;
  els.aiStatus.textContent = data.provider === 'gemini' ? 'online' : 'offline';
  els.aiDot.classList.toggle('is-live', data.provider === 'gemini');
  await loadBootstrap();
  updateView('chat');
}

function bindEvents() {
  document.querySelectorAll('.dock-tab').forEach((button) => {
    button.addEventListener('click', () => updateView(button.dataset.view));
  });

  els.memoryImportance.addEventListener('input', () => {
    els.importanceValue.textContent = String(els.memoryImportance.value);
  });

  els.saveMemory.addEventListener('click', saveMemory);
  els.saveDemo.addEventListener('click', applySample);
  els.focusChat.addEventListener('click', () => updateView('chat'));
  els.refreshAll.addEventListener('click', loadBootstrap);
  els.runSearch.addEventListener('click', () => runSearch());
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

  document.querySelectorAll('.suggestion').forEach((button) => {
    button.addEventListener('click', () => {
      els.searchQuery.value = button.dataset.search || '';
      runSearch(button.dataset.search || '');
    });
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === '1') updateView('chat');
    if (event.key === '2') updateView('capture');
    if (event.key === '3') updateView('search');
    if (event.key === '4') updateView('ledger');
    if (event.key === '/' && document.activeElement !== els.searchQuery) {
      event.preventDefault();
      updateView('search');
      els.searchQuery.focus();
    }
  });
}

async function boot() {
  bindEvents();
  await loadBootstrap();
  updateView(state.activeView);
}

boot().catch((error) => {
  console.error(error);
  els.captureStatus.textContent = '초기화에 실패했습니다. 백엔드를 확인하세요.';
});
