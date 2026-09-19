'use strict';
// Read-only progressive enhancement. Losing observation never means work stopped.
(() => {
  let body = document.getElementById('rh-status');
  const notice = document.getElementById('rh-observation');
  if (!body || !notice) return;
  let etag = 'W/"' + body.dataset.revision + '"';
  let running = false;
  async function refresh() {
    if (running || document.hidden) return;
    running = true;
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 6000);
    try {
      const response = await fetch('/status?revision=' + encodeURIComponent(body.dataset.revision), {
        credentials: 'same-origin', cache: 'no-store', signal: abort.signal,
        headers: {'If-None-Match': etag}
      });
      if (response.status === 304 || response.status === 204) {
        notice.textContent = '连接正常，任务状态无变化 · ' + new Date().toLocaleTimeString();
        notice.dataset.stale = 'false';
        return;
      }
      if (!response.ok) throw new Error('status_http_' + response.status);
      const doc = new DOMParser().parseFromString(await response.text(), 'text/html');
      const next = doc.getElementById('rh-status');
      if (!next) throw new Error('session_expired');
      // Reuse unchanged cards, preserving expanded receipts and keyboard focus.
      const previous = new Map([...body.querySelectorAll('[data-operation]')].map(n => [n.dataset.operation, n]));
      for (const card of next.querySelectorAll('[data-operation]')) {
        const old = previous.get(card.dataset.operation);
        if (old && old.dataset.view === card.dataset.view) card.replaceWith(old);
      }
      body.replaceChildren(...next.childNodes);
      body.dataset.revision = next.dataset.revision;
      etag = response.headers.get('ETag') || ('W/"' + next.dataset.revision + '"');
      notice.textContent = '已更新真实状态 · ' + new Date().toLocaleTimeString();
      notice.dataset.stale = 'false';
    } catch (_) {
      notice.textContent = '状态观察暂不可确认，以下为上次快照。请检查连接或重新登录；不要重复提交任务。';
      notice.dataset.stale = 'true';
    } finally {
      clearTimeout(timer);
      running = false;
    }
  }
  setInterval(refresh, 5000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
})();
