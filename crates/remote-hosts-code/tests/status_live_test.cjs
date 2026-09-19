'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../src/status_live.js'), 'utf8');
function fixture() {
  let refresh;
  const notice = {dataset: {}, textContent: ''};
  const body = {dataset: {revision: 'base'}, querySelectorAll: () => [], replaceChildren: () => {body.replaced = true;}};
  const document = {hidden: false, getElementById: id => id === 'rh-status' ? body : notice, addEventListener: () => {}};
  const calls = [];
  const context = {document, Map, Date, AbortController,
    setInterval: fn => {refresh = fn;}, setTimeout: () => 1, clearTimeout: () => {},
    fetch: async (...args) => {calls.push(args); return context.reply();},
    DOMParser: class {parseFromString() {return {getElementById: () => null};}},
    reply: async () => ({status: 304})};
  vm.runInNewContext(source, context);
  return {context, notice, body, document, calls, refresh: () => refresh()};
}
test('unchanged snapshot does not replace task cards', async () => {
  const f = fixture(); await f.refresh();
  assert.equal(f.body.replaced, undefined);
  assert.equal(f.notice.dataset.stale, 'false');
  assert.equal(f.calls[0][1].headers['If-None-Match'], 'W/"base"');
  assert.equal(f.calls[0][1].credentials, 'same-origin');
});
test('application revision works when a proxy strips ETag', async () => {
  const f = fixture(); f.context.reply = async () => ({status: 204});
  await f.refresh();
  assert.equal(f.calls[0][0], '/status?revision=base');
  assert.equal(f.notice.dataset.stale, 'false');
  assert.equal(f.body.replaced, undefined);
});
test('network failure preserves the last snapshot and marks uncertainty', async () => {
  const f = fixture(); f.context.reply = async () => {throw new Error('network');};
  await f.refresh(); assert.equal(f.notice.dataset.stale, 'true');
  assert.equal(f.body.replaced, undefined); assert.match(f.notice.textContent, /不要重复提交/);
});
test('hidden pages do not poll', async () => {
  const f = fixture(); f.document.hidden = true; await f.refresh(); assert.equal(f.calls.length, 0);
});
test('expired session is never rendered as an empty successful task list', async () => {
  const f = fixture(); f.context.reply = async () => ({status: 200, ok: true, text: async () => '<form>login</form>'});
  await f.refresh(); assert.equal(f.notice.dataset.stale, 'true'); assert.equal(f.body.replaced, undefined);
});
