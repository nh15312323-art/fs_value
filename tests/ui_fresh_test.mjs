// 사용법: node tests/ui_fresh_test.mjs (Playwright + Chromium 필요)
// 화면 맨 위 「데이터 기준일」 줄: 평일 기준 늦음 계산, 최신/늦음/조회 실패 표시, 누르면 데이터 관리로 이동.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uifresh_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
let mode = 'ok', calls = 0;
const srv = http.createServer((q, r) => {
  const p = new URL(q.url, 'http://x').pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/data/fresh') { calls++; if (mode === 'err') { r.statusCode = 500; return r.end('{"error":"x"}'); } return r.end(JSON.stringify({ market_last: mode === 'stale' ? '20200102' : '20990101', screen_last: '20261018' })); }
  r.statusCode = 404; r.end('{"error":"none"}');
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const open = async (m) => { mode = m; const pg = await (await b.newContext({ viewport: { width: 1280, height: 800 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e))); await pg.goto(base); await pg.waitForTimeout(600); return pg; };
// 평일 계산 (한국시간 기준, 시세는 평일 18:01 갱신 → 18:30 이후부터 그날 자료가 있어야 함)
const pg = await open('ok');
const K = (y, mo, d, h, mi) => Date.UTC(y, mo - 1, d, h - 9, mi); // 한국시각 → epoch
const lag = (m, t) => pg.evaluate(([m, t]) => freshLag(m, t), [m, t]);
ok('금요일 19:00(한국) · 금요일 자료 → 0일 늦음', (await lag('20261023', K(2026, 10, 23, 19, 0))) === 0);
ok('금요일 17:00 · 목요일 자료 → 0일(아직 오늘 갱신 전)', (await lag('20261022', K(2026, 10, 23, 17, 0))) === 0);
ok('월요일 10:00 · 금요일 자료 → 0일(주말 건너뜀)', (await lag('20261023', K(2026, 10, 26, 10, 0))) === 0);
ok('월요일 19:00 · 금요일 자료 → 1일 늦음', (await lag('20261023', K(2026, 10, 26, 19, 0))) === 1);
ok('수요일 19:00 · 지난 금요일 자료 → 3일 늦음(월·화·수)', (await lag('20261016', K(2026, 10, 21, 19, 0))) === 3);
ok('토요일 · 금요일 자료 → 0일', (await lag('20261023', K(2026, 10, 24, 12, 0))) === 0);
ok('형식이 틀리면 계산하지 않음(null)', (await lag(null, K(2026, 10, 24, 12, 0))) === null && (await lag('abc', 1)) === null);
// 표시
ok('최신 자료: 줄 표시, 경고 아님, 기준일·스크리닝 날짜 포함', await pg.isVisible('#freshBar') && !(await pg.getAttribute('#freshBar', 'class') || '').includes('warn') && (await pg.innerText('#freshBar')).includes('2099-01-01') && (await pg.innerText('#freshBar')).includes('2026-10-18'), await pg.innerText('#freshBar'));
const c0 = calls; await pg.evaluate(() => { switchTab('momentum'); switchTab('financial'); switchTab('macro'); }); await pg.waitForTimeout(300);
ok('탭을 옮겨도 10분 안에는 다시 조회하지 않음(읽기 절약)', calls === c0, c0 + ' → ' + calls);
await pg.close();
const ps = await open('stale');
const t = await ps.innerText('#freshBar');
ok('오래된 시세: 경고 색 + 얼마나 늦었는지 + 휴장 가능성 + 최신 아님 안내', (await ps.getAttribute('#freshBar', 'class')).includes('warn') && t.includes('2020-01-02') && /평일 기준 \d+일 늦음/.test(t) && t.includes('공휴일') && t.includes('최신이 아닙니다'), t);
await ps.click('#freshBar'); await ps.waitForTimeout(300);
ok('줄을 누르면 데이터 관리 화면으로 이동(검증·관리 묶음)', await ps.isVisible('#tabData') && (await ps.getAttribute('#grpBtn_admin', 'class')).includes('toggle-active'));
await ps.close();
const pe = await open('err');
const te = await pe.innerText('#freshBar');
ok('조회 실패: 숨기지 않고 알림(최신이 아닐 수 있음)', (await pe.getAttribute('#freshBar', 'class')).includes('err') && te.includes('불러오지 못했습니다') && te.includes('최신이 아닐 수'), te);
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
