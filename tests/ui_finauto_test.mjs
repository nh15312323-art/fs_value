// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uifinauto_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');

const hits = [];
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/fin/auto-status') return r.end(JSON.stringify({ last: { at: '2026-09-20T11:31:00.000Z', companies: 120, missing: 7, queued: 7, dry: false }, queue: 5, today: '20260921', periods: ['2026 반기'] }));
  if (p === '/api/fin/auto-run') { hits.push(q.url); return r.end(JSON.stringify({ at: 'x', today: '20260921', dry: q.url.includes('dry=1'), periods: ['2026 반기'], companies: 120, missing: 7, queued: q.url.includes('dry=1') ? 0 : 7, byPeriod: { '2026 11012': 7 } })); }
  if (p === '/api/data/summary') return r.end('{}');
  r.end('{}');
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('data')); await pg.waitForTimeout(500);
let t = await pg.innerText('#finAutoBox');
ok('데이터 관리: 대상 기간·대기 건수·마지막 실행 표시', t.includes('2026 반기') && t.includes('수집 대기 5건') && t.includes('빈 기간 7건') && t.includes('큐 등록 7건'), t);
await pg.click('button:has-text("미리보기(등록 안 함)")'); await pg.waitForTimeout(300);
t = await pg.innerText('#finAutoBox');
ok('미리보기: dry=1로 호출, 등록하지 않음 문구', hits[0].includes('dry=1') && t.includes('[미리보기]') && t.includes('등록하지 않음') && t.includes('2026 반기 7건'), t);
await pg.click('button:has-text("지금 확인해서 큐에 등록")'); await pg.waitForTimeout(300);
t = await pg.innerText('#finAutoBox');
ok('실행: dry 없이 호출, 큐에 7건 등록', !hits[1].includes('dry') && t.includes('큐에 7건 등록'), t);
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
