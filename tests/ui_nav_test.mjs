// 사용법: node tests/ui_nav_test.mjs (Playwright + Chromium 필요)
// 메뉴 4묶음(발굴·종목·내 투자·검증/관리): 묶음 이동, 보조 메뉴 표시, 마지막으로 본 화면 기억, 하단바 강조, 운영자 화면이 투자 메뉴와 분리됐는지 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uinav_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const srv = http.createServer((q, r) => {
  const p = new URL(q.url, 'http://x').pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  r.statusCode = 404; r.end('{"error":"none"}');
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const SHOT = process.env.SHOT;
// 데스크톱
{
  const pg = await (await b.newContext({ viewport: { width: 1280, height: 800 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
  await pg.goto(base); await pg.waitForTimeout(400);
  const names = await pg.$$eval('#mainNav button', (bs) => bs.map((x) => x.innerText.trim()));
  ok('상단 메뉴는 4개 묶음', names.length === 4 && names.join('|') === '🔎 발굴|📈 종목|💼 내 투자|⚙ 검증·관리', names.join('|'));
  ok('종목 묶음은 화면이 하나라 보조 메뉴 숨김', !(await pg.isVisible('#grpNav')));
  await pg.click('#grpBtn_find'); await pg.waitForTimeout(250);
  const find = await pg.$$eval('#grpNav button', (bs) => bs.filter((x) => x.offsetParent).map((x) => x.innerText.trim()));
  ok('발굴: 보조 메뉴 = 스크리닝 · 거시·상관, 스크리닝 화면', find.join('|') === '스크리닝|거시·상관' && await pg.isVisible('#tabMomentum'), find.join('|'));
  await pg.click('#tabBtnMacro'); await pg.waitForTimeout(200);
  await pg.click('#grpBtn_stock'); await pg.waitForTimeout(200); await pg.click('#grpBtn_find'); await pg.waitForTimeout(200);
  ok('묶음을 다시 누르면 그 묶음에서 마지막에 보던 화면(거시·상관)', await pg.isVisible('#tabMacro') && !(await pg.isVisible('#tabMomentum')));
  await pg.click('#grpBtn_admin'); await pg.waitForTimeout(250);
  const adm = await pg.$$eval('#grpNav button', (bs) => bs.filter((x) => x.offsetParent).map((x) => x.innerText.trim()));
  ok('검증·관리: 백테스트 · 데이터 관리 · DB 용량, 기본은 백테스트', adm.join('|') === '백테스트|데이터 관리|DB 용량' && await pg.isVisible('#tabBt'), adm.join('|'));
  ok('운영자 화면(데이터 관리·DB 용량)은 투자 묶음(발굴·종목·내 투자)에 보조 메뉴로 나오지 않음', await pg.evaluate(() => { const g = document.getElementById('grpNav'); return ['find', 'stock', 'my'].every((k) => { const vis = Array.from(g.querySelectorAll('button')).filter((b) => b.getAttribute('data-grp') === k).map((b) => b.id); return !vis.some((id) => id === 'tabBtnData' || id === 'tabBtnDb'); }); }));
  await pg.click('#tabBtnData'); await pg.waitForTimeout(250);
  ok('데이터 관리 화면 + 묶음 강조는 검증·관리', await pg.isVisible('#tabData') && (await pg.getAttribute('#grpBtn_admin', 'class')).includes('toggle-active') && (await pg.getAttribute('#tabBtnData', 'class')).includes('toggle-active'));
  if (SHOT) { await pg.screenshot({ path: SHOT + '_desktop.png' }); }
  await pg.click('#grpBtn_my'); await pg.waitForTimeout(250);
  ok('내 투자 → 내 계좌 화면', await pg.isVisible('#tabPf'));
  await pg.goto(base + '#db'); await pg.waitForTimeout(300);
  ok('#db 링크로 열면 검증·관리 묶음 + DB 용량 보조 메뉴 강조', await pg.isVisible('#tabDb') && (await pg.getAttribute('#tabBtnDb', 'class')).includes('toggle-active') && (await pg.isVisible('#grpNav')));
}
// 모바일
{
  const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
  const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
  await pg.goto(base); await pg.waitForTimeout(400);
  const bn = await pg.$$eval('#bnav button', (bs) => bs.map((x) => x.innerText.replace(/\s+/g, ' ').trim()));
  ok('하단바: 발굴 · 종목 · 내 투자 · 검증·관리 · 더보기 (5개)', bn.length === 5 && bn[0].includes('발굴') && bn[3].includes('검증·관리') && bn[4].includes('더보기'), bn.join('|'));
  await pg.click('#bn_admin'); await pg.waitForTimeout(250);
  ok('모바일: 검증·관리를 누르면 보조 메뉴(위쪽)에 3개 화면', await pg.isVisible('#grpNav') && (await pg.$$eval('#grpNav button', (bs) => bs.filter((x) => x.offsetParent).length)) === 3);
  await pg.click('#tabBtnDb'); await pg.waitForTimeout(250);
  ok('모바일: DB 용량 화면, 하단바 강조는 검증·관리', await pg.isVisible('#tabDb') && (await pg.getAttribute('#bn_admin', 'class')) === 'on');
  const over = await pg.evaluate(() => document.documentElement.scrollWidth - innerWidth);
  ok('모바일 가로 넘침 없음', over <= 1, String(over));
  await pg.click('#bn_more'); const more = await pg.innerText('#moreSheet .ms-body');
  ok('더보기에는 종목 비교 · 잠그기만 (화면 이동 메뉴 없음)', more.includes('종목 비교') && more.includes('잠그기') && !more.includes('백테스트') && !more.includes('데이터 관리'), more.replace(/\n/g, ' '));
  await pg.mouse.click(195, 100); await pg.waitForTimeout(150);
  if (SHOT) { await pg.click('#bn_find'); await pg.waitForTimeout(300); await pg.screenshot({ path: SHOT + '_mobile.png' }); }
}
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
