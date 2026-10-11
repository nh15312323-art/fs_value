// 사용법: node tests/ui_search_test.mjs   (Playwright + Chromium 필요. 없으면 건너뛰어도 됨)
// 가짜 API로 화면을 실제 브라우저에서 띄워 종목 검색 자동완성·저장 종목 패널 동작을 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_ui_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href);
fs.unlinkSync(tmp);
import http from 'node:http';
import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/');
const { chromium } = require('playwright');
const stocks = [['삼성전자','005930'],['삼성SDI','006400'],['삼성물산','028260'],['삼성생명','032830'],['현대자동차','005380'],['SK하이닉스','000660'],['NAVER','035420']].map(([n,c])=>({n,c}));
const companies = [{corp_name:'삼성전자',stock_code:'005930',n:40,first_year:'2016',last_year:'2025',acc_mt:'12'},{corp_name:'현대자동차',stock_code:'005380',n:38,first_year:'2016',last_year:'2025',acc_mt:'12'}];
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type','text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type','application/json');
  if (q.url.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks }));
  if (q.url.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies }));
  r.statusCode = 404; r.end(JSON.stringify({ error: '모의 서버: 없음 ' + q.url }));
}).listen(0);
const port = srv.address().port;
const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] }).catch(async () => chromium.launch({ args: ['--no-sandbox'] }));
const pg = await b.newPage({ viewport: { width: 390, height: 800 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e))); pg.on('console', (m) => { if (m.type() === 'error') errs.push(m.text()); });
await pg.goto('http://localhost:' + port + '/');
await pg.waitForTimeout(500);
const ok = (n, v, d) => console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : ''));
await pg.click('#corpName'); await pg.fill('#corpName', '삼성');
await pg.waitForTimeout(200);
const items = await pg.$$eval('#tabFinancial .combo-list .combo-item', (e) => e.map((x) => x.innerText.replace(/\n/g, ' ')));
ok('"삼성" 입력 → 후보 4개, 저장된 삼성전자가 맨 위', items.length === 4 && items[0].includes('삼성전자') && items[0].includes('저장 40기간'), JSON.stringify(items));
ok('미저장 종목은 "미저장" 배지', items.some((t) => t.includes('삼성SDI') && t.includes('미저장')));
let hint = await pg.innerText('#comboHint_corpName');
ok('정확한 이름이 아니면 후보 안내', hint.includes('정확히 일치하는 종목명이 아닙니다'), hint);
await pg.fill('#corpName', 'ㅅㅅㅈㅈ'); await pg.waitForTimeout(100);
const cho = await pg.$$eval('#tabFinancial .combo-list .combo-item', (e) => e.map((x) => x.innerText));
ok('초성 검색 ㅅㅅㅈㅈ → 삼성전자', cho.length >= 1 && cho[0].includes('삼성전자'), JSON.stringify(cho));
await pg.fill('#corpName', '005380'); await pg.waitForTimeout(100);
ok('종목코드 검색 → 현대자동차', (await pg.innerText('#tabFinancial .combo-list')).includes('현대자동차'));
await pg.fill('#corpName', '없는회사'); await pg.waitForTimeout(100);
hint = await pg.innerText('#comboHint_corpName');
ok('없는 이름은 서버 호출 전에 즉시 안내', hint.includes('일치하는 종목이 없습니다'), hint);
await pg.fill('#corpName', '삼성전자'); await pg.waitForTimeout(100);
hint = await pg.innerText('#comboHint_corpName');
ok('정확한 이름 → ✔ + 저장 정보', hint.includes('✔ 삼성전자') && hint.includes('40개 기간 저장됨'), hint);
await pg.fill('#corpName', 'SK하이닉스'); await pg.waitForTimeout(100);
hint = await pg.innerText('#comboHint_corpName');
ok('미저장 종목 → DART 새로 받음 안내', hint.includes('재무 미저장') && hint.includes('DART'), hint);
// 저장 종목 패널
await pg.click('#savedSummary'); await pg.waitForTimeout(100);
const chips = await pg.$$eval('#savedChips .saved-chip', (e) => e.map((x) => x.innerText.replace(/\n/g, '')));
ok('저장 종목 패널에 2개 표시', chips.length === 2 && chips[0].includes('삼성전자'), JSON.stringify(chips));
await pg.fill('#savedFilter', 'ㅎㄷ'); await pg.waitForTimeout(100);
ok('패널 안 초성 필터', (await pg.$$('#savedChips .saved-chip')).length === 1);
// 칩 클릭 → 입력창 값 채움 + 분석 시작(모의 서버라 오류 문구가 나오는 것이 정상)
await pg.click('#savedChips .saved-chip'); await pg.waitForTimeout(400);
ok('칩 클릭 → 입력창에 종목명, 분석 호출', (await pg.inputValue('#corpName')) === '현대자동차' && (await pg.innerText('#status')).length > 0, await pg.innerText('#status'));
// 키보드 선택
await pg.fill('#corpName', '삼성'); await pg.waitForTimeout(100);
await pg.keyboard.press('ArrowDown'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(300);
ok('↓ + Enter로 두 번째 후보 선택', (await pg.inputValue('#corpName')) === '삼성물산', await pg.inputValue('#corpName'));
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_ui.png') });
ok('자바스크립트 오류 없음', errs.filter((e) => !e.includes('404')).length === 0, errs.join(' | '));
await b.close(); srv.close();
