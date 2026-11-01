// 사용법: node tests/ui_guide_test.mjs (Playwright + Chromium 필요)
// 백테스트 결과의 「상위 그룹에 들어간 종목 상세」: 기간별 상위 그룹 종목·점수·수익률 표, 기간 선택, 종목명 클릭 시 C 카드 연결, CSV를 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uiguide_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
const NS = 16;
const mk = (code, y, ni, pr) => ({ corp_code: code, bsns_year: String(y), reprt_code: '11011', period_label: y + ' 사업보고서', period_order: y * 10 + 4, fs_div: 'CFS', revenue: 1000e8, cogs: 600e8, operating_income: 150e8, net_income: ni, total_equity: 2000e8, total_liabilities: 800e8, cash: 300e8, ocf: 180e8, capex: 60e8, fcf: 120e8, total_shares: 1e7, treasury_shares: 0, parent_net_income: ni, parent_equity: 2000e8, filing_date: (y + 1) + '0320', price_at_period_end: pr, price_next_day: pr, price_next_day_date: (y + 1) + '0321' });
const all = {};
for (let k = 0; k < NS; k++) { const c = 'C' + String(k).padStart(2, '0'); all[c] = []; for (let y = 2019; y <= 2023; y++) { const pr = 10000 * (1 + 0.05 * k + 0.02 * (y - 2019)); all[c].push(mk(c, y, (50 + k * 6) * 1e8, pr)); } }
const comps = Object.keys(all).map((c, i) => ({ corp_code: c, corp_name: '종목' + String.fromCharCode(65 + i), stock_code: String(100000 + i), n: 5, acc_mt: 12, ind: i % 2 === 0 ? '26110' : '' }));
const filled = [];
const srv = http.createServer((q, r) => {
  const u = new URL(q.url, 'http://x'), p = u.pathname;
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p === '/api/db/companies') return r.end(JSON.stringify({ companies: comps }));
  if (p === '/api/backtest/fin-batch') { const cs = (u.searchParams.get('codes') || '').split(','); return r.end(JSON.stringify({ rows: [].concat.apply([], cs.map((c) => all[c] || [])) })); }
  if (p === '/api/stock/list') return r.end(JSON.stringify({ stocks: [] }));
  if (p === '/api/db/usage') return r.end(JSON.stringify({ sizeBytes: 1e6, limitBytes: 5e8, limitMb: 500, warnPct: 90, pct: 0.2, warn: false }));
  if (p === '/api/industry/lookup') return r.end(JSON.stringify({ map: {} }));
  if (p === '/api/industry/fill') { let body = ''; q.on('data', (d) => body += d); q.on('end', () => { const cs = JSON.parse(body).codes; filled.push(cs); const saved = {}; cs.forEach((c) => { saved[c] = '20119'; }); r.end(JSON.stringify({ saved })); }); return; }
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto(base); await pg.waitForTimeout(700);

// ---- 쉬운 설명
await pg.click('#bn_admin'); await pg.click('#tabBtnGuide'); await pg.waitForTimeout(300);
ok('검증·관리에 쉬운 설명 탭이 열림', await pg.isVisible('#tabGuide'));
const n0 = (await pg.$$('#guideOut details.gd')).length;
ok('설명 항목이 35개 이상', n0 >= 35, String(n0));
const chk = await pg.evaluate(() => {
  const ids = GUIDE.map((g) => g.id), dup = ids.filter((x, i) => ids.indexOf(x) !== i);
  const need = ['one', 'ex', 'app', 'warn', 't', 'k'].filter((f) => GUIDE.some((g) => !g[f]));
  const html = document.documentElement.outerHTML; const used = [];
  html.replace(/guideOpen\((?:'|&quot;)([a-z0-9-]+)(?:'|&quot;)\)/g, (m, id) => { used.push(id); return m; });
  Object.keys(FSC_GUIDE).forEach((k) => used.push(FSC_GUIDE[k]));
  return { dup: dup, need: need, missing: used.filter((id) => ids.indexOf(id) < 0), nUsed: used.length };
});
ok('모든 항목에 한마디·예·앱 위치·조심할 점이 있고 id 중복 없음', chk.dup.length === 0 && chk.need.length === 0, JSON.stringify(chk));
ok('화면의 「쉬운 설명」 링크가 가리키는 항목이 전부 존재', chk.missing.length === 0 && chk.nUsed >= 17, JSON.stringify(chk));
await pg.fill('#guideQ', '현금'); await pg.waitForTimeout(150);
const n1 = (await pg.$$('#guideOut details.gd')).length;
ok('검색 「현금」 → 일부만 남고 FCF 포함', n1 > 0 && n1 < n0 && (await pg.innerText('#guideOut')).includes('FCF'), String(n1));
await pg.fill('#guideQ', '존재하지않는말'); await pg.waitForTimeout(100);
ok('없는 말은 안내 문구', (await pg.innerText('#guideOut')).includes('일치하는 설명이 없습니다'));
await pg.fill('#guideQ', ''); await pg.selectOption('#guideCat', '4'); await pg.waitForTimeout(100);
const cats = await pg.$$eval('#guideOut details summary', (e) => e.map((x) => x.textContent));
ok('분야 「현금흐름」 선택 → CFO·CAPEX·FCF 포함', cats.some((t) => t.includes('CFO')) && cats.some((t) => t.includes('FCF')) && !cats.some((t) => t.includes('ROIC')), cats.join(','));
await pg.selectOption('#guideCat', '');
// 다른 화면에서 링크로 열기
await pg.click('#bn_find'); await pg.click('#tabBtnFscr'); await pg.waitForTimeout(200);
await pg.click('#fscFilters a[title="쉬운 설명"] >> nth=0'); await pg.waitForTimeout(300);
const op = await pg.evaluate(() => ({ tab: document.getElementById('tabGuide').style.display !== 'none', open: Array.prototype.filter.call(document.querySelectorAll('#guideOut details.gd'), (d) => d.open).map((d) => d.id) }));
ok('재무 스크리닝 조건의 ? → 쉬운 설명 탭에서 그 항목만 펼쳐짐', op.tab && op.open.join() === 'gd_roic', JSON.stringify(op));

// ---- 저장 종목 목록(종목분석)
await pg.click('#bn_stock'); await pg.waitForTimeout(300);
await pg.evaluate(() => { document.getElementById('savedBox').open = true; loadDbCompanies(true); }); await pg.waitForTimeout(700);
let opts = await pg.$$eval('#savedInd option', (o) => o.map((x) => x.value + ':' + x.textContent));
ok('종목분석 저장 종목: 업종 선택 목록(전체·26·업종 미확인)', opts.length === 3 && opts[1].startsWith('26:') && opts[2].startsWith('none:'), opts.join(' | '));
await pg.selectOption('#savedInd', '26'); await pg.waitForTimeout(100);
ok('26 선택 → 8개만', (await pg.$$('#savedChips .saved-chip')).length === NS / 2);
await pg.selectOption('#savedInd', ''); await pg.check('#savedGrp'); await pg.waitForTimeout(100);
const hs = await pg.$$eval('#savedChips .ind-h', (e) => e.map((x) => x.textContent));
ok('업종별로 묶기 → 머리글 2개(전자부품… , 업종 미확인) 순서', hs.length === 2 && hs[0].includes('전자부품') && hs[1].includes('업종 미확인'), hs.join(' | '));
ok('칩에 업종 표시', (await pg.innerText('#savedChips .saved-chip')).includes('전자부품'));
await pg.click('#savedBox button:has-text("업종 받기")'); await pg.waitForTimeout(1500);
ok('업종 받기 → 미확인 8종목을 3개씩(3·3·2) 요청', filled.map((a) => a.length).join() === '3,3,2', JSON.stringify(filled.map((a) => a.length)));
opts = await pg.$$eval('#savedInd option', (o) => o.map((x) => x.value));
ok('받은 뒤 업종 목록에 20 추가, 미확인 없음', opts.join() === ',20,26' || opts.join() === ',26,20', opts.join());

// ---- 데이터 관리 > DB 저장 종목 목록
await pg.evaluate(() => { switchTab('data'); return toggleDbList(true); }); await pg.waitForTimeout(700);
opts = await pg.$$eval('#dbListInd option', (o) => o.map((x) => x.value));
ok('데이터 관리 목록에도 업종 선택', opts.length === 3, opts.join());
await pg.selectOption('#dbListInd', '20'); await pg.waitForTimeout(100);
ok('20 선택 → 8개, 개수 표시', (await pg.$$('#dbListBody .db-chip')).length === 8 && (await pg.innerText('#dbListCount')).includes('8개 일치'));
await pg.selectOption('#dbListInd', ''); await pg.check('#dbListGrp'); await pg.waitForTimeout(100);
const firstGroupChip = await pg.evaluate(() => { const c = document.querySelector('#dbListBody .db-chip'); c.click(); return c.textContent; });
await pg.waitForTimeout(400);
ok('묶은 목록에서 칩을 눌러도 올바른 종목이 조회됨', (await pg.inputValue('#corpName')).length > 0 && firstGroupChip.includes((await pg.inputValue('#corpName'))), firstGroupChip + ' / ' + await pg.inputValue('#corpName'));

// ---- 백테스트 상위 종목에 업종
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(700);
await pg.evaluate(() => btLoadStocks()); await pg.waitForTimeout(1500);
await pg.evaluate(() => { document.querySelectorAll('.btFac').forEach((c) => { c.checked = c.value === 'ep'; }); return btRunFin(); }); await pg.waitForTimeout(1500);
const head = await pg.$$eval('#btTopBody_fin thead th', (e) => e.map((x) => x.textContent));
ok('상위 그룹 표에 업종 열', head.indexOf('업종') === head.indexOf('코드') + 1, head.join(','));
const cellsI = await pg.$$eval('#btTopBody_fin tbody tr', (rs) => rs.map((r) => r.children[3].textContent));
ok('업종 칸이 채워짐(미확인 없음, "-" 없음)', cellsI.length > 0 && cellsI.every((t) => t && t !== '-'), cellsI.join('|'));
await pg.click('#btTopBody_fin tbody tr td:nth-child(2) a'); await pg.waitForTimeout(900);
ok('종목별 보기(C)에 업종 표시', (await pg.innerText('#btStkOut')).match(/종목코드 \d+ · .+/) !== null, (await pg.innerText('#btStkOut')).slice(0, 80).replace(/\n/g, ' '));
ok('페이지 오류 없음', errs.length === 0, errs.join(';'));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
