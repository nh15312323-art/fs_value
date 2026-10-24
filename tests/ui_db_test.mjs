// 사용법: node tests/ui_db_test.mjs (Playwright + Chromium 필요)
// DB 용량 탭(경고 배너·현황·삭제 미리보기/실행)과 주가 차트 거래대금/거래량 막대를 아이폰(390)·태블릿(820) 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uidb_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');
// 참고: fullPage 스크린샷은 화면이 뷰포트보다 길면 터치(pointer: coarse) 에뮬레이션을 풀어 버려 이후 측정이 달라지므로 쓰지 않는다.
const SHOT = process.env.SHOT_DIR || os.tmpdir();
const dates = []; { let d = new Date(Date.UTC(2024, 9, 2)); while (dates.length < 400) { if (d.getUTCDay() % 6) dates.push(d.toISOString().slice(0, 10).replace(/-/g, '')); d = new Date(d.getTime() + 86400000); } }
const close = dates.map((_, i) => Math.round(50000 + i * 40 + 3000 * Math.sin(i / 20)));
const vol = dates.map((_, i) => 100000 + (i % 17) * 20000), tv = vol.map((v, i) => v * close[i]);
const rows = []; for (let y = 2018; y <= 2024; y++) for (const [rc, lab, po] of [['11013','1분기',1],['11012','반기',2],['11014','3분기',3],['11011','사업보고서',4]]) {
  const g = 1 + (y - 2018) * 0.1;
  rows.push({ corp_code: 'X', bsns_year: String(y), reprt_code: rc, period_label: y + ' ' + lab, period_order: y * 10 + po, fs_div: 'CFS', revenue: 1000e8 * g, cogs: 600e8 * g, operating_income: 150e8 * g, net_income: 110e8 * g, total_equity: 2000e8 * g, total_liabilities: 800e8, cash: 300e8, ocf: 180e8 * g * po, capex: 60e8 * po, fcf: 120e8 * g * po, total_shares: 1e7, parent_net_income: 110e8 * g, parent_equity: 2000e8 * g, filing_date: y + '0' + (po * 2) + '15', per_at_filing: 10, pbr_at_filing: 1.2, fcf_yield_at_filing: 0.06, dividend_per_share: 500, price_at_period_end: 50000 });
}
let withTv = true, usagePct = 92, purged = 0; const calls = [];
const usage = () => ({ sizeBytes: usagePct / 100 * 500 * 1048576, limitBytes: 500 * 1048576, limitMb: 500, warnPct: 90, pct: usagePct, warn: usagePct >= 90, tables: [{ name: 'market_raw_daily', rows: 800000, bytes: 300000000 }, { name: 'financial_raw', rows: 120000, bytes: 90000000 }, { name: 'screen_lists', rows: 600, bytes: 20000000 }], measured: false, tablesAt: Date.now(), protected: true, purgeCap: { perDay: 20000, left: 20000, chunk: 2000 }, priceKeepDays: 380 });
const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  if (p.startsWith('/api/stock/list')) return r.end(JSON.stringify({ stocks: [{ n: '모의전자', c: '000001' }] }));
  if (p.startsWith('/api/db/companies')) return r.end(JSON.stringify({ companies: [{ corp_name: '모의전자', stock_code: '000001', n: 8, acc_mt: 12 }] }));
  if (p === '/api/db/usage') return r.end(JSON.stringify(usage()));
  if (p === '/api/db/settings') { let b = ''; q.on('data', (c) => b += c); q.on('end', () => { calls.push(['settings', b]); r.end(JSON.stringify(usage())); }); return; }
  if (p === '/api/db/purge') { let b = ''; q.on('data', (c) => b += c); q.on('end', () => { const j = JSON.parse(b); calls.push(['purge', b]);
    if (j.preview) return r.end(JSON.stringify({ label: '20240101 이전 일별 시세', count: 4500, estWrites: 9000, daysNeeded: 1, capLeft: 20000, perDay: 20000 }));
    purged += 2000; const done = purged >= 4500; if (done) usagePct = 70;
    r.end(JSON.stringify({ deleted: done ? 500 : 2000, done, capLeft: 20000 - purged, sizeAfter: usagePct / 100 * 500 * 1048576, label: '20240101 이전 일별 시세' })); }); return; }
  if (p.startsWith('/api/price/series')) return r.end(JSON.stringify(Object.assign({ corp_name: '모의전자', stock_code: '000001', n: dates.length, from: dates[0], to: dates[dates.length - 1], d: dates, c: close, v: vol }, withTv ? { tv } : {})));
  if (p.startsWith('/api/latest-price')) return r.end(JSON.stringify({ corp_name: '모의전자', close: close[close.length - 1], date: dates[dates.length - 1] }));
  if (p.startsWith('/api/financial-history-db')) return r.end(JSON.stringify({ corp_name: '모의전자', corp_code: 'X', acc_mt: 12, rows }));
  r.statusCode = 404; r.end(JSON.stringify({ error: 'none' }));
}).listen(0);
const base = 'http://localhost:' + srv.address().port + '/';
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
for (const dev of [{ name: '아이폰', w: 390, h: 844, mobile: true }, { name: '태블릿', w: 820, h: 1180, mobile: true }]) {
  usagePct = 92; purged = 0; calls.length = 0;
  const ctx = await b.newContext({ viewport: { width: dev.w, height: dev.h }, isMobile: dev.mobile, hasTouch: true, deviceScaleFactor: 2 });
  const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e))); pg.on('dialog', (d) => d.accept());
  await pg.goto(base); await pg.waitForTimeout(700);
  const T = dev.name + ': ';
  ok(T + '용량 90% 넘으면 접속 즉시 경고 배너', await pg.isVisible('#dbBanner') && (await pg.innerText('#dbBannerTxt')).includes('92.0%'));
  ok(T + '배너 버튼 높이 ≥ 40px', await pg.$$eval('#dbBanner button', (bs) => bs.every((x) => x.getBoundingClientRect().height >= 39.5)));
  // 종목분석: 저장 종목 목록이 처음부터 보임
  await pg.evaluate(() => switchTab('financial')); await pg.waitForTimeout(200);
  await pg.click('#savedSummary'); await pg.waitForTimeout(200);
  ok(T + '종목분석 첫 진입 시 저장 종목 칩 표시', (await pg.$$('#savedChips .saved-chip')).length === 1);
  // DB 탭 열기
  await pg.click('#dbBanner button'); await pg.waitForTimeout(500);
  ok(T + '배너 → DB 용량 탭 이동', await pg.isVisible('#tabDb') && await pg.isVisible('#dbGauge'));
  const gt = await pg.innerText('#dbGauge');
  ok(T + '현황 표시: 92% · 460MB / 500MB', gt.includes('92') && /460/.test(gt) && /500/.test(gt), gt.replace(/\n/g, ' ').slice(0, 80));
  ok(T + '테이블별 사용량 표', (await pg.innerText('#dbTables')).includes('market_raw_daily'));
  await pg.screenshot({ path: path.join(SHOT, 'db_' + dev.w + '_status.png') });
  // 설정 저장
  await pg.click('#tabDb details.adv > summary'); await pg.waitForTimeout(100);
  await pg.fill('#dbLimitMb', '1000'); await pg.fill('#dbWarnPct', '80');
  await pg.click('#dbTab button[onclick*="dbSaveSettings"], #tabDb button[onclick*="dbSaveSettings"]'); await pg.waitForTimeout(300);
  ok(T + '설정 저장 요청 전송', calls.some((c) => c[0] === 'settings' && JSON.parse(c[1]).limit_mb === 1000 && JSON.parse(c[1]).warn_pct === 80));
  // 삭제: 미리보기 → 실행
  await pg.selectOption('#dbKind', 'price_old'); await pg.waitForTimeout(150);
  ok(T + '삭제 실행 버튼은 미리보기 전엔 숨김', !(await pg.isVisible('#dbRunBtn')));
  await pg.click('#tabDb button[onclick*="dbPreview"]'); await pg.waitForTimeout(300);
  const pv = await pg.innerText('#dbPreviewBox');
  ok(T + '미리보기: 4,500행 표시, 실행 버튼 노출', pv.includes('4,500') && await pg.isVisible('#dbRunBtn'), pv.slice(0, 100));
  await pg.click('#dbRunBtn'); await pg.waitForTimeout(1200);
  const rb = await pg.innerText('#dbRunBox');
  ok(T + '삭제 완료 결과·용량 감소 안내', calls.filter((c) => c[0] === 'purge' && !JSON.parse(c[1]).preview).length === 3 && /4,500|완료/.test(rb), rb.replace(/\n/g, ' ').slice(0, 120));
  ok(T + '삭제 후 배너 사라짐(70%)', !(await pg.isVisible('#dbBanner')));
  await pg.screenshot({ path: path.join(SHOT, 'db_' + dev.w + '_purge.png') });
  const m = await pg.evaluate(() => {
    const vis = (e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; }, root = document.getElementById('tabDb'), small = [], btn = [];
    root.querySelectorAll('input:not([type=checkbox]), select').forEach((e) => { if (vis(e) && parseFloat(getComputedStyle(e).fontSize) < 16) small.push(e.id); });
    root.querySelectorAll('button').forEach((e) => { if (vis(e) && e.getBoundingClientRect().height < 39.5) btn.push(e.textContent.trim().slice(0, 12)); });
    return { small, btn, over: document.documentElement.scrollWidth - innerWidth };
  });
  ok(T + 'DB 탭 입력 16px 이상·버튼 40px 이상·가로 넘침 없음', !m.small.length && !m.btn.length && m.over <= 1, JSON.stringify(m));
  // 거래대금/거래량 차트
  await pg.evaluate(() => switchTab('financial')); await pg.waitForTimeout(150);
  await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(1500);
  const paint = () => pg.evaluate(() => { const c = document.getElementById('priceCanvas'); const x = c.getContext('2d').getImageData(0, Math.round(c.height * 0.62), c.width, Math.round(c.height * 0.15)).data; let n = 0; for (let i = 3; i < x.length; i += 4) if (x[i] > 0) n++; return n; });
  ok(T + '주가 차트 아래 거래대금 막대가 그려짐(기본 거래대금)', await pg.evaluate(() => PRICE.vm) !== 'off' && (await paint()) > 500);
  ok(T + '거래대금/거래량/끄기 버튼 높이 ≥ 40px', await pg.$$eval('#volBtn_tv, #volBtn_vol, #volBtn_off', (bs) => bs.length === 3 && bs.every((x) => x.getBoundingClientRect().height >= 39.5)));
  await pg.screenshot({ path: path.join(SHOT, 'px_' + dev.w + '_tv.png') });
  await pg.click('#volBtn_vol'); await pg.waitForTimeout(150);
  ok(T + '거래량으로 전환', await pg.evaluate(() => PRICE.vm) === 'vol' && (await paint()) > 500);
  const shot = () => pg.evaluate(() => document.getElementById('priceCanvas').toDataURL().length + ':' + document.getElementById('priceCanvas').toDataURL().slice(-200));
  const onShot = await shot();
  await pg.click('#volBtn_off'); await pg.waitForTimeout(150);
  ok(T + '끄면 막대가 사라지고 차트 배치가 바뀜', await pg.evaluate(() => PRICE.vm) === 'off' && (await shot()) !== onShot);
  ok(T + '선택이 저장됨(pxVm)', await pg.evaluate(() => localStorage.getItem('pxVm')) === 'off');
  await pg.click('#volBtn_tv'); await pg.waitForTimeout(150);
  const box = await pg.locator('#priceCanvas').boundingBox();
  await pg.mouse.move(box.x + box.width * 0.5, box.y + box.height * 0.3); await pg.waitForTimeout(150);
  await pg.screenshot({ path: path.join(SHOT, 'px_' + dev.w + '_tip.png') });
  ok(T + '주가 화면 가로 넘침 없음', await pg.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  await ctx.close();
}
// 거래대금 데이터 없을 때(구버전 응답) 거래량으로 대체
withTv = false;
{ const ctx = await b.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true }); const pg = await ctx.newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
  await pg.goto(base); await pg.waitForTimeout(500); await pg.evaluate(() => { try { localStorage.setItem('pxVm', 'tv'); } catch (e) {} switchTab('financial'); });
  await pg.fill('#corpName', '모의전자'); await pg.keyboard.press('Enter'); await pg.waitForTimeout(1500);
  const c = await pg.evaluate(() => { const c = document.getElementById('priceCanvas'); const x = c.getContext('2d').getImageData(0, Math.round(c.height * 0.62), c.width, Math.round(c.height * 0.15)).data; let n = 0; for (let i = 3; i < x.length; i += 4) if (x[i] > 0) n++; return n; });
  ok('거래대금 자료 없으면 거래량으로 대체해 그림', c > 500, String(c)); await ctx.close(); }
ok('페이지 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close();
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
