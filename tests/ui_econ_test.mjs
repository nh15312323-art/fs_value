// 사용법: node tests/ui_econ_test.mjs   (Playwright + Chromium 필요)
// "경제 상황" 화면: 가짜 /api/macro/series로 신호표(최신값·변화·위치·신호)·파생지표(금리차, 순유동성, M2·물가 증가율)·그래프를 실제 브라우저에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_econ_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
import { createRequire } from 'node:module';
const require = createRequire('/opt/npm-tools/node_modules/');
const { chromium } = require('playwright');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href);
fs.unlinkSync(tmp);
const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, '');
const END = Date.UTC(2026, 10, 18); // 2026-11-18(수)
const daily = (fn, start = Date.UTC(2017, 0, 2)) => { const o = []; for (let t = start, i = 0; t <= END; t += 86400000) { const d = new Date(t); if (d.getUTCDay() % 6) o.push({ date: ymd(d), value: fn(i++) }); } return o; };
const weekly = (fn) => { const o = []; for (let t = Date.UTC(2017, 0, 4), i = 0; t <= END; t += 7 * 86400000) o.push({ date: ymd(new Date(t)), value: fn(i++) }); return o; };
const monthly = (fn) => { const o = []; for (let y = 2017, m = 0, i = 0; y < 2026 || m <= 9; i++) { o.push({ date: y + String(m + 1).padStart(2, '0') + '01', value: fn(i) }); if (++m > 11) { m = 0; y++; } } return o; };
const S = {
  us2y: daily(() => 4.5), us10y: daily(() => 4.0), ktb3y: daily(() => 3.0), ktb10y: daily(() => 3.4),
  usrrp: daily(() => 100), ustga: weekly(() => 800000), usbs: weekly(() => 7000000),
  usm2: monthly((i) => 20000 + i * 50), krm2: [], uscpi: monthly((i) => 100 * Math.pow(1.04, i / 12)), krcpi: monthly((i) => 100 + i * 0.2),
  usunemp: monthly((i) => i >= 115 ? 4.8 : 4.0), ushy: daily(() => 4.0), usvix: daily(() => 32), usdxy: daily(() => 120),
  krbase: monthly(() => 2.5), kospi: daily((i) => 2000 + i), sp500: daily((i) => 3000 + i), usdkrw: daily(() => 1400), wti: daily(() => 70),
};
const dashKeys = new Set(['us2y', 'us10y', 'usrrp', 'ustga', 'usbs', 'usm2', 'krm2', 'uscpi', 'krcpi', 'usunemp', 'ushy', 'usvix', 'usdxy', 'krbase']);
let fail500 = false;
const srv = http.createServer((q, r) => {
  if (q.url === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (q.url.startsWith('/api/macro/series')) {
    if (fail500) { r.statusCode = 500; return r.end(JSON.stringify({ error: 'boom' })); }
    const dash = q.url.includes('group=dash'); const rows = [];
    Object.keys(S).forEach((k) => { if (dashKeys.has(k) === dash) S[k].forEach((p) => rows.push({ series: k, date: p.date, value: p.value })); });
    return r.end(JSON.stringify({ rows }));
  }
  r.statusCode = 404; r.end(JSON.stringify({ error: 'mock 404' }));
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
const pg = await b.newPage({ viewport: { width: 390, height: 900 } });
const errs = []; pg.on('pageerror', (e) => errs.push(String(e)));
const ok = (n, v, d) => console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : ''));
await pg.goto('http://localhost:' + srv.address().port + '/#econ'); await pg.waitForTimeout(800);
ok('경제 상황 화면이 열림', await pg.isVisible('#tabEcon'));
const row = (name) => pg.evaluate((n) => { const tr = Array.from(document.querySelectorAll('#econOut tbody tr')).find((t) => t.children[0] && t.children[0].textContent.includes(n)); return tr ? tr.innerText.replace(/\s+/g, ' ') : null; }, name);
const t10 = await row('미국 국채 10년'); ok('미국 10년 최신값 4.00', t10 && t10.includes('4.00'), t10);
const t2 = await row('미국 국채 2년'); ok('미국 2년 최신값 4.50', t2 && t2.includes('4.50'), t2);
const sp = await row('미국 10년-2년'); ok('금리차 -0.50 → 역전 신호', sp && sp.includes('-0.50') && sp.includes('역전'), sp);
const nl = await row('순유동성'); ok('순유동성 = (7,000,000-800,000)/1000-100 = 6,100', nl && nl.includes('6,100') , nl);
const m2 = await row('미국 M2 증가율'); const i0 = 117, exp = ((20000 + i0 * 50) / (20000 + (i0 - 12) * 50) - 1) * 100; ok('M2 증가율(전년동월비) 계산', m2 && m2.includes(exp.toFixed(2)), m2 + ' exp=' + exp.toFixed(2));
const cp = await row('미국 소비자물가'); ok('물가 상승률 4.00% → 물가 부담', cp && cp.includes('4.00') && cp.includes('물가 부담'), cp);
const un = await row('미국 실업률'); ok('Sahm 룰 발동(3개월 평균 4.8 vs 직전 최저 4.0)', un && un.includes('삼 룰 발동'), un);
const vx = await row('VIX'); ok('VIX 32 → 공포 높음', vx && vx.includes('공포 높음'), vx);
const hy = await row('하이일드'); ok('하이일드 4.0 → 중립', hy && hy.includes('중립'), hy);
const kr = await row('한국 M2'); ok('한국 M2 자료 없으면 "자료 없음"(숫자 지어내지 않음)', kr && kr.includes('자료 없음'), kr);
const kb = await row('한국 기준금리'); ok('한국 기준금리 2.50, 동결 수준', kb && kb.includes('2.50') && kb.includes('동결'), kb);
const k10 = await row('국고채 10년-3년'); ok('한국 장단기 금리차 +0.40 정상', k10 && k10.includes('0.40') && k10.includes('정상'), k10);
ok('그래프 SVG가 지표마다 그려짐', (await pg.locator('#econOut svg').count()) >= 20, 'n=' + await pg.locator('#econOut svg').count());
ok('종합 점수 같은 단일 점수가 없음', !(await pg.innerText('#econOut')).includes('종합 점수'));
await pg.click('#econOut button >> text="한국"'); await pg.waitForTimeout(100);
const onlyKr = await pg.evaluate(() => document.getElementById('econOut').innerText);
ok('한국만 보기: 미국 지표 숨김', onlyKr.includes('한국 기준금리') && !onlyKr.includes('미국 국채 2년'));
await pg.click('#econOut button:has-text("유동성")'); await pg.waitForTimeout(100);
ok('분야 필터(유동성)', (await pg.innerText('#econOut')).includes('한국 M2') );
await pg.click('#econOut button >> text="미국"'); await pg.waitForTimeout(100);
ok('미국+유동성에 TGA·역레포 표시', (await pg.innerText('#econOut')).includes('재무부 TGA') && (await pg.innerText('#econOut')).includes('역레포'));
const nsvg = await pg.locator('#econOut svg').count();
await pg.click('#econOut button:has-text("1년")'); await pg.waitForTimeout(100);
ok('기간 1년 선택 후에도 그래프 유지', (await pg.locator('#econOut svg').count()) === nsvg);
const ow = await pg.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 2);
ok('모바일 390px에서 페이지 가로 넘침 없음', ow);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ.png'), fullPage: false });
// 서버 오류
fail500 = true; await pg.click('#tabEcon button:has-text("새로고침")'); await pg.waitForTimeout(500);
ok('서버 오류 시 안내(빈 화면 아님)', (await pg.innerText('#econStatus')).includes('불러오지 못했습니다'));

// ---- 지수 방향 확률 모델
await pg.goto('http://localhost:' + srv.address().port + '/#econ'); await pg.waitForTimeout(1200);
const mt = await pg.innerText('#econModelOut');
ok('코스피·S&P500 확률 카드가 나옴', mt.includes('코스피 — 앞으로 3개월') && mt.includes('S&P500 — 앞으로 3개월'));
ok('과거 채점(Brier)·기준선 비교 표시', mt.includes('과거 채점') && mt.includes('기준선'));
const mtc = await pg.evaluate(() => document.getElementById('econModelOut').textContent);
ok('쉬운 설명과 한계(Welch & Goyal) 포함', mtc.includes('아주 쉬운 설명') && mtc.includes('Welch & Goyal') && mtc.includes('시험 보기'));
const sums = await pg.evaluate(() => Array.from(document.querySelectorAll('#econModelOut [title]')).map((e) => e.getAttribute('title')));
ok('막대 3칸(상승·중립·하락)씩 그려짐', sums.length >= 12, 'n=' + sums.length);
const math = await pg.evaluate(() => {
  // 1) 신호가 있는 합성 자료: x>0이면 상승(0), x<0이면 하락(2) → 학습 후 확률이 그 방향으로 기울어야 함
  const rows = []; let seed = 7; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
  for (let i = 0; i < 120; i++) { const x = rnd() * 2 - 1; rows.push({ x: [x], y: x > 0.2 ? 0 : x < -0.2 ? 2 : 1 }); }
  const m = econFit(rows, 1); const up = econPredict(m, [0.9]), dn = econPredict(m, [-0.9]);
  // 2) 확률은 합이 1
  const sum = up[0] + up[1] + up[2];
  // 3) 미래 누수 없음: D 이후 가격을 바꿔도 D 시점 특징값은 같다
  const mk = (bump) => { const px = []; for (let i = 0; i < 800; i++) { const d = new Date(Date.UTC(2020, 0, 1 + i)); px.push({ date: d.toISOString().slice(0, 10).replace(/-/g, ''), value: 100 + i * 0.1 + (i > 600 ? bump : 0) }); } return px; };
  const S1 = { kospi: mk(0), usvix: mk(0), us10m2: mk(0) }, S2 = { kospi: mk(500), usvix: mk(0), us10m2: mk(0) };
  const D = '20210601'; const f = { id: 'm12_1' }, f3 = { id: 'm3' };
  const same = econFeatVal('kospi', f, S1, D) === econFeatVal('kospi', f, S2, D) && econFeatVal('kospi', f3, S1, D) === econFeatVal('kospi', f3, S2, D);
  return { upUp: up[0] > up[2] + 0.2, dnDn: dn[2] > dn[0] + 0.2, sum, same };
});
ok('모델이 신호 있는 자료에서 방향을 학습', math.upUp && math.dnDn, JSON.stringify(math));
ok('확률 합 = 1', Math.abs(math.sum - 1) < 1e-9);
ok('특징값에 미래 가격이 새지 않음', math.same);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_model.png') });
ok('JS 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); process.exit(0);
