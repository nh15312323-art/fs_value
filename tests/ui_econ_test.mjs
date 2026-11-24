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
  usunemp: monthly((i) => i >= 115 ? 4.8 : 4.0), ushy: daily(() => 4.0), usvix: daily((i) => 32 + (i % 50) / 10), usdxy: daily((i) => 120 + (i % 20) / 10),
  krbase: monthly(() => 2.5), krlei: monthly((i) => i >= 116 ? 98.5 : 99.5), sp500o: monthly((i) => 100 + i), usres: weekly((i) => i >= 508 ? 2400000 : 3000000), sofr: daily((i) => 4.55 + (i % 3) / 100), iorb: daily(() => 4.4), effr: daily(() => 4.33), tb3m: daily(() => 4.2), kcall: daily((i) => 2.5 + (i % 3) / 100), kcd91: daily((i) => 3.0 + (i % 5) / 100), kcp91: daily((i) => 3.4 + (i % 5) / 100), kospi: daily((i) => 2000 + i), sp500: daily((i) => 3000 + i), usdkrw: daily((i) => 1400 + (i % 40)), wti: daily(() => 70),
};
const dashKeys = new Set(['us2y', 'us10y', 'usrrp', 'ustga', 'usbs', 'usm2', 'krm2', 'uscpi', 'krcpi', 'usunemp', 'ushy', 'usvix', 'usdxy', 'krbase', 'krlei', 'sp500o', 'usres', 'sofr', 'iorb', 'effr', 'tb3m', 'kcall', 'kcd91', 'kcp91']);
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
const kr = await row('한국 신M2'); ok('한국 신M2 자료 없으면 "자료 없음"(숫자 지어내지 않음)', kr && kr.includes('자료 없음'), kr);
const kb = await row('한국 기준금리'); ok('한국 기준금리 2.50, 동결 수준', kb && kb.includes('2.50') && kb.includes('동결'), kb);
const k10 = await row('국고채 10년-3년'); ok('한국 장단기 금리차 +0.40 정상', k10 && k10.includes('0.40') && k10.includes('정상'), k10);
const sm = await row('SOFR - IORB'); ok('SOFR 4.55-IORB 4.40 = +0.15 → 자금 압박 조짐(임계값은 실무 관행으로 표기)', sm && sm.includes('0.15') && sm.includes('자금 압박 조짐'), sm);
const cdm = await row('CD(91일) - 기준금리'); ok('CD 3.0x - 기준금리 2.5 ≈ +0.5 → 경색 조짐', cdm && cdm.includes('단기자금 경색 조짐'), cdm);
const cpm = await row('CP(91일) - CD(91일)'); ok('CP-CD 스프레드 행이 있음(+0.4 보통)', cpm && cpm.includes('보통'), cpm);
const rs = await row('지급준비금'); ok('미국 지급준비금: 3개월 감소 신호', rs && rs.includes('3개월 감소'), rs);
const lei = await row('선행지수 순환변동치'); ok('선행지수 순환변동치 98.5(100 미만·하락) → 둔화·악화, 신호 근거는 실무 관행', lei && lei.includes('98.5') && lei.includes('둔화·악화'), lei);
const spo = await row('OECD 월평균'); ok('미국 주가지수 OECD 대용 지표는 "참고용"으로 표기', spo && spo.includes('참고용'), spo);
const ax = await pg.evaluate(() => { const sv = document.querySelector('#econOut svg[aria-label$="시계열"]'); return { txt: sv ? sv.textContent : '', ticks: sv ? sv.querySelectorAll('line[stroke-dasharray="2 3"]').length : 0 }; });
ok('각 지표 그래프 가로축에 연도 눈금이 있음(3년 범위: 연 단위)', /20\d\d/.test(ax.txt) && ax.ticks >= 2, JSON.stringify(ax).slice(0, 120));
ok('기간 버튼에 10년이 있음', await pg.isVisible('#econOut button:has-text("10년")'));
ok('단기자금 분야 버튼이 있음', await pg.isVisible('#econOut button:has-text("단기자금")'));
ok('그래프 SVG가 지표마다 그려짐', (await pg.locator('#econOut svg').count()) >= 20, 'n=' + await pg.locator('#econOut svg').count());
ok('종합 점수 같은 단일 점수가 없음', !(await pg.innerText('#econOut')).includes('종합 점수'));
await pg.click('#econOut button >> text="한국"'); await pg.waitForTimeout(100);
const onlyKr = await pg.evaluate(() => document.querySelector('#econOut .ec-tbl-wrap').innerText);
ok('한국만 보기: 미국 지표 숨김', onlyKr.includes('한국 기준금리') && !onlyKr.includes('미국 국채 2년'));
await pg.click('#econOut button:has-text("유동성")'); await pg.waitForTimeout(100);
ok('분야 필터(유동성)', (await pg.innerText('#econOut')).includes('한국 신M2') );
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
ok('코스피·S&P500 확률 카드가 나옴', mt.includes('코스피') && mt.includes('S&P500') && mt.includes('앞으로 3개월'));
ok('과거 채점(Brier)·기준선 비교 표시', mt.includes('과거 채점') && mt.includes('기준선'));
const mtc = await pg.evaluate(() => document.getElementById('econModelOut').textContent);
ok('쉬운 설명과 한계(Welch & Goyal) 포함', mtc.includes('더 자세히') && mtc.includes('Welch & Goyal') && mtc.includes('시험 보기'));
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
await pg.locator('#econAllocOut').screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_alloc.png') }).catch(() => {});

// ---- 겹쳐보기(최대 5개) + 모델 설명
await pg.goto('http://localhost:' + srv.address().port + '/#econ'); await pg.waitForTimeout(1200);
const ovSel = () => pg.evaluate(() => econOv.slice());
ok('겹쳐보기 그래프가 기본 4개로 그려짐', (await pg.locator('#econOvSvg').count()) === 1 && (await pg.locator('#econOvSvg path').count()) === 4, JSON.stringify(await ovSel()));
await pg.click('#econOut button[data-k="usdxy"]'); await pg.waitForTimeout(100);
ok('5번째 선택까지 가능', (await ovSel()).length === 5 && (await pg.locator('#econOvSvg path').count()) === 5);
await pg.click('#econOut button[data-k="wti"]'); await pg.waitForTimeout(100);
ok('6번째는 막고 안내 문구 표시', (await ovSel()).length === 5 && (await pg.innerText('#econOvMsg')).includes('최대 5개'));
await pg.click('#econOut button[data-k="usdxy"]'); await pg.waitForTimeout(100);
ok('해제하면 다시 4개', (await ovSel()).length === 4);
await pg.click('#econOut button:has-text("미국 금리 (2년·10년·금리차)")'); await pg.waitForTimeout(100);
ok('바로 고르기 프리셋 적용', JSON.stringify(await ovSel()) === JSON.stringify(['us2y', 'us10y', 'us10m2']));
await pg.click('#econOut button:has-text("시작=100")'); await pg.waitForTimeout(100);
ok('시작=100에서 음수 값 지표(금리차)는 사유와 함께 제외', (await pg.innerText('#econOut')).includes('시작=100') && (await pg.innerText('#econOut')).includes('0 이하 값이 있어'));
await pg.click('#econOut button:has-text("표준화(z-점수)")'); await pg.waitForTimeout(100);
const sd = await pg.evaluate(() => { const L = econOvBuild().skipped; return L; });
ok('표준화 모드에서 상수 지표(값이 안 변함)는 제외 사유 표시', sd.length >= 3 && sd.every((x) => x.includes('표준화 불가')), JSON.stringify(sd));
await pg.click('#econOut button:has-text("모두 해제")'); await pg.waitForTimeout(100);
await pg.click('#econOut button[data-k="kospi"]'); await pg.click('#econOut button[data-k="sp500"]'); await pg.waitForTimeout(100);
await pg.locator('#econOvSvg').scrollIntoViewIfNeeded(); const box2 = await pg.locator('#econOvSvg').boundingBox();
await pg.mouse.move(box2.x + box2.width * 0.5, box2.y + 60); await pg.waitForTimeout(150);
const tip = await pg.innerText('#econOvTip');
ok('마우스를 올리면 날짜와 실제 값이 보임', /\d{4}-\d{2}-\d{2}/.test(tip) && tip.includes('코스피') && tip.includes('S&P500'), tip.replace(/\s+/g, ' '));
ok('모델 설명이 접지 않고 바로 보임(5단계·읽는 법·근거 수준)', await pg.isVisible('text=이 확률은 무엇이고, 어떻게 만들어지나요?') && await pg.isVisible('text=한 줄 요약') && await pg.isVisible('text=화면 읽는 법'));
const ovAx = await pg.evaluate(() => { const sv = document.getElementById('econOvSvg'); return sv ? sv.querySelectorAll('line[stroke-dasharray="2 3"]').length + '|' + (sv.textContent.match(/20\d\d(\.\d\d)?/g) || []).join(',') : ''; });
ok('겹쳐보기 그래프 가로축에도 시기 눈금이 있음', /^[2-9]\|.*20\d\d/.test(ovAx), ovAx);
const introTxt = await pg.evaluate(() => document.getElementById('econModelOut').textContent);
ok('설명에 기준선·시험 보기·근거 수준 포함', introTxt.includes('기준선') && introTxt.includes('시험 보기') && introTxt.includes('Estrella') && introTxt.includes('Jegadeesh'));
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_ov.png'), fullPage: true });
const kp = await pg.innerText('#econKpi');
// ---- 자산배분 가이드 ----
await pg.waitForSelector('#econAllocOut .ec-card', { timeout: 5000 }).catch(() => {});
const al = await pg.evaluate(() => document.getElementById('econAllocOut').textContent);
ok('자산배분 가이드가 화면에 있고 기본 비중·참고 비중 막대가 둘 다 보임', al.includes('자산배분 가이드') && al.includes('참고 비중') && al.includes('기본 비중'), al.slice(0, 80));
ok('신호별 근거 수준(유력 근거/실무 관행)과 출처가 표시됨', al.includes('유력 근거') && al.includes('실무 관행') && al.includes('Faber') && al.includes('Estrella'));
ok('조합 규칙은 검증되지 않은 가설이라고 명시', al.includes('증명되지 않은 것') && al.includes('가설') && al.includes('백테스트'));
ok('단일 "매수 점수"를 만들지 않음', !al.includes('매수 점수') && !al.includes('AI 점수'));
const allocChk = await pg.evaluate(() => {
  const out = [];
  ['KR', 'US'].forEach((m) => ['cons', 'mid', 'aggr'].forEach((p) => { const R = econAllocCompute(m, p); out.push({ m: m, p: p, sum: R.w[0] + R.w[1] + R.w[2], min: Math.min(R.w[0], R.w[1], R.w[2]), dev: Math.abs(R.w[0] - R.base[0]) }); }));
  return out;
});
ok('6개 조합 모두 합계 100%·음수 없음·주식 편차 ≤ 상한(15)+0.5', allocChk.every((r) => r.sum === 100 && r.min >= 0 && r.dev <= 15.5), JSON.stringify(allocChk.slice(0, 2)));
// 극단 시나리오: 모든 신호가 경계 → 합계 -16 → 상한 -15 적용(중립형 50 → 35)
const ext = await pg.evaluate(() => {
  const keep = econSer; const mk = (n, f) => { const o = []; for (let i = 0; i < n; i++) { const d = new Date(Date.UTC(2020, i, 28)); o.push({ date: d.toISOString().slice(0, 10).replace(/-/g, ''), value: f(i) }); } return o; };
  econSer = Object.assign({}, keep, { kospi: mk(60, (i) => 3000 - i * 20), usvix: mk(60, (i) => 12 + i), kr10m3: mk(60, () => -0.3), ushy: mk(60, (i) => 3 + i / 10), krlei: mk(60, (i) => 101 - i * 0.05) });
  const R = econAllocCompute('KR', 'mid'); econSer = keep; return { raw: R.raw, T: R.T, w: R.w, n: R.sg.length, scores: R.sg.map((x) => x.score) };
});
ok('모든 신호가 경계(-)면 합계 -16%p → 상한 -15%p로 제한(중립형 주식 50 → 35)', ext.raw === -16 && ext.T === -15 && ext.w[0] === 35 && ext.w.reduce((a, b) => a + b, 0) === 100, JSON.stringify(ext));
const none = await pg.evaluate(() => { const keep = econSer; econSer = Object.assign({}, keep, { kospi: [], usvix: [], ushy: [], krlei: [], kr10m3: [] }); const R = econAllocCompute('KR', 'mid'); econSer = keep; return { raw: R.raw, w: R.w, na: R.sg.filter((x) => x.score == null).length }; });
ok('자료가 없으면 신호를 0으로 두고 기본 비중 유지, 판정 불가로 표시', none.raw === 0 && none.w.join() === '50,40,10' && none.na === 5, JSON.stringify(none));
await pg.click('#econAllocOut button[data-val="US"]'); await pg.waitForTimeout(100);
const alUs = await pg.evaluate(() => document.getElementById('econAllocOut').textContent);
ok('미국 기준으로 전환하면 S&P500 추세와 미국 금리차로 바뀜', alUs.includes('S&P500 10개월 추세') && alUs.includes('10Y-2Y') && !alUs.includes('선행지수 순환변동치'));
await pg.click('#econAllocOut button[data-val="aggr"]'); await pg.waitForTimeout(100);
ok('투자 성향을 바꾸면 기본 비중이 70/25/5로 바뀜', (await pg.evaluate(() => document.getElementById('econAllocOut').textContent)).includes('공격형 70/25/5'));
await pg.click('#econAllocOut button[data-val="KR"]'); await pg.click('#econAllocOut button[data-val="mid"]');
// ---- 백테스트 ----
await pg.click('#econAllocOut button:has-text("백테스트 실행")'); await pg.waitForSelector('#econBtOut table', { timeout: 8000 }).catch(() => {});
const bt = await pg.evaluate(() => ({ txt: document.getElementById('econBtOut').textContent, rows: document.querySelectorAll('#econBtOut tbody tr').length, svg: !!document.querySelector('#econBtOut svg') }));
await pg.locator('#econBtOut').screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_bt.png') }).catch(() => {});
ok('백테스트 표에 12개 규칙(가이드·정적·주식100%·신호 1개씩·상한 민감도)이 나옴', bt.rows === 12, 'rows=' + bt.rows + ' ' + bt.txt.slice(0, 80));
ok('한 줄 요약·근사 수익률·완전한 표본 밖 검증이 아님을 명시', bt.txt.includes('한 줄 요약') && bt.txt.includes('근사') && bt.txt.includes('표본 밖 검증이 아니며') && bt.txt.includes('12개 규칙'));
ok('누적 성과 그래프(연도 눈금)가 그려짐', bt.svg);
const btc = await pg.evaluate(() => {
  const out = {}; const keep = econBtCost;
  econBtCost = 0; const P = econBtPrepare(); if (P.err) return { err: P.err };
  // (1) 정적 기본 비중, 비용 0: 매월 수익률 = 기본 비중 x (주식, 채권, 현금 수익률)
  const base = ECON_ALLOC_BASE.mid.w; const st = econBtSim(P, { mode: 'static', prof: 'mid' });
  let maxErr = 0; for (let k = 0; k < st.rets.length; k++) { const a = P.use[k], b = P.use[k + 1]; const exp = (base[0] * (b.K / a.K - 1) + base[1] * (a.y / 1200 - econBtDur * (b.y - a.y) / 100) + base[2] * (a.c / 1200)) / 100; maxErr = Math.max(maxErr, Math.abs(exp - st.rets[k])); }
  out.maxErr = maxErr;
  // (2) 비용: 비용이 커질수록 가이드 누적이 작아짐(회전율>0일 때)
  const gv = { mode: 'guide', prof: 'mid', cap: 15, mask: null };
  const cum = (r) => r.rets.reduce((c, x) => c * (1 + x), 1);
  const c0 = cum(econBtSim(P, gv)); econBtCost = 0.01; const c1 = cum(econBtSim(P, gv)); econBtCost = keep; out.c0 = c0; out.c1 = c1;
  // (3) 상한 5%p면 주식 비중이 기본에서 5%p 넘게 벗어나지 않음
  const g5 = econBtSim(P, { mode: 'guide', prof: 'mid', cap: 5, mask: null }); out.dev5 = Math.max.apply(null, g5.ws.map((w) => Math.abs(w[0] - base[0]))); out.sum = Math.max.apply(null, g5.ws.map((w) => Math.abs(w[0] + w[1] + w[2] - 100)));
  out.n = P.use.length; return out;
});
ok('정적 기본 비중의 월 수익률이 수식과 일치(비용 0, 오차 < 1e-12)', btc.maxErr < 1e-12, JSON.stringify(btc));
ok('거래비용을 높이면 누적 성과가 낮아짐(비용 0 > 비용 1%)', btc.c0 > btc.c1, btc.c0 + ' vs ' + btc.c1);
ok('상한 ±5%p 변형은 주식 비중 편차 ≤ 5, 합계 100', btc.dev5 <= 5.0001 && btc.sum < 1e-9, JSON.stringify([btc.dev5, btc.sum]));
// 미래 자료 차단: 마지막 12개월 코스피·VIX 값을 크게 바꿔도 앞쪽 달의 신호는 그대로여야 함
const la = await pg.evaluate(() => {
  const keep = econSer; const P1 = econBtPrepare(); const cut = P1.use[P1.use.length - 13].date;
  const bump = (s) => s.map((p) => p.date > cut ? { date: p.date, value: p.value * 3 + 50 } : p);
  econSer = Object.assign({}, keep, { kospi: bump(keep.kospi), usvix: bump(keep.usvix), kr10m3: bump(keep.kr10m3), krlei: bump(keep.krlei) });
  const P2 = econBtPrepare(); econSer = keep;
  const m = P1.use.length - 14; let same = 0; for (let k = 0; k < m; k++) if (JSON.stringify(P1.recs[k]) === JSON.stringify(P2.recs[k])) same++;
  return { m: m, same: same, changed: JSON.stringify(P1.recs.slice(-3)) !== JSON.stringify(P2.recs.slice(-3)) };
});
ok('미래 자료를 바꿔도 과거 시점의 신호가 변하지 않음(look-ahead 없음)', la.m > 20 && la.same === la.m, JSON.stringify(la));
const bt2 = await pg.evaluate(() => { const keep = econSer; econSer = Object.assign({}, keep, { ktb3y: [] }); const P = econBtPrepare(); econSer = keep; return P.err || ''; });
ok('자료가 없으면 백테스트를 만들지 않고 이유를 안내', bt2.includes('국고채 3년'), bt2);
ok('핵심 지표 카드 8개(금리차·순유동성·VIX·HY·한국 금리차·환율·SOFR-IORB·CD-기준금리)', (await pg.locator('#econKpi .ec-kpis > .ec-card').count()) === 8 && kp.includes('순유동성') && kp.includes('VIX'));
await pg.setViewportSize({ width: 1100, height: 1000 }); await pg.waitForTimeout(200);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_desktop.png'), fullPage: true });
await pg.setViewportSize({ width: 390, height: 900 }); await pg.waitForTimeout(200);
await pg.screenshot({ path: path.join(os.tmpdir(), 'fs_value_econ_mobile.png'), fullPage: true });
ok('JS 오류 없음', errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); process.exit(0);
