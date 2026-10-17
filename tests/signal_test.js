// 시그널·수익률 분해 순수 함수 테스트
const fs = require('fs'), path = require('path');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('// ===== 시그널·수익률 분해'), j = text.indexOf('// ===== /시그널·수익률 분해 =====');
if (i < 0 || j < 0) throw new Error('구간 없음');
const pre = "const anFin = (v) => v != null && typeof v === 'number' && isFinite(v); const anPct = (v) => (v*100).toFixed(1)+'%'; const anEsc = (s) => String(s); const anBadge = () => ''; const anSection = () => ''; const AN_STATE = {}; let AN_CTX = null;";
const S = new Function(pre + text.slice(i, j) + '\nreturn { sigDecomp, sigCompare, sigCloseAsOf, sigExpected, sigDetect, sigSma, sigAvg, sigMedian };')();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const near = (a, b, t) => Math.abs(a - b) <= (t || 1e-9);
// 6개 연도: EPS 연 10% 성장, PER 10 고정, 주당배당 20원
function mk(over) {
  const L = []; for (let k = 0; k < 6; k++) { const eps = 100 * Math.pow(1.1, k); L.push({ year: 2019 + k, eps, shareChg: 0, ebit: 100, ni: 100, fcf: 100, r: { price_at_period_end: eps * 10, dividend_per_share: 20 } }); }
  return over ? over(L) : L;
}
let d = S.sigDecomp(mk(), 3);
check('EPS 연 10%·PER 불변 → 주가 CAGR 10%, PER 변화 0', near(d.epsG, 0.1, 1e-9) && near(d.perCh, 0, 1e-9) && near(d.priceG, 0.1, 1e-9));
check('항등식: (1+EPS성장)(1+PER변화)−1 = 주가 CAGR', near(d.exactPrice, d.priceG, 1e-12));
check('배당수익률 > 0, 총수익률 > 주가 CAGR', d.divY > 0 && d.tr > d.priceG, d.divY.toFixed(4) + ' ' + d.tr.toFixed(4));
check('덧셈 근사와 총수익률 차이는 작음(<1%p)', Math.abs(d.approx - d.tr) < 0.01, (d.approx - d.tr).toFixed(5));
// 배당수익률 직접 계산: 연 20/(전년말 주가)의 평균
{ const L = mk(); let s = 0; for (let k = 3; k < 6; k++) s += 20 / L[k - 1 + 0].r.price_at_period_end; const y = S.sigDecomp(L, 3); const L2 = L.slice(2); let s2 = 0; for (let k = 1; k <= 3; k++) s2 += 20 / L2[k - 1].r.price_at_period_end; check('연평균 배당수익률 = 평균(DPS/전년말 주가)', near(y.divY, s2 / 3, 1e-12)); }
// 재평가: EPS 불변, 주가 2배
d = S.sigDecomp(mk((L) => { L.forEach((x, k) => { x.eps = 100; x.r.price_at_period_end = 1000 * (k === 5 ? 2 : 1); }); return L; }), 1);
check('EPS 불변·주가 2배 → PER 변화 +100%, EPS 성장 0', near(d.epsG, 0, 1e-12) && near(d.perCh, 1, 1e-12) && near(d.priceG, 1, 1e-12));
d = S.sigDecomp(mk((L) => { L[5].eps = -5; return L; }), 1);
check('끝 EPS 적자 → 산출 불가(거짓 수치 없음)', !!d.err && d.epsG === undefined, d.err);
d = S.sigDecomp(mk((L) => { L[4].shareChg = 0.5; return L; }), 3);
check('주식수 30%↑ 변동 구간 → 분해 안 함', /30%/.test(d.err));
d = S.sigDecomp(mk((L) => { L[4].r.dividend_per_share = null; return L; }), 3);
check('배당 자료 없는 해가 있으면 배당·총수익률만 null, 가격 분해는 유지', d.divY === null && d.tr === null && near(d.priceG, 0.1, 1e-9));
d = S.sigDecomp(mk((L) => { L.splice(3, 1); return L; }), 3);
check('빠진 연도 → 산출 불가', !!d.err);
check('자료 부족(연도 수) → 산출 불가', !!S.sigDecomp(mk().slice(0, 3), 3).err);
d = S.sigDecomp(mk((L) => { L[2].r.price_at_period_end = null; return L; }), 5);
check('기간말 주가 없음 → 산출 불가 또는 총수익 null', !!d.err || (d.tr === null));
// 기대수익률
check('기대수익률: g 10%, PER 3년간 −30%, 배당 2% → 약 −0.4%', near(S.sigExpected(0.02, 0.1, -0.3, 3), (1.1) * Math.pow(0.7, 1 / 3) - 1 + 0.02, 1e-12));
check('PER 유지·배당 없음 → g 그대로', near(S.sigExpected(null, 0.07, 0, 5), 0.07, 1e-12));
check('잘못된 입력(g≤−100%, 기간 0) → null', S.sigExpected(0, -1, 0, 3) === null && S.sigExpected(0, 0.1, 0, 0) === null && S.sigExpected(0, 0.1, -1, 3) === null);
// 변화 감지
const base = { list: mk(), ttm: null };
let r = S.sigDetect(base, null);
check('변화 없음 + 주가 없음 → 항목 0, priceReady=false', r.items.length === 0 && r.meta.priceReady === false);
const chg = { list: mk((L) => { L[4].ebit = -10; L[5].ebit = 20; L[4].ni = 50; L[5].ni = -5; L[4].r.dividend_per_share = 0; L[5].r.dividend_per_share = 100; return L; }), ttm: { ebit: -30 } };
r = S.sigDetect(chg, null);
const ids = r.items.map((x) => x.id + ':' + x.tone).join(',');
check('영업이익 흑자전환·순이익 적자전환·배당 시작 감지', /chg_ebit:good/.test(ids) && /chg_ni:bad/.test(ids) && /chg_div:good/.test(ids), ids);
check('TTM 부호 전환(영업이익 +20억→TTM −30)', /chg_ttm:bad/.test(ids));
check('fcf 부호 같으면 감지 없음', !/chg_fcf/.test(ids));
// 가격: 300일, 우상향, 마지막 5일 거래 3배, 중간에 +30% 하루, 1000만주 거래
const n = 300, cl = [], v = [], dt = [];
for (let k = 0; k < n; k++) { cl.push(1000 + k * 2); v.push(100); dt.push(String(20240101 + k)); }
for (let k = n - 5; k < n; k++) v[k] = 300;
cl[150] = cl[149] * 1.3; for (let k = 151; k < n; k++) cl[k] = cl[150] + (k - 150) * 2; v[150] = 12000000;
r = S.sigDetect(base, { c: cl, v, d: dt, tv: null });
const by = (id) => r.items.find((x) => x.id === id);
check('주가 자료 있음 → priceReady', r.meta.priceReady === true);
check('거래량 급증(3배) 감지', by('vol') && /급증/.test(by('vol').label) && near(r.meta.volRatio, 3, 1e-9), by('vol') && by('vol').detail);
check('정배열 감지', by('trend') && /정배열/.test(by('trend').label), by('trend') && by('trend').label);
check('상한가권 1회·1,000만주 1일 집계 + 경고 근거 평가 포함', by('spike') && /1회/.test(by('spike').label) && /1일/.test(by('spike').label) && /Bali/.test(by('spike').note));
check('52주 고점 대비 계산', by('hi52') && r.meta.fromHi <= 0 && r.meta.fromHi > -0.2);
check('130거래일 미만이면 가격 시그널 없음', S.sigDetect(base, { c: cl.slice(0, 100), v: v.slice(0, 100), d: dt.slice(0, 100) }).meta.priceReady === false);
// 거래대금 우선 사용, 결측 처리
r = S.sigDetect(base, { c: cl, v, d: dt, tv: v.map((x) => x * 10) });
check('거래대금 자료가 있으면 거래대금 기준', /거래대금/.test(r.items.find((x) => x.id === 'vol').label));
r = S.sigDetect(base, { c: cl, v, d: dt, tv: v.map(() => null) });
check('거래대금이 전부 결측이면 거래량으로 대체', /거래량/.test(r.items.find((x) => x.id === 'vol').label));
// 최근 1개월 급등 경고
const cl2 = cl.slice(); for (let k = n - 20; k < n; k++) cl2[k] = cl2[n - 21] * (1 + 0.02 * (k - (n - 21)));
r = S.sigDetect(base, { c: cl2, v, d: dt });
check('1개월 +30%↑ → 단기 과열 경고', r.items.some((x) => x.id === 'rev1m'), String(r.meta.r1m));

// ----- 분해로 복원한 주가변화 vs 실제 주가변화(일별 시세) -----
{
  const L = mk(); const dd = S.sigDecomp(L, 3);                               // 2021→2024 (2021→2024)
  const dates = [], cl = [];
  const pxOf = (yr) => L.find((q) => q.year === yr).r.price_at_period_end;
  for (let y = 2019; y <= 2024; y++) { dates.push(y + '1229'); cl.push(pxOf(y)); }  // 기간말 이틀 전 종가(연말 휴장 가정)
  const px = { d: dates, c: cl };
  check('기준일 이전 가장 가까운 종가를 찾음(12/31 → 12/29)', S.sigCloseAsOf(px, '20231231') === pxOf(2023));
  check('시세 범위 밖이면 null', S.sigCloseAsOf(px, '20181231') === null && S.sigCloseAsOf(px, '20251231') === null);
  check('10일보다 오래된 종가는 쓰지 않음', S.sigCloseAsOf({ d: ['20231201'], c: [10] }, '20231231') === null && S.sigCloseAsOf({ d: ['20231201', '20250101'], c: [10, 11] }, '20231231') === null);
  const k = S.sigCompare(dd, px);
  check('일별 시세가 보고서 기간말 주가와 같으면 실제 = 복원, 차이 0, 경고 없음', near(k.real, k.restored, 1e-12) && near(k.gap, 0, 1e-12) && k.flag === false, k.real + ' vs ' + k.restored);
  const px2 = { d: dates, c: cl.map((v, i) => i === cl.length - 1 ? v * 1.2 : v) };  // 마지막 기간말 종가만 20% 어긋남
  const k2 = S.sigCompare(dd, px2);
  check('끝 종가가 20% 다르면 연환산 차이가 생기고 ⚠(1%p 초과)', k2.gap > 0.05 && k2.flag === true, k2.gap.toFixed(4));
  const k3 = S.sigCompare(dd, null);
  check('일별 시세가 없으면 실제 = null, 차이 = null, 경고 없음', k3.real === null && k3.gap === null && k3.flag === false);
  check('시작 기간말이 시세 범위 밖이면 비교하지 않음(null)', S.sigCompare(S.sigDecomp(L, 5), { d: dates.slice(2), c: cl.slice(2) }).real === null);
  check('근사 오차 = 덧셈 근사 − 총수익률', near(k.approxErr, dd.approx - dd.tr, 1e-12) && Math.abs(k.approxErr) < 0.01);
}
console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과'); process.exit(fail ? 1 : 0);
