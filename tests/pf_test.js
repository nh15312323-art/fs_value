// 사용법: node tests/pf_test.js  — 내 계좌·매매 관리 계산 엔진(원장·비중·포지션 크기·분할·사전 점검·행동 통계) 단위 테스트
const fs = require('fs'), path = require('path');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const a = text.indexOf('function pfLedger('), b = text.indexOf('export default {');
if (a < 0 || b < 0) throw new Error('엔진 구간을 찾지 못했습니다');
const E = new Function(text.slice(a, b) + '\nreturn { pfLedger, pfSummary, pfSizePosition, pfSplitTranches, pfTradeWarnings, pfBehavior, pfKelly };')();
let fail = 0;
const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const near = (x, y, e) => x != null && Math.abs(x - y) < (e || 1e-6);
let id = 0;
const T = (side, date, code, qty, price, o) => Object.assign({ id: ++id, side, trade_date: date, stock_code: code, corp_name: code === 'A' ? '에이' : '비이', qty, price, fee: 0, tax: 0 }, o || {});

// ① 원장: 평균단가·실현손익·현금
let L = E.pfLedger([T('deposit', '20250101', null, 1, 10000000), T('buy', '20250102', 'A', 100, 10000, { fee: 150 }), T('buy', '20250110', 'A', 100, 8000, { fee: 120 }), T('sell', '20250120', 'A', 50, 12000, { fee: 90, tax: 100 })]);
const pa = L.positions.filter((x) => x.code === 'A')[0];
// 비용 포함 원가 = 1,000,000+150 + 800,000+120 = 1,800,270 / 200주 = 9001.35
check('평균단가(수수료 포함, 이동평균) = 1,800,270 ÷ 200주', near(pa.avg, 9001.35), String(pa.avg));
// 매도 50주: 제거 원가 450,067.5, 수령 600,000−90−100 = 599,810 → 실현 149,742.5
check('실현손익 = 순수령액 − 매도분 평균원가', near(L.realized, 149742.5), String(L.realized));
check('잔여 수량 150주, 잔여 원가 = 평단 × 150', pa.qty === 150 && near(pa.cost, 9001.35 * 150, 1e-4));
// 현금: 10,000,000 − 1,000,150 − 800,120 + 599,810 = 8,799,540
check('현금 = 입금 − 매수(+수수료) + 매도(−수수료·세금)', near(L.cash, 8799540), String(L.cash));
check('수수료·세금 합계 = 150+120+90+100', near(L.fees, 460));
// 전량 매도 후 재매수: 보유기간 시작일 갱신
let L2 = E.pfLedger([T('buy', '20250101', 'A', 10, 100), T('sell', '20250111', 'A', 10, 120), T('buy', '20250301', 'A', 5, 100), T('sell', '20250401', 'A', 5, 90)]);
check('매도 기록: 첫 거래 +20% 10일 보유, 둘째 −10% 31일 보유(재매수 시점부터)', L2.sells.length === 2 && near(L2.sells[0].pct, 0.2) && L2.sells[0].holdDays === 10 && near(L2.sells[1].pct, -0.1) && L2.sells[1].holdDays === 31, JSON.stringify(L2.sells.map((s) => [s.pct, s.holdDays])));
// 보유보다 많이 매도: 오류로 기록하고 반영하지 않음
let L3 = E.pfLedger([T('buy', '20250101', 'A', 10, 100), T('sell', '20250102', 'A', 20, 100)]);
check('보유 초과 매도는 반영하지 않고 오류로 표시', L3.errors.length === 1 && L3.positions[0].qty === 10);
check('무효 처리한 거래는 계산에서 제외', E.pfLedger([T('buy', '20250101', 'A', 10, 100, { voided: 1 })]).positions.length === 0);

// ② 요약: 비중·집중도·경고
const L4 = E.pfLedger([T('deposit', '20250101', null, 1, 1000000), T('buy', '20250102', 'A', 100, 5000), T('buy', '20250102', 'B', 50, 4000)]);
const S = E.pfSummary(L4, { A: { close: 6000, date: '20250110' }, B: { close: 3000, date: '20250110' } }, { max_position_pct: 30, min_cash_pct: 30 }, { A: '반도체' }, '20250112');
// 현금 = 1,000,000 − 500,000 − 200,000 = 300,000, 평가 = 600,000+150,000 = 750,000, 총 1,050,000
check('총자산 = 현금 + 평가액', near(S.total, 1050000) && near(S.cash, 300000), String(S.total));
check('비중: A 57.1%, 현금 28.6%', near(S.rows[0].weight, 600000 / 1050000) && near(S.cashPct, 300000 / 1050000));
check('상위 1·3개 집중도', near(S.top1, 600000 / 1050000) && near(S.top3, 750000 / 1050000));
check('평가손익 = 평가 − 원가(150,000+... )', near(S.unreal, 750000 - 700000));
check('정책 한도 초과 경고(비중 30%, 현금 30% 미달)', S.warnings.some((w) => w.code === 'over_weight') && S.warnings.some((w) => w.code === 'low_cash'), JSON.stringify(S.warnings.map((w) => w.code)));
check('업종 미입력은 "미분류"로 묶음', S.sectors.some((x) => x.name === '미분류') && S.sectors.some((x) => x.name === '반도체'));
const S2 = E.pfSummary(L4, { A: { close: 6000, date: '20250110' } }, {}, {}, '20250112');
check('현재가 없는 종목이 있으면 부정확 경고 + partial', S2.partial && S2.warnings.some((w) => w.code === 'unpriced'));
check('가격 기준일이 7일 넘게 오래되면 경고', E.pfSummary(L4, { A: { close: 1, date: '20240101' }, B: { close: 1, date: '20240101' } }, {}, {}, '20250112').warnings.some((w) => w.code === 'stale'));

// ③ 포지션 크기
const base = { base: 100000000, riskPct: 1, buyPrice: 50000, riskPrice: 40000, maxPosPct: 10, cash: 30000000, minCashPct: 10, cashNeeds: 0, feePct: 0.015, adv: 2000000000, advPct: 5, curPosValue: 0 };
const r = E.pfSizePosition(base);
// 위험: 1,000,000 / 10,000 = 100주 ; 집중도: 10,000,000/50,000 = 200주 ; 현금: (30,000,000−10,000,000)/50,007.5 = 399주 ; 유동성: 100,000,000/50,000 = 2000주
check('위험 예산 한도 100주가 가장 보수적 → 100주', r.ok && r.qty === 100 && r.binding.join() === '위험 예산', JSON.stringify(r.limits.map((l) => [l.key, l.qty])));
check('투자금 5,000,000, 비중 5%, 매수 후 현금 약 25,000,000', near(r.amount, 5000000) && near(r.weightAfter, 0.05) && near(r.cashAfter, 30000000 - 5000000 - 750, 1), String(r.cashAfter));
check('위험관리 기준가까지 하락 시 손실 = 100주 × 10,000 = 1,000,000(자산의 1%)', near(r.scenarioLoss, 1000000) && near(r.scenarioLossPct, 0.01));
const rb = E.pfSizePosition(Object.assign({}, base, { bearPrice: 30000 }));
check('Bear 가격이 있으면 시나리오 손실은 Bear 기준(100주 × 20,000)', near(rb.scenarioLoss, 2000000) && rb.scenarioRef === 'Bear 가격');
const rc = E.pfSizePosition(Object.assign({}, base, { riskPct: 5 }));
check('위험 예산을 키우면 집중도 한도(200주)가 걸림', rc.qty === 200 && rc.binding.join() === '집중도(종목 최대 비중)', rc.binding.join());
const rd = E.pfSizePosition(Object.assign({}, base, { riskPct: 5, curPosValue: 9000000 }));
check('이미 9백만원 보유 → 집중도 한도 남은 1백만원 = 20주', rd.qty === 20, String(rd.qty));
const re = E.pfSizePosition(Object.assign({}, base, { riskPct: 5, cash: 10500000 }));
check('가용 현금이 적으면 현금 한도가 걸림', re.binding.join() === '가용 현금' && re.qty < 10, JSON.stringify([re.qty, re.binding]));
const rf = E.pfSizePosition(Object.assign({}, base, { riskPct: 5, maxPosPct: 50, adv: 100000000 }));
check('유동성: 하루 거래대금 1억의 5% = 500만원 = 100주', rf.qty === 100 && rf.binding.join() === '유동성(하루 거래대금 대비)', JSON.stringify([rf.qty, rf.binding]));
check('유동성 자료가 없으면 한도 미적용 + 경고', E.pfSizePosition(Object.assign({}, base, { adv: null })).warnings.some((w) => w.indexOf('유동성 자료가 없어') >= 0));
check('위험 기준가 ≥ 매수가면 계산하지 않고 오류', !E.pfSizePosition(Object.assign({}, base, { riskPrice: 50000 })).ok && E.pfSizePosition(Object.assign({}, base, { riskPrice: 60000 })).errors.length > 0);
check('필수 입력 누락(위험 예산·기준 자산)이면 오류', !E.pfSizePosition(Object.assign({}, base, { riskPct: null })).ok && !E.pfSizePosition(Object.assign({}, base, { base: 0 })).ok);
check('한도가 0주면 보류도 유효하다는 안내', E.pfSizePosition(Object.assign({}, base, { cash: 5000000 })).warnings.some((w) => w.indexOf('0주') >= 0));

// ④ 분할 수량
check('분할 100주 40/30/30 → 40,30,30', JSON.stringify(E.pfSplitTranches(100, [40, 30, 30])) === '[40,30,30]');
check('분할 10주 1:1:1 → 합계 10, 앞 회차에 나머지', JSON.stringify(E.pfSplitTranches(10, [1, 1, 1])) === '[4,3,3]', JSON.stringify(E.pfSplitTranches(10, [1, 1, 1])));
check('분할: 가중치 0인 회차는 0주', JSON.stringify(E.pfSplitTranches(10, [1, 0, 1])) === '[5,0,5]');

// ⑤ 사전 점검
const led = E.pfLedger([T('deposit', '20250101', null, 1, 100000000), T('buy', '20250102', 'A', 100, 10000)]);
const pol = { max_position_pct: 10, min_cash_pct: 10, cooldown_hours: 24 };
const now = Date.UTC(2025, 1, 1);
const plan = { status: '매수검토', created_ms: now - 2 * 3600000, entry_high: 10500, risk_price: 8000, max_qty: 150 };
const ids = (w) => w.map((x) => x.id).sort().join(',');
let w1 = E.pfTradeWarnings({ trade: { side: 'buy', code: 'A', qty: 10, price: 9000 }, ledger: led, policy: pol, plan, totalValue: 100000000, nowMs: now });
check('평단보다 낮은 추가매수 → avgdown(체크리스트 필요), 숙려 시간 경고', w1.some((x) => x.id === 'avgdown' && x.needsCheck) && w1.some((x) => x.id === 'cooldown'), ids(w1));
let w2 = E.pfTradeWarnings({ trade: { side: 'buy', code: 'A', qty: 10, price: 11000 }, ledger: led, policy: pol, plan, totalValue: 100000000, nowMs: now });
check('계획 구간 상단 초과 체결 → FOMO 경고, 평단 위라 물타기 아님', w2.some((x) => x.id === 'fomo') && !w2.some((x) => x.id === 'avgdown'), ids(w2));
let w3 = E.pfTradeWarnings({ trade: { side: 'buy', code: 'B', qty: 10, price: 100 }, ledger: led, policy: pol, plan: null, totalValue: 100000000, nowMs: now });
check('계획 없는 매수 → noplan 경고', w3.some((x) => x.id === 'noplan'), ids(w3));
let w4 = E.pfTradeWarnings({ trade: { side: 'buy', code: 'A', qty: 1500, price: 10000 }, ledger: led, policy: pol, plan: Object.assign({}, plan, { created_ms: now - 99 * 3600000 }), totalValue: 100000000, nowMs: now });
check('수량·비중 한도 초과 → maxqty, conc 경고, 숙려 시간은 통과', w4.some((x) => x.id === 'maxqty') && w4.some((x) => x.id === 'conc') && !w4.some((x) => x.id === 'cooldown'), ids(w4));
let w5 = E.pfTradeWarnings({ trade: { side: 'sell', code: 'A', qty: 500, price: 10000 }, ledger: led, policy: pol, nowMs: now });
check('보유 초과 매도 → block, 사유 없음 → 경고', w5.some((x) => x.id === 'oversell' && x.level === 'block') && w5.some((x) => x.id === 'reason'));
let w6 = E.pfTradeWarnings({ trade: { side: 'sell', code: 'A', qty: 10, price: 9000, reason: '가격 위험' }, ledger: led, policy: pol, nowMs: now, emotion: '불안' });
check('불안 상태 + 가설 훼손 아닌 매도 → 참고 안내(info)', w6.some((x) => x.id === 'emotion_sell' && x.level === 'info'));
check('입금·출금은 점검 대상 아님', E.pfTradeWarnings({ trade: { side: 'deposit' }, ledger: led }).length === 0);
check('수량이 소수이거나 가격이 0이면 block', E.pfTradeWarnings({ trade: { side: 'buy', code: 'A', qty: 1.5, price: 0 }, ledger: led, policy: pol, nowMs: now }).filter((x) => x.level === 'block').length === 2);

// ⑥ 행동 통계: 표본 부족 처리, 처분효과 지표
const few = E.pfBehavior(L2, [], [], 1000000, now);
check('매도 표본이 10건 미만이면 enough=false(결론 내지 않음)', few.disposition.enough === false && few.disposition.n === 2);
const many = [T('buy', '20240101', 'A', 100, 100)];
for (let k = 0; k < 6; k++) { many.push(T('buy', '20240201', 'B', 10, 100), T('sell', '20240301', 'B', 10, 120)); }            // 이익 6번, 보유 29일
for (let k = 0; k < 6; k++) { many.push(T('buy', '20240301', 'B', 10, 100), T('sell', '20240601', 'B', 10, 80)); }             // 손실 6번, 보유 92일
const lm = E.pfLedger(many), bh = E.pfBehavior(lm, many, [], 1000000, now);
check('처분효과 지표: 손실 평균 보유 ÷ 이익 평균 보유 ≈ 3.2, 표본 충분', bh.disposition.enough && near(bh.disposition.ratio, 92 / 29, 0.01), JSON.stringify([bh.disposition.winHold, bh.disposition.loseHold, bh.disposition.ratio]));
const bt = [Object.assign(T('buy', '20250101', 'A', 1, 1), { plan_id: 1, warn_json: '[]' }), Object.assign(T('buy', '20250102', 'A', 1, 1), { plan_id: null, warn_json: '[{"id":"avgdown"}]' })];
const bc = E.pfBehavior(E.pfLedger(bt), bt, [], 1000, now).compliance;
check('계획 준수율: 매수 2건 중 계획 연결 50%, 경고 동반 50%, 물타기 1건', near(bc.planLinkedPct, 0.5) && near(bc.warnedPct, 0.5) && bc.avgdown === 1);
const jm = E.pfBehavior(L2, [], [{ kind: '사후검토', process_score: 90, outcome_pct: -0.1 }, { kind: '사후검토', process_score: 40, outcome_pct: 0.2 }, { kind: '사후검토', process_score: 80, outcome_pct: 0.1 }], 1000, now).matrix;
check('과정·결과 2×2: 좋은 과정+나쁜 결과 1, 나쁜 과정+좋은 결과 1, 좋은 과정+좋은 결과 1', jm.goodBad === 1 && jm.badGood === 1 && jm.goodGood === 1);


// ⑧ 켈리 참고값(보조)
const mk = (nW, wPct, nL, lPct) => [].concat(Array.from({ length: nW }, () => ({ pct: wPct })), Array.from({ length: nL }, () => ({ pct: lPct })));
let K = E.pfKelly(mk(10, 0.1, 10, -0.1));
check('켈리: 매도 30건 미만이면 계산하지 않고 사유 표시', K.enough === false && /표본 부족/.test(K.reason) && K.f === undefined);
K = E.pfKelly(mk(28, 0.1, 3, -0.1));
check('켈리: 손실 5건 미만이어도 계산하지 않음', K.enough === false);
K = E.pfKelly(mk(26, 0.1, 24, -0.1));
check('켈리: p=.52,W=L=10% → f=0.4, 1/2=0.2, 1/4=0.1', K.enough && near(K.f, 0.4, 1e-9) && near(K.half, 0.2, 1e-9) && near(K.quarter, 0.1, 1e-9), JSON.stringify([K.f, K.half, K.quarter]));
check('켈리: 승률 ±1 표준오차에서 부호가 바뀌면 "불안정" 표시', K.fragile === true && K.fLow <= 0, K.fLow + '~' + K.fHigh);
K = E.pfKelly(mk(15, 0.1, 25, -0.1));
check('켈리: 기대값이 음수면 비중 0, edge=false', K.enough && K.edge === false && K.half === 0 && K.quarter === 0);
K = E.pfKelly(mk(20, 0.2, 20, -0.1));
check('켈리: 계산값이 100%를 넘으면 100%로 자르고 capped 표시(원값 2.5 보존)', K.capped === true && K.f === 1 && near(K.fRaw, 2.5, 1e-9) && near(K.quarter, 0.625, 1e-9));
K = E.pfKelly(mk(20, 0.1, 18, -0.1).concat(Array.from({ length: 6 }, () => ({ pct: 0 }))));
check('켈리: 본전(0%) 거래는 이익·손실 어느 쪽에도 넣지 않고 표본 수에는 포함', K.enough && K.n === 44 && K.nWin === 20 && K.nLose === 18);

console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과');
process.exit(fail ? 1 : 0);
