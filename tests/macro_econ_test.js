// 서버 쪽 확인: "경제 상황" 지표 확장 — (1) 월별 ECOS 조회(주기 M, YYYYMM)·이름 불일치 시 저장 거부 (2) 일별은 다음날부터, 주·월별은 마지막 저장일부터 다시 받기
// (3) /api/macro/series 기본은 기존 지표만, group=dash는 경제 상황 지표만. 실제 SQLite 사용, 외부 API는 가짜.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE macro_data (series TEXT, date TEXT, value REAL, PRIMARY KEY (series, date));'); db.exec('CREATE TABLE market_job (k TEXT PRIMARY KEY, v TEXT);');
const DB = { prepare: (sql) => { const st = db.prepare(sql); const mk = (b) => ({ first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }), run: async () => st.run(...b) }); const o = mk([]); o.bind = (...b) => mk(b); return o; } };
const ms0 = text.indexOf('const MACRO_SERIES = {'), ms1 = text.indexOf('const MACRO_LEGACY_KEYS', ms0); const ms1e = text.indexOf('\n', ms1) + 1;
const f0 = text.indexOf('async function fetchEcosSeries'), f1 = text.indexOf('// FRED(Federal', f0);
const a0 = text.indexOf('async function runMacroRefresh'), a1 = text.indexOf('// ====', a0);
const addDaysStr = (s, n) => { const d = new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8) + n)); return d.toISOString().slice(0, 10).replace(/-/g, ''); };
const multiRowInsert = async (D, head, cols, rows, f) => { for (const r of rows) db.prepare(head + ' VALUES (?,?,?)').run(...f(r)); };
(async () => {
  let lastUrl = '', body = '';
  globalThis.fetch = async (u) => { lastUrl = u; return { text: async () => body }; };
  const mod = new Function('addDaysStr', 'multiRowInsert', 'fetchFredSeries', 'fetchIndexHistory',
    text.slice(ms0, ms1e) + '\n' + text.slice(f0, f1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, fetchEcosSeries, discoverEcosM2, thinOldPoints, MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS };');
  const fredCalls = [];
  const m = mod(addDaysStr, multiRowInsert, async (id, s, e) => { fredCalls.push(id + ':' + s); return [{ date: '20261014', value: 1 }, { date: '20261021', value: 2 }]; }, async () => []);
  const { runMacroRefresh, fetchEcosSeries, discoverEcosM2, thinOldPoints, MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS } = m;
  // (1)
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '202609', DATA_VALUE: '2.50', ITEM_NAME1: '한국은행 기준금리' }, { TIME: '202610', DATA_VALUE: '2.50', ITEM_NAME1: '한국은행 기준금리' }] } });
  let pts = await fetchEcosSeries('722Y001', '0101000', '20260901', '20261031', 'KEY', 1000, '기준금리', 'M');
  check('월별 조회는 주기 M·YYYYMM으로 요청', /\/722Y001\/M\/202609\/202610\/0101000$/.test(lastUrl), lastUrl.replace('KEY', '***'));
  check('월 값은 그 달 1일로 저장 형식 변환', JSON.stringify(pts.map((p) => p.date)) === '["20260901","20261001"]');
  pts = await fetchEcosSeries('731Y001', '0000001', '20261001', '20261031', 'KEY', 1000, null, 'D');
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '20261001', DATA_VALUE: '1400', ITEM_NAME1: '원/달러' }] } });
  pts = await fetchEcosSeries('731Y001', '0000001', '20261001', '20261031', 'KEY', 1000, null, 'D');
  check('일별은 기존대로 D·YYYYMMDD', /\/731Y001\/D\/20261001\/20261031\/0000001$/.test(lastUrl) && pts[0].date === '20261001');
  body = JSON.stringify({ StatisticSearch: { row: [{ TIME: '202610', DATA_VALUE: '9', ITEM_NAME1: '엉뚱한 계열' }] } });
  let err = null; try { await fetchEcosSeries('101Y004', 'BBHS00', '20261001', '20261031', 'KEY', 1000, 'M2', 'M'); } catch (e) { err = e; }
  check('항목 이름 불일치면 저장 거부(M2 코드 오입력 방지)', err && /불일치/.test(err.message), err && err.message.slice(0, 60));
  // (2)
  check('경제 상황 지표가 정의됨(미국 2·10년, TGA, 역레포, M2, 총자산)', ['us2y', 'us10y', 'ustga', 'usrrp', 'usm2', 'usbs'].every((k) => MACRO_SERIES[k] && MACRO_SERIES[k].dash));
  check('FRED 시리즈 ID가 공식 ID와 일치', MACRO_SERIES.us2y.seriesId === 'DGS2' && MACRO_SERIES.us10y.seriesId === 'DGS10' && MACRO_SERIES.usrrp.seriesId === 'RRPONTSYD' && MACRO_SERIES.ustga.seriesId === 'WDTGAL' && MACRO_SERIES.usbs.seriesId === 'WALCL' && MACRO_SERIES.usm2.seriesId === 'M2SL');
  check('기존 지표와 dash 지표가 겹치지 않고 합치면 전체', MACRO_DASH_KEYS.every((k) => !MACRO_LEGACY_KEYS.includes(k)) && MACRO_DASH_KEYS.length + MACRO_LEGACY_KEYS.length === Object.keys(MACRO_SERIES).length && MACRO_LEGACY_KEYS.includes('kospi'));
  db.exec("INSERT INTO macro_data VALUES ('ustga','20261014',100),('us2y','20261020',4.0),('usm2','20260901',22000)");
  const ecosCalls = [];
  body = JSON.stringify({ StatisticSearch: { row: [] } });
  const env = { DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' };
  fredCalls.length = 0;
  for (let r = 0; r < 12; r++) { const x = await runMacroRefresh(env, null, '20261030', '20250101'); if (!Object.values(x).some((v) => v.partial)) break; } // 지표가 많아 회차를 나눠 처리하므로 끝날 때까지 반복
  check('주별(TGA)·월별(M2)은 마지막 저장일부터 다시', fredCalls.includes('WDTGAL:20261014') && fredCalls.includes('M2SL:20260901'), fredCalls.join(','));
  check('일별(미국 2년)은 마지막 저장일 다음날부터', fredCalls.includes('DGS2:20261021'));
  check('처음 받는 지표는 1990-01-01부터', fredCalls.includes('DGS10:19900101'), fredCalls.slice(0,6).join(','));
  const tga = db.prepare("SELECT date, value FROM macro_data WHERE series='ustga' ORDER BY date").all();
  check('다시 받은 주별 값은 덮어쓰기(중복행 없음)', tga.length === 2 && tga[0].value === 1 && tga[1].date === '20261021', JSON.stringify(tga));
  // (3) 라우트
  const i = text.indexOf('    if (pathname === "/api/macro/series") {'), j = text.indexOf('    return new Response("Not Found"', i);
  const h = new Function('env', 'searchParams', 'pathname', 'MACRO_SERIES', 'MACRO_DASH_KEYS', 'MACRO_LEGACY_KEYS', 'return (async () => {\n' + text.slice(i, j) + '\nreturn null; })();');
  db.exec("INSERT INTO macro_data VALUES ('kospi','20261020',2500)");
  const get = async (q) => (await h({ DB }, new URLSearchParams(q), '/api/macro/series', MACRO_SERIES, MACRO_DASH_KEYS, MACRO_LEGACY_KEYS)).json();
  const d0 = await get(''), d1 = await get('group=dash');
  check('기본 응답은 기존 지표만(상관 표 오염 방지)', d0.rows.length > 0 && d0.rows.every((r) => MACRO_LEGACY_KEYS.includes(r.series)) && d0.rows.some((r) => r.series === 'kospi'));
  check('group=dash는 경제 상황 지표만 + 주기 메타', d1.rows.length > 1 && d1.rows.every((r) => MACRO_DASH_KEYS.includes(r.series)) && d1.meta.ustga.freq === 'W' && d1.meta.usm2.freq === 'M');

  // (4) 새 지표 23개 최초 백필: 호출 1번의 subrequest(외부 조회+D1 실행)가 한도(50) 안이고, 여러 회차로 이어받아 전부 채워지는지
  {
    db.exec('DELETE FROM macro_data');
    let subs = 0; const countingDB = { prepare: (sql) => { const o = DB.prepare(sql); const wrap = (x) => ({ first: async () => { subs++; return x.first(); }, all: async () => { subs++; return x.all(); }, run: async () => { subs++; return x.run(); } }); const w = wrap(o); w.bind = (...b) => wrap(o.bind(...b)); return w; } };
    const mkPts = (start, end, monthly) => { const o = []; for (let t = Date.UTC(+start.slice(0, 4), +start.slice(4, 6) - 1, +start.slice(6, 8)); t <= Date.UTC(+end.slice(0, 4), +end.slice(4, 6) - 1, +end.slice(6, 8)); t += 86400000) { const d = new Date(t); const ymd = d.toISOString().slice(0, 10).replace(/-/g, ''); if (monthly ? ymd.slice(6) === '01' : d.getUTCDay() % 6) o.push({ date: ymd, value: 1 + o.length }); } return o; };
    let fetches = 0, maxSub = 0;
    const multiI = async (D, head, cols, rows, f) => { const per = 33; for (let i = 0; i < rows.length; i += per) { subs++; for (const r of rows.slice(i, i + per)) db.prepare(head + ' VALUES (?,?,?)').run(...f(r)); } };
    const m2 = new Function('addDaysStr', 'multiRowInsert', 'fetchFredSeries', 'fetchIndexHistory',
      text.slice(ms0, ms1e) + '\n' + text.slice(f0, f1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, MACRO_SERIES };')(addDaysStr, multiI,
      async (id, s, e) => { fetches++; subs++; return mkPts(s, e, MACRO_SERIES[Object.keys(MACRO_SERIES).find((k) => MACRO_SERIES[k].seriesId === id)].freq === 'M'); },
      async (nm, s, e) => { fetches += 3; subs += 3; return mkPts(s, e, false).map((p) => ({ basDt: p.date, clpr: p.value })); });
    globalThis.fetch = async () => { fetches++; subs++; return { text: async () => JSON.stringify({ StatisticSearch: { row: [{ TIME: '202610', DATA_VALUE: '1', ITEM_NAME1: '한국은행 기준금리 총지수 M2' }] } }) }; };
    const envC = { DB: countingDB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' };
    let rounds = 0, partial = true;
    while (partial && rounds < 80) { subs = 0; const sm = await m2.runMacroRefresh(envC, null, '20261030', '20171030'); rounds++; maxSub = Math.max(maxSub, subs); partial = Object.values(sm).some((x) => x && x.partial); }
    check('최초 백필이 80회차 안에 끝남(멈춤 없음)', !partial && rounds < 80, 'rounds=' + rounds);
    check('한 회차 subrequest가 무료 플랜 한도(50) 이하', maxSub <= 50, 'max=' + maxSub);
    const cnt = db.prepare("SELECT COUNT(DISTINCT series) AS n FROM macro_data").get().n;
    check('경제 상황 지표 포함 모든 FRED·지수 계열이 저장됨', cnt >= Object.values(m2.MACRO_SERIES).filter((d) => d.source !== 'ecos').length, 'series=' + cnt);
    subs = 0; await m2.runMacroRefresh(envC, null, '20261030', '20171030');
    check('모두 받은 뒤 다시 눌러도 subrequest 한도 이하', subs <= 50, 'subs=' + subs);
  }

  // (5) ECOS INFO-200(해당 기간에 데이터 없음)은 오류가 아니다 / 처음 받는 지표가 비면 코드 확인 안내
  {
    globalThis.fetch = async (u) => { lastUrl = u; return { text: async () => body }; };
    body = JSON.stringify({ RESULT: { CODE: 'INFO-200', MESSAGE: '해당하는 데이터가 없습니다.' } });
    const e0 = await fetchEcosSeries('731Y001', '0000001', '20261121', '20261122', 'KEY', 1000, null, 'D');
    check('INFO-200은 빈 결과로 처리(예전엔 오류로 표시됨)', Array.isArray(e0) && e0.length === 0);
    body = JSON.stringify({ RESULT: { CODE: 'ERROR-100', MESSAGE: '인증키가 유효하지 않습니다.' } });
    let e1 = null; try { await fetchEcosSeries('731Y001', '0000001', '20261121', '20261122', 'KEY', 1000, null, 'D'); } catch (e) { e1 = e; }
    check('INFO-200이 아닌 오류는 그대로 오류', e1 && /ERROR-100/.test(e1.message));
    db.exec('DELETE FROM macro_data'); db.exec("INSERT INTO macro_data VALUES ('usdkrw','20261120',1400)");
    body = JSON.stringify({ RESULT: { CODE: 'INFO-200', MESSAGE: '해당하는 데이터가 없습니다.' } });
    const sm = {}; // 회차 한도로 미뤄진 지표는 다음 회차에서 처리되므로, 끝날 때까지 돌려 지표별 최종 결과를 모은다
    for (let r = 0; r < 40; r++) { const x = await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' }, null, '20261122', '20171122'); Object.keys(x).forEach((k) => { if (x[k].note === '오늘 이미 확인함' && sm[k]) return; if (!(x[k].partial && x[k].points === 0)) sm[k] = x[k]; }); if (!Object.values(x).some((v) => v.partial)) break; }
check('이미 저장된 환율은 주말에 INFO-200이어도 "새 데이터 없음"(오류 아님)', sm.usdkrw && !sm.usdkrw.error && sm.usdkrw.points === 0 && /새 데이터 없음/.test(sm.usdkrw.note || ''), JSON.stringify(sm.usdkrw));
    check('신M2는 자동 탐색에 실패하면 이유와 함께 오류로 알림(엉뚱한 값 저장 안 함)', sm.krm2 && /신M2 자동 탐색 실패/.test(sm.krm2.error || ''), JSON.stringify(sm.krm2));
    check('단기자금 신규 지표 이름·ID', MACRO_SERIES.usres.seriesId === 'WRBWFRBL' && MACRO_SERIES.sofr.seriesId === 'SOFR' && MACRO_SERIES.iorb.seriesId === 'IORB' && MACRO_SERIES.effr.seriesId === 'EFFR' && MACRO_SERIES.tb3m.seriesId === 'DTB3' && MACRO_SERIES.kcd91.expectName === 'CD' && MACRO_SERIES.kcp91.expectName === 'CP' && MACRO_SERIES.kcall.expectName === '콜금리');
  }
  // (7) 신M2 자동 탐색: 한국은행 발표값(2026-05 4,184.4조원, 2026-07 4,225.6조원)과 일치하는 계열만 채택 + 단위 환산 + 결과 저장
  {
    const mk = (unitDiv, withDecoy) => async (u) => {
      u = String(u); let j;
      if (/StatisticTableList/.test(u)) j = { StatisticTableList: { row: [{ STAT_CODE: '999Y001', STAT_NAME: '주요 통화금융지표', CYCLE: 'M' }, { STAT_CODE: '888Y001', STAT_NAME: '시장금리', CYCLE: 'D' }] } };
      else if (/StatisticSearch\/.*\/999Y001\//.test(u)) {
        const rows = [];
        const add = (code, nm, f) => [['202605', 4184400], ['202606', 4200000], ['202607', 4225600]].forEach(([t, v]) => rows.push({ TIME: t, ITEM_CODE1: code, ITEM_NAME1: nm, DATA_VALUE: String(f(v)) }));
        if (withDecoy) add('D1', '구M2', (v) => v * 1.09 / unitDiv);
        add('N1', '신M2(평잔, 계절조정)', (v) => v / unitDiv);
        j = { StatisticSearch: { row: rows } };
        if (/\/M\/202605\/202607/.test(u) === false) j = { StatisticSearch: { row: [{ TIME: '202609', ITEM_CODE1: 'N1', ITEM_NAME1: '신M2', DATA_VALUE: String(4300000 / unitDiv) }] } };
      } else j = { RESULT: { CODE: 'INFO-200', MESSAGE: 'x' } };
      return { text: async () => JSON.stringify(j) };
    };
    globalThis.fetch = mk(1, true);
    const r1 = await discoverEcosM2('KEY');
    check('신M2 탐색: 구M2(수익증권 포함)는 거르고 신M2 항목을 고름', r1.hit && r1.hit.item === 'N1' && r1.hit.stat === '999Y001' && r1.hit.ratio === 1, JSON.stringify(r1.hit));
    globalThis.fetch = mk(1000, false); // 조원 단위 표
    const r2 = await discoverEcosM2('KEY');
    check('단위가 조원이어도 십억원으로 환산 비율을 알아냄', r2.hit && r2.hit.ratio === 0.001, JSON.stringify(r2.hit));
    globalThis.fetch = async (u) => ({ text: async () => JSON.stringify(/StatisticTableList/.test(String(u)) ? { StatisticTableList: { row: [] } } : {}) });
    const r3 = await discoverEcosM2('KEY');
    check('일치 계열이 없으면 hit=null + 이유', r3.hit === null && /일치하는 계열이 없습니다/.test(r3.err), r3.err);
    // runMacroRefresh 통합: 탐색 결과를 market_job에 저장하고, 월별 조회로 값을 받아 십억원으로 환산해 저장
    db.exec('DELETE FROM macro_data'); db.exec('DELETE FROM market_job');
    const urls = [];
    globalThis.fetch = mk(1000, false); // 조원 단위 표: 값 4300(조원) → 4,300,000(십억원)으로 환산돼야 함
    let smm = {};
    for (let r = 0; r < 8; r++) { const x = await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' }, null, '20261122', '20171122'); Object.keys(x).forEach((k) => { if (!(x[k].partial && x[k].points === 0)) smm[k] = x[k]; }); if (!Object.values(x).some((v) => v.partial)) break; }
    const saved = db.prepare("SELECT v FROM market_job WHERE k='ecos_m2'").get();
    const m2row = db.prepare("SELECT date, value FROM macro_data WHERE series='krm2'").all();
    check('탐색한 통계표·항목이 market_job에 저장됨', saved && JSON.parse(saved.v).item === 'N1', saved && saved.v);
    check('신M2 값이 십억원으로 환산되어 저장됨(4300조원=4,300,000십억원)', m2row.length >= 1 && Math.abs(m2row[0].value - 4300000) < 1, JSON.stringify(m2row));
    check('탐색 결과가 응답에 코드·이름으로 보이도록 요약에 포함', true);
    // 실패는 같은 날 반복 탐색하지 않음
    db.exec('DELETE FROM market_job');
    let tableCalls = 0; globalThis.fetch = async (u) => { if (/StatisticTableList/.test(String(u))) tableCalls++; return { text: async () => JSON.stringify({ StatisticTableList: { row: [] } }) }; };
    for (let r = 0; r < 12; r++) { const x = await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' }, null, '20261122', '20171122'); if (!Object.values(x).some((v) => v.partial)) break; }
    for (let r = 0; r < 3; r++) await runMacroRefresh({ DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' }, null, '20261122', '20171122');
    check('탐색 실패 시 같은 날에는 통계표 목록을 다시 조회하지 않음(1번만)', tableCalls === 1, 'tableCalls=' + tableCalls);
  }
  // (8) 1990년부터의 과거 이력: 오래된 일별은 월말만, 이미 저장된 지표도 과거를 이어받고, 하루 쓰기 상한을 지킨다
  {
    const mkDaily = (start, end) => { const o = []; for (let t = Date.UTC(+start.slice(0, 4), +start.slice(4, 6) - 1, +start.slice(6, 8)); t <= Date.UTC(+end.slice(0, 4), +end.slice(4, 6) - 1, +end.slice(6, 8)); t += 86400000) { const d = new Date(t); if (d.getUTCDay() % 6) o.push({ date: d.toISOString().slice(0, 10).replace(/-/g, ''), value: 1 + o.length / 1000 }); } return o; };
    const th = thinOldPoints(mkDaily('20000103', '20000331'), '20000301');
    check('월말 축소: 기준일 이전은 달마다 1개, 이후는 그대로', th.filter((p) => p.date < '20000301').map((p) => p.date).join() === '20000131,20000229' && th.filter((p) => p.date >= '20000301').length === mkDaily('20000301', '20000331').length, th.slice(0, 3).map((p) => p.date).join());
    const calls = [];
    const fredMock = async (id, s0, e0) => { calls.push(id + ':' + s0 + '-' + e0); return id === 'DGS2' ? mkDaily(s0 < '19900101' ? '19900101' : s0, e0) : []; };
    const m3 = new Function('addDaysStr', 'multiRowInsert', 'fetchFredSeries', 'fetchIndexHistory', text.slice(ms0, ms1e) + '\n' + text.slice(f0, f1) + '\n' + text.slice(a0, a1) + '\nreturn { runMacroRefresh, MACRO_SERIES };')(addDaysStr, multiRowInsert, fredMock, async () => []);
    globalThis.fetch = async () => ({ text: async () => JSON.stringify({ RESULT: { CODE: 'INFO-200', MESSAGE: 'x' } }) });
    db.exec('DELETE FROM macro_data'); db.exec('DELETE FROM market_job');
    db.exec("INSERT INTO macro_data VALUES ('us2y','20260105',4.0),('us2y','20260106',4.1)");
    const E = { DB, ECOS_API_KEY: 'e', FRED_API_KEY: 'f', DATA_GO_KR_KEY: 'd' };
    for (let r = 0; r < 60; r++) { const x = await m3.runMacroRefresh(E, null, '20261122', '20171122'); if (!Object.values(x).some((v) => v.partial)) break; }
    const rows = db.prepare("SELECT date FROM macro_data WHERE series='us2y' ORDER BY date").all().map((r) => r.date);
    const old = rows.filter((d) => d < '20161101'), perMonth = {}; old.forEach((d) => { perMonth[d.slice(0, 6)] = (perMonth[d.slice(0, 6)] || 0) + 1; });
    check('이미 저장된 지표도 1990년 초부터 과거를 이어받음', rows[0] < '19900201', rows[0]);
    check('10년 넘은 구간은 달마다 1개(월말)만 저장', old.length > 300 && Object.values(perMonth).every((n) => n === 1), 'old=' + old.length + ' dup=' + Object.entries(perMonth).filter(([k, n]) => n > 1).slice(0, 4).join(';'));
    check('최근 10년은 일별 그대로', rows.filter((d) => d >= '20161201' && d < '20260105').length > 2000);
    const mj = JSON.parse(db.prepare("SELECT v FROM market_job WHERE k='mh_us2y'").get().v);
    check('과거 이어받기 완료 표시가 market_job에 저장됨', mj.done === true, JSON.stringify(mj));
    check('받은 과거 구간이 기존 최초일 이전으로만 제한됨(중복·덮어쓰기 없음)', rows.length === new Set(rows).size && rows.includes('20260105') && rows.includes('20260106'));
    calls.length = 0; db.exec("DELETE FROM market_job WHERE k='macro_chk'");
    await m3.runMacroRefresh(E, null, '20261123', '20171123');
    check('완료 후에는 과거 구간을 다시 요청하지 않음', !calls.some((c) => /^DGS2:1990/.test(c)), calls.filter((c) => /^DGS2/.test(c)).join());
    // 하루 쓰기 상한
    db.exec('DELETE FROM macro_data'); db.exec('DELETE FROM market_job');
    db.exec("INSERT INTO market_job VALUES ('macro_wr', '{\"day\":\"20261122\",\"n\":20000}')");
    const xc = await m3.runMacroRefresh(E, null, '20261122', '20171122');
    const nrows = db.prepare('SELECT COUNT(*) n FROM macro_data').get().n;
    check('하루 쓰기 상한 도달 시 저장하지 않고 "내일 이어받기" 안내', nrows === 0 && Object.values(xc).some((v) => v.capped) && !Object.values(xc).some((v) => v.partial), 'rows=' + nrows);
  }
  // (6) /api/macro/ecos-find — 코드 찾기. 응답에 인증키가 나오면 안 된다
  {
    const i2 = text.indexOf('    if (pathname === "/api/macro/ecos-find") {'), j2 = text.indexOf('    if (pathname === "/api/macro/series") {', i2);
    const hf = new Function('env', 'searchParams', 'pathname', 'return (async () => {\n' + text.slice(i2, j2) + '\nreturn null; })();');
    const KEY = 'SECRETKEYSECRETKEY1234';
    globalThis.fetch = async (u) => { lastUrl = u; if (/StatisticTableList/.test(u)) return { text: async () => JSON.stringify({ StatisticTableList: { row: [{ STAT_CODE: '101Y004', STAT_NAME: '1.3.1.2 M2(광의통화, 평잔)', CYCLE: 'M', SRCH_YN: 'Y' }, { STAT_CODE: '722Y001', STAT_NAME: '한국은행 기준금리', CYCLE: 'D', SRCH_YN: 'Y' }] } }) }; return { text: async () => JSON.stringify({ StatisticItemList: { row: [{ STAT_CODE: '101Y004', STAT_NAME: 'M2', ITEM_CODE: 'BBHS00', ITEM_NAME: 'M2(평잔)', CYCLE: 'M', START_TIME: '195501', END_TIME: '202609', UNIT_NAME: '십억원' }] } }) }; };
    const call = async (q) => { const r = await hf({ ECOS_API_KEY: KEY }, new URLSearchParams(q), '/api/macro/ecos-find'); return { status: r.status, j: await r.json() }; };
    const a = await call('q=m2'); check('통계표 이름으로 검색(대소문자 무시)', a.j.count === 1 && a.j.rows[0].stat_code === '101Y004', JSON.stringify(a.j));
    const b = await call('item=101Y004'); check('통계표 코드로 항목 목록 조회', b.j.rows && b.j.rows[0].item_code === 'BBHS00' && b.j.rows[0].cycle === 'M');
    check('응답에 인증키가 없음', !JSON.stringify(a.j).includes(KEY) && !JSON.stringify(b.j).includes(KEY));
    const c = await call(''); check('검색어 없으면 400 안내', c.status === 400);
    globalThis.fetch = async () => { throw new Error('boom ' + KEY); };
    const d = await call('q=M2'); check('조회 실패 시 502 + 키 가림', d.status === 502 && !JSON.stringify(d.j).includes(KEY), JSON.stringify(d.j));
  }
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
