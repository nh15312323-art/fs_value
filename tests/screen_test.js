// 사용법: node tests/screen_test.js   (Node 22+ : node:sqlite)
// computeScreenLists의 "실제 SQL"을 메모리 SQLite(가짜 D1)에서 끝까지 실행해, 거래대금 Z-점수·12-1·52주 고가 계산을 검증한다.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const lineOf = (p) => text.split('\n').find((x) => x.startsWith(p));
const code = [
  lineOf('const MARKET_MIN_TRADING_VALUE'), lineOf('const MARKET_MIN_MCAP'),
  between('const WEEKLY_SCREEN_UTC_DOW', 'let screenTablesReady'),
  'async function ensureScreenTables() {} async function ensureMarketQueueColumns() {}',
  'const __cfg = { minTvEok: 15, minMcapEok: 500, minTv: 1.5e9, minMcap: 5e10 }; async function getUniverseCfg() { return __cfg; }',
  between('async function computeScreenLists', 'async function saveScreenLists'),
  'return { computeScreenLists, zFromSqKey, __cfg };',
].join('\n');
const S = new Function(code)();

let fail = 0;
const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };

function mkEnv(nDays) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE market_index_daily (market_date TEXT, idx_name TEXT, close_price REAL, PRIMARY KEY (market_date, idx_name));' +
    'CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, volume REAL, trading_value REAL, market_cap REAL, PRIMARY KEY (market_date, stock_code));' +
    'CREATE TABLE market_universe_status (stock_code TEXT PRIMARY KEY, corp_name TEXT, backfilled INTEGER);');
  const days = []; for (let i = 0; i < nDays; i++) days.push(String(20250101 + i)); // 오름차순 가짜 날짜
  const idx = db.prepare('INSERT INTO market_index_daily VALUES (?,?,?)'), raw = db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?,?,?,?)');
  const uni = db.prepare('INSERT INTO market_universe_status VALUES (?,?,1)');
  days.forEach((d, i) => idx.run(d, '코스피', 2500 + i));
  const names = ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'S10'];
  names.forEach((nm, k) => {
    const sc = String(100000 + k * 10); uni.run(sc, '종목' + nm);
    days.forEach((d, i) => {
      const last = i === nDays - 1;
      let tv = (50 + (i % 7)) * 1e8 * (1 + k * 0.1);               // 평소 50~54억 (종목마다 규모 다름), 값이 조금씩 변함
      if (nm === 'S1' && last) tv = 400e8;                          // 마지막 날 폭증 → Z 최상위
      if (nm === 'S2') tv = 80e8;                                   // 항상 같은 거래대금 → 표준편차 0 → N/A
      if (nm === 'S3' && i > nDays - 13 && i < nDays - 1) return;   // 직전 11일 중 최근 12일(마지막 제외) 저장 안 됨(관측 부족 확인용)
      raw.run(d, sc, 1000 + i + k, 1000, tv, 1000e8);
    });
  });
  return { db, days, DB: { prepare: (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ all: async () => ({ results: st.all(...b) }), first: async () => st.get(...b) || null }), all: async () => ({ results: st.all() }), first: async () => st.get() || null, run: async () => st.run() }; } } };
}

(async () => {
  // 이력 260일: 12-1 / 52주 고가는 "자료 부족" (253 미만 아님 → 경계 확인용으로 아래에서 별도 실행)
  const e1 = mkEnv(150);
  const r1 = await S.computeScreenLists({ DB: e1.DB }, { pcMcap: 0, pcTv: 0, minMcapEok: 500, minTvEok: 15, minPrice: 0 });
  const z10 = r1.lists.z10, z20 = r1.lists.z20;
  check('Z10 목록 1위 = 마지막 날 거래대금 폭증 종목(S1)', z10.length > 0 && z10[0].corp_name === '종목S1', z10[0] && z10[0].corp_name + ' z=' + z10[0].z10);
  const series = (n) => Array.from({ length: n }, (_, q) => 50 + ((149 - 1 - q) % 7)); // 마지막 날(i=149) 직전 n일의 S1 거래대금(억)
  const zOf = (n) => { const v = series(n); const m = v.reduce((a, c) => a + c, 0) / n; const sd = Math.sqrt(v.reduce((a, c) => a + (c - m) ** 2, 0) / n); return (400 - m) / sd; };
  check('Z10 값 = (400−평균)/모표준편차(ddof=0)', Math.abs(z10[0].z10 - zOf(10)) < 1e-6, z10[0].z10 + ' vs ' + zOf(10));
  check('Z20 값도 직전 20일 기준으로 따로 계산되고 Z10과 다름', z20[0].corp_name === '종목S1' && Math.abs(z20[0].z20 - zOf(20)) < 1e-6 && Math.abs(z20[0].z20 - z10[0].z10) > 1e-6, 'z20=' + z20[0].z20 + ' vs ' + zOf(20));
  const all = [...z10, ...z20, ...r1.lists.rw];
  const s2 = all.find((x) => x.corp_name === '종목S2');
  check('거래대금이 매일 같은 종목(표준편차 0)은 Z = N/A(null)', s2 && s2.z10 === null && s2.z20 === null, JSON.stringify(s2 && [s2.z10, s2.z20]));
  const s3 = all.find((x) => x.corp_name === '종목S3');
  check('직전 관측이 모자라면(10일 중 0일 저장) Z10 = N/A, 0으로 채우지 않음', !s3 || s3.z10 === null, JSON.stringify(s3 && s3.z10));
  check('Z 목록에 null Z 종목은 순위에 들어가지 않음', z10.every((x) => x.z10 !== null) && z20.every((x) => x.z20 !== null));
  check('이력 150일 → 12-1·52주 고가는 자료 부족(null)', r1.meta.mom.ready === false && z10[0].m121 === null && z10[0].hi52 === null);
  check('기존 목록(rw 등)은 계속 생성', r1.lists.rw.length > 0 && r1.lists.m16.length > 0);
  check('복원 함수: 부호 유지', Math.abs(S.zFromSqKey(-4) + 2) < 1e-12 && Math.abs(S.zFromSqKey(9) - 3) < 1e-12 && S.zFromSqKey(null) === null);

  // 이력 270일: 12-1·52주 고가 계산됨 (가격은 날마다 +1 상승 → 마지막이 신고가)
  const e2 = mkEnv(270);
  const r2 = await S.computeScreenLists({ DB: e2.DB }, { pcMcap: 0, pcTv: 0, minMcapEok: 500, minTvEok: 15, minPrice: 0 });
  const x = r2.lists.rw.find((q) => q.corp_name === '종목S1');
  check('이력 270일 → mom.ready = true', r2.meta.mom.ready === true, JSON.stringify(r2.meta.mom));
  check('S1 52주 고가 대비 = 1.00(신고가)', x && Math.abs(x.hi52 - 1) < 1e-9, x && String(x.hi52));
  // S1(k=0) 종가 = 1000 + i. c21 = i=248 → 1248, c252 = i=269-252=17 → 1017
  check('S1 12-1 = 1248/1017 − 1', x && Math.abs(x.m121 - (1248 / 1017 - 1)) < 1e-9, x && String(x.m121));
  // 저장 기준(설정)을 따르는지: 하한을 올리면 올려서 적용하고 안내, 지정이 없으면 설정값이 기본
  S.__cfg.minTvEok = 60; S.__cfg.minTv = 6e9;
  const r3 = await S.computeScreenLists({ DB: e2.DB }, { pcMcap: 0, pcTv: 0, minPrice: 0 });
  check('거래대금 하한 설정(60억)을 기본으로 따름 + 평균 거래대금 미달 종목 제외', r3.meta.minTvEok === 60 && r3.meta.passed_floor < r2.meta.passed_floor, r3.meta.passed_floor + ' < ' + r2.meta.passed_floor);
  const r4 = await S.computeScreenLists({ DB: e2.DB }, { pcMcap: 0, pcTv: 0, minPrice: 0, minTvEok: 20 });
  check('설정보다 낮은 값을 직접 지정하면 설정값으로 올리고 안내', r4.meta.minTvEok === 60 && r4.meta.clamped.some((x) => x.indexOf('60억') >= 0));
  check('Top N = 100으로 변경됨(meta.topN)', r2.meta.topN === 100);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
