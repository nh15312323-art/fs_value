// 재료(공시) 점검 순수 함수 + 공시목록 수집 테스트: node tests/material_test.js
const fs = require('fs'), path = require('path');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const cut = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const server = cut('// ===== 공시목록(재료 점검) 수집 =====', '// ===== /공시목록(재료 점검) 수집 =====');
const client = cut('// ===== 재료(공시) 점검', '// ===== /재료(공시) 점검 =====');
const pre = "const anEsc = (s) => String(s); function sigPx() { return null; } function sigRefresh() {} const document = { getElementById: () => null };";
const M = new Function(pre + client + '\nreturn { matClass, matPrep, matNewGone, matMonthly, matReact, matBigMoves, matIdxMap, matAge, matSummary, MAT_ST, matTokens, matTerms, matStaleShare, matNewsMonthly, matSafeUrl };')();
const newsSrc = cut('// ===== 뉴스(재료 점검) 수집 =====', '// ===== /뉴스(재료 점검) 수집 =====');
const SV = new Function(server + newsSrc + '\nreturn { collectDisclosures, collectNews, newsPubToYmd, newsClean };')();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const near = (a, b, t) => Math.abs(a - b) <= (t || 1e-9);

(async () => {
  // 1) 분류
  const K = (nm) => M.matClass(nm).cat;
  check('단일판매·공급계약 → order, 기재정정은 corr', K('단일판매ㆍ공급계약체결') === 'order' && M.matClass('[기재정정]단일판매ㆍ공급계약체결').corr === true && K('[기재정정]단일판매ㆍ공급계약체결') === 'order');
  check('유상증자·전환사채 → fund', K('유상증자결정') === 'fund' && K('전환사채권발행결정') === 'fund');
  check('배당·자기주식 → ret', K('현금ㆍ현물배당결정') === 'ret' && K('자기주식취득결정') === 'ret');
  check('영업(잠정)실적 → earn', K('연결재무제표기준영업(잠정)실적(공정공시)') === 'earn');
  check('분기보고서·사업보고서 → periodic', K('분기보고서 (2025.09)') === 'periodic' && K('사업보고서 (2024.12)') === 'periodic');
  check('임원·주요주주 소유상황 → insider', K('임원ㆍ주요주주특정증권등소유상황보고서') === 'insider');
  check('불성실공시·횡령배임·조회공시 → risk', K('불성실공시법인지정예고') === 'risk' && K('횡령ㆍ배임혐의발생') === 'risk' && K('풍문또는보도에대한해명(미확정)') !== 'risk' && K('조회공시요구(현저한시황변동)') === 'risk');
  check('타법인주식 취득·합병 → mna', K('타법인주식및출자증권취득결정') === 'mna' && K('회사합병결정') === 'mna');
  check('규칙에 없으면 other', K('주식매수선택권부여에관한신고') === 'other');
  check('날짜 형식이 아닌 항목은 버림', M.matPrep([{ d: '2025-10-01', nm: '유상증자결정' }, { d: '20251001', nm: '유상증자결정', no: '1' }]).length === 1);

  // 2) 새로 생김 / 사라짐 (기준일 20251010)
  const asof = '20251010';
  const dAgo = (n) => { const t = new Date(Date.UTC(2025, 9, 10) - n * 86400000); return t.getUTCFullYear() + String(t.getUTCMonth() + 1).padStart(2, '0') + String(t.getUTCDate()).padStart(2, '0'); };
  const ev = M.matPrep([
    { d: dAgo(10), nm: '영업(잠정)실적(공정공시)', no: 'a' }, { d: dAgo(200), nm: '영업(잠정)실적(공정공시)', no: 'b' },
    { d: dAgo(30), nm: '단일판매ㆍ공급계약체결', no: 'c' },
    { d: dAgo(200), nm: '유상증자결정', no: 'd' },
    { d: dAgo(20), nm: '분기보고서 (2025.06)', no: 'e' },            // 정기보고서는 제외
    { d: dAgo(15), nm: '[기재정정]자기주식취득결정', no: 'f' },        // 정정은 제외
    { d: dAgo(400), nm: '현금ㆍ현물배당결정', no: 'g' },              // 1년 밖
  ]);
  const ng = M.matNewGone(ev, asof), by = (k) => ng.find((x) => x.k === k);
  check('최근 3개월에만 있는 유형 = new (수주)', by('order').st === 'new' && by('order').rec === 1);
  check('이전에만 있던 유형 = gone (자금조달)', by('fund').st === 'gone' && by('fund').pri === 1);
  check('양쪽에 있으면 both (실적)', by('earn').st === 'both');
  check('정정·정기보고서·1년 밖은 세지 않음', by('ret').st === 'none' && ng.every((x) => x.k !== 'periodic'));

  // 3) 월별
  const mo = M.matMonthly(ev, asof);
  check('월 12칸, 마지막은 기준일의 달', mo.months.length === 12 && mo.months[11] === '202510' && mo.months[0] === '202411');
  check('월별 합 = 세어진 건수(실적2+수주1+증자1=4)', Object.keys(mo.cnt).reduce((a, k) => a + mo.cnt[k].reduce((x, y) => x + y, 0), 0) === 4);

  // 4) 반응 (가짜 가격: 매일 +1%, 코스피 불변)
  const days = []; for (let k = 0; k < 60; k++) { const t = new Date(Date.UTC(2025, 5, 1) + k * 86400000); days.push(t.getUTCFullYear() + String(t.getUTCMonth() + 1).padStart(2, '0') + String(t.getUTCDate()).padStart(2, '0')); }
  const px = { d: days, c: days.map((_, k) => 100 * Math.pow(1.01, k)) };
  const ixm = M.matIdxMap({ d: days, c: days.map(() => 2500) });
  let r = M.matReact(days[10], px, ixm);
  check('1일 반응 = 공시일 종가/직전 종가 − 1, 시장조정(지수 불변)이면 같음', near(r.r1, 0.01, 1e-9) && near(r.a1, 0.01, 1e-9));
  check('5일 = 직전 종가 대비 5번째 거래일', near(r.r5, Math.pow(1.01, 5) - 1, 1e-9));
  r = M.matReact(days[10], px, M.matIdxMap({ d: days, c: days.map((_, k) => 2500 * Math.pow(1.005, k)) }));
  check('코스피가 0.5%/일 오르면 초과수익 = 주가 − 지수', near(r.a5, (Math.pow(1.01, 5) - 1) - (Math.pow(1.005, 5) - 1), 1e-9));
  r = M.matReact(days[55], px, ixm);
  check('미래 거래일이 부족하면 20일 = null (0으로 채우지 않음)', r.r20 === null && r.a20 === null && r.r1 != null);
  check('첫 거래일 공시(직전 종가 없음) = null', M.matReact(days[0], px, ixm) === null);
  const pj = { d: days, c: days.map((v, k) => 100 * (k >= 30 ? 0.4 : 1)) };   // k=30에서 −60% = 분할 의심
  r = M.matReact(days[29], pj, ixm);
  check('±30% 넘는 변동(분할 의심)이 낀 구간만 계산하지 않음(1일은 계산, 5일은 null)', r.r1 !== null && r.r5 === null && r.r20 === null);
  r = M.matReact(days[10], px, {});
  check('지수 자료가 없으면 시장조정 null, 원수익률은 유지', r.a1 === null && near(r.r1, 0.01, 1e-9));

  // 5) 큰 움직임 + 공시 유무 (asof=days[59])
  const c2 = days.map((_, k) => 100);
  for (let k = 20; k < 60; k++) c2[k] = 100;
  c2[20] = 110; for (let k = 21; k < 60; k++) c2[k] = 110;          // k=20: +10%
  c2[40] = 100; for (let k = 41; k < 60; k++) c2[k] = 100;           // k=40: −9.1%
  c2[50] = 50; for (let k = 51; k < 60; k++) c2[k] = 50;             // k=50: −50% (분할 의심 → 제외)
  const pxb = { d: days, c: c2 };
  const evb = M.matPrep([
    { d: days[20], nm: '단일판매ㆍ공급계약체결', no: '1' },
    { d: days[40], nm: '임원ㆍ주요주주특정증권등소유상황보고서', no: '2' },   // 내부자 보고는 이유로 인정하지 않음
    { d: days[40], nm: '분기보고서 (2025.06)', no: '3' },                     // 정기보고서도 불인정
  ]);
  const bm = M.matBigMoves(pxb, M.matIdxMap({ d: days, c: days.map(() => 2500) }), evb, days[59]);
  check('큰 움직임 2일(−50%는 분할 의심으로 제외 1일)', bm.total === 2 && bm.skipped === 1, JSON.stringify([bm.total, bm.skipped]));
  check('수주 공시가 있는 날 = 공시 있음, 내부자/정기보고서만 있는 날 = 공시 없음', bm.withD === 1 && bm.noD === 1 && bm.list.find((o) => o.d === days[40]).has === false);
  const b20 = bm.list.find((o) => o.d === days[20]), b40 = bm.list.find((o) => o.d === days[40]);
  check('이후 5일 시장조정 수익률(변화 없음)=0, 이후 20일에는 다음 하락(−9.1%)이 반영', near(b20.f5, 0, 1e-9) && near(b20.f20, 100 / 110 - 1, 1e-9));
  check('이후 거래일이 모자라면 null(0으로 채우지 않음)', b40.f20 === null && b40.f5 !== null);
  const bm2 = M.matBigMoves(pxb, M.matIdxMap({ d: days, c: days.map((_, k) => (k === 20 ? 2750 : k > 20 ? 2750 : 2500)) }), evb, days[59]);
  check('코스피도 같은 날 10% 오르면 시장조정 후 큰 움직임이 아님', !bm2.list.some((o) => o.d === days[20]));

  // 6) 공시목록 수집
  const row = (n, d) => ({ rcept_dt: d || '20251001', report_nm: 'r' + n, rcept_no: 'N' + n, flr_nm: 'x' });
  let calls = 0;
  let g = await SV.collectDisclosures(async (p) => { calls++; return { status: '000', total_count: 5, total_page: 3, list: p === 1 ? [row(1), row(2)] : p === 2 ? [row(3), row(4)] : [row(5)] }; });
  check('여러 쪽을 합침(5건·3호출)·partial=false', g.items.length === 5 && calls === 3 && g.partial === false && g.error === null);
  g = await SV.collectDisclosures(async () => ({ status: '013', message: '조회된 데이터가 없습니다.' }));
  check('013 = 데이터 없음(오류 아님)', g.items.length === 0 && g.error === null);
  g = await SV.collectDisclosures(async () => ({ status: '010', message: '등록되지 않은 키' }));
  check('첫 쪽 오류 → 오류 메시지(프록시 점검 안내 포함)', g.error && g.error.indexOf('010') >= 0 && g.error.indexOf('프록시') >= 0);
  g = await SV.collectDisclosures(async () => ({ status: '000', total_count: 30, total_page: 3, list: [row(1), row(2)] }));
  check('프록시가 page_no를 무시해 같은 쪽이 반복되면 중단하고 partial', g.items.length === 2 && g.partial === true);
  g = await SV.collectDisclosures(async (p) => { if (p === 2) throw new Error('timeout'); return { status: '000', total_count: 4, total_page: 2, list: [row(1), row(2)] }; });
  check('2쪽부터 실패하면 받은 것만 쓰고 partial', g.items.length === 2 && g.partial === true && g.error === null);
  g = await SV.collectDisclosures(async () => { throw new Error('net'); });
  check('첫 쪽 예외 → 오류', g.error && g.items.length === 0);

  // 7) 뉴스 수집(네이버 API 응답 모의)
  check('pubDate(+0900) → 한국 날짜', SV.newsPubToYmd('Mon, 13 Oct 2025 07:08:00 +0900') === '20251013' && SV.newsPubToYmd('Sun, 12 Oct 2025 23:30:00 +0000') === '20251013' && SV.newsPubToYmd('garbage') === null);
  check('HTML 태그·엔티티 제거', SV.newsClean('<b>모의전자</b> &quot;수주&quot; &amp; 증설') === '모의전자 "수주" & 증설');
  const mkItem = (i, pub, title, desc) => ({ title: title || '<b>모의전자</b> 뉴스 ' + i, description: desc || '', originallink: 'https://n.example/' + i, link: 'https://l.example/' + i, pubDate: pub });
  const pubOf = (n) => new Date(Date.UTC(2025, 9, 13, 0, 0, 0) - n * 86400000).toUTCString().replace('GMT', '+0000');
  let ncalls = 0;
  let nr = await SV.collectNews(async (st) => { ncalls++; const items = []; for (let k = 0; k < 100; k++) items.push(mkItem(st + k, pubOf((st - 1 + k) * 2))); return { total: 5000, start: st, display: 100, items }; }, '모의전자', '20250101');
  check('기간(cutoff) 이전 기사에 닿으면 중단하고 partial=false', nr.partial === false && ncalls < 10 && nr.items.length > 100 && nr.items.every((i) => i.d >= '20250101'), 'calls ' + ncalls + ' n ' + nr.items.length);
  ncalls = 0;
  nr = await SV.collectNews(async (st) => { ncalls++; const items = []; for (let k = 0; k < 100; k++) items.push(mkItem(st + k, pubOf(Math.floor((st - 1 + k) / 50)))); return { total: 5000, items }; }, '모의전자', '20240101');
  check('10쪽(1,000건)을 다 써도 기간 끝에 못 닿으면 partial=true', ncalls === 10 && nr.partial === true);
  nr = await SV.collectNews(async () => ({ total: 2, items: [mkItem(1, pubOf(1)), mkItem(2, pubOf(2), '무관한 기사', '내용 없음')] }), '모의전자', '20250101');
  check('종목명이 제목·요약에 없는 기사는 버림, 끝까지 가면 partial=false', nr.items.length === 1 && nr.partial === false);
  nr = await SV.collectNews(async () => ({ total: 2, items: [mkItem(1, pubOf(1)), mkItem(1, pubOf(1))] }), '모의전자', '20250101');
  check('같은 링크 중복 제거', nr.items.length === 1);
  nr = await SV.collectNews(async () => ({ errorCode: 'SE01', errorMessage: 'Incorrect query request' }), '모의전자', '20250101');
  check('API 오류 → 오류 반환(키는 노출 안 함)', nr.error && nr.items.length === 0 && nr.error.indexOf('SE01') >= 0);
  nr = await SV.collectNews(async (st) => { if (st > 1) throw new Error('net'); const items = []; for (let k = 0; k < 100; k++) items.push(mkItem(k, pubOf(k))); return { total: 900, items }; }, '모의전자', '20240101');
  check('2쪽부터 실패하면 받은 기사만 쓰고 partial', nr.items.length === 100 && nr.partial === true);

  // 8) 뉴스 분석
  const toks = M.matTokens('모의전자, 美 반도체 장비 공급계약 체결… 주가 급등', '모의전자');
  check('토큰: 종목명·불용어·숫자 제거, 핵심어 유지', toks.indexOf('모의전자') < 0 && toks.indexOf('반도체') >= 0 && toks.indexOf('공급계약') >= 0 && toks.indexOf('급등') < 0);
  const nw = [];
  const addN = (age, t) => nw.push({ d: dAgo(age), t: t, l: 'https://x/' + nw.length });
  for (let k = 0; k < 4; k++) addN(10 + k, '모의전자 로봇 사업 진출 ' + k + '호');            // 최근: 로봇 4회 → 새로 등장
  for (let k = 0; k < 6; k++) addN(200 + k, '모의전자 중국 공장 증설 소식 ' + k);            // 이전: 공장 6회 → 사라짐
  addN(15, '모의전자 공장 가동'); // 최근에도 나오는 단어는 '사라짐'이 아님
  const tm = M.matTerms(nw, '모의전자', asof);
  check('최근 3회 이상 + 이전 0회 단어 = 새로 등장(로봇)', tm.fresh.some((x) => x[0] === '로봇' && x[1] === 4));
  check('이전 5회 이상 + 최근 0회 단어 = 사라짐(중국), 최근에도 있는 단어(공장)는 제외', tm.gone.some((x) => x[0] === '중국') && !tm.gone.some((x) => x[0] === '공장'));
  const dup = [{ d: dAgo(30), t: '모의전자 로봇 사업 진출' }, { d: dAgo(29), t: '모의전자 로봇 사업 진출 확정' }, { d: dAgo(28), t: '모의전자 배당 확대 결정' }];
  const sh = M.matStaleShare(dup, '모의전자', asof);
  check('반복도: 직전 기사와 제목이 겹치면 stale (3건 중 1건)', sh.n === 3 && sh.stale === 1, JSON.stringify(sh));
  const nm2 = M.matNewsMonthly(nw, asof);
  check('월별 기사 수 합 = 12개월 안 기사 수', nm2.cnt.reduce((a, b) => a + b, 0) === nw.length && nm2.months.length === 12);
  check('링크: http(s)만 허용, 따옴표·꺾쇠 이스케이프', M.matSafeUrl('javascript:alert(1)') === '#' && M.matSafeUrl('https://a.com/x"onmouseover="y') .indexOf('"') < 0);

  // 9) 큰 움직임 × 뉴스
  const newsB = [{ d: days[19], t: '모의전자 수주 기대', l: 'https://x/1' }, { d: days[5], t: '오래전 기사', l: 'https://x/2' }];
  const bmN = M.matBigMoves(pxb, M.matIdxMap({ d: days, c: days.map(() => 2500) }), evb, days[59], null, newsB);
  const m20 = bmN.list.find((o) => o.d === days[20]), m40 = bmN.list.find((o) => o.d === days[40]);
  check('큰 움직임 날 직전 거래일~당일에 뉴스가 있으면 건수·제목 표시', m20.nn === 1 && m20.nt.indexOf('수주') >= 0);
  check('공시도 뉴스도 없는 날은 별도 집계(noDnoN)', m40.nn === 0 && bmN.noDnoN === 1 && bmN.noDwithN === 0, JSON.stringify([bmN.noDnoN, bmN.noDwithN]));
  const bmO = M.matBigMoves(pxb, M.matIdxMap({ d: days, c: days.map(() => 2500) }), evb, days[59], null, [{ d: days[30], t: '모의전자', l: 'https://x' }]);
  check('뉴스 수집 구간(가장 오래된 기사) 이전 날짜는 ‘없음’이 아니라 확인 불가(null)', bmO.list.find((o) => o.d === days[20]).nn === null && bmO.outOfRange === 1, String(bmO.outOfRange));
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
