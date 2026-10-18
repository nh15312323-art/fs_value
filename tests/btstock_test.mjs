// 종목별 백테스트 값 보기 순수 함수 테스트: node tests/btstock_test.mjs
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_btstk_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const i = HTML_PAGE.indexOf('// ===== 종목별 백테스트 값 보기 ====='), j = HTML_PAGE.indexOf('// ===== /종목별 백테스트 값 보기 =====');
if (i < 0 || j < 0) throw new Error('구간 없음');
const pre = "const anFin = (v) => v != null && typeof v === 'number' && isFinite(v); const anEsc = (s) => String(s);" +
  "const isUsable = (r) => !r.error && r.fs_div != null;" +
  "const anBuildSeries = (rows) => rows.filter((r) => r.reprt_code === '11011').map((r) => ({ year: Number(r.bsns_year), revenue: r.revenue, ebit: r.operating_income, ni: r.net_income, fcf: r.fcf, eps: r.net_income / 1e7, roic: 0.1, roe: 0.12 })).sort((a, b) => a.year - b.year);" +
  "const BT_FACTORS = { fscore: { name: 'F', ev: 'x', fn: () => 7 }, ep: { name: 'EP', ev: 'x', fn: (L, d, P) => P > 0 ? d.eps / P : null }, graham: { name: 'G', ev: 'x', fn: () => -12.5 }, bp: { name: 'BP', ev: 'x', fn: () => 0.8 } };";
const S = new Function(pre + HTML_PAGE.slice(i, j) + '\nreturn { btStockTable, btFacTxt, btFmtDate, btStkCsv, btStockHtml };')();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const row = (y, over) => Object.assign({ bsns_year: String(y), reprt_code: '11011', fs_div: 'CFS', revenue: 1000e8, operating_income: 100e8, net_income: 80e8, fcf: 50e8, filing_date: y + 1 + '0320', price_at_period_end: 9000, price_next_day: 10000 + (y - 2020) * 1000, price_next_day_date: y + 1 + '0321' }, over || {});
const rows = [2020, 2021, 2022, 2023].map((y) => row(y)).concat([row(2021, { reprt_code: '11013' })]);
let t = S.btStockTable({ rows });
check('연도별 한 줄, 최신 연도가 위', t.length === 4 && t[0].year === 2023 && t[3].year === 2020);
check('수익률 = 다음 해 매도가/이번 해 매수가 − 1 (2020: 11000/10000−1)', Math.abs(t[3].f - 0.1) < 1e-12 && t[3].P0 === 10000 && t[3].P1 === 11000 && t[3].status === 'ok');
check('마지막 해는 다음 해 공시 전이라 수익률 없음 + 보유 중', t[0].f === null && t[0].status === 'open' && t[0].P0 === 13000);
check('분기보고서 행은 무시(연도 중복 없음)', t.filter((r) => r.year === 2021).length === 1);
check('가격 필요한 팩터는 매수가로 계산(EP = EPS/매수가)', Math.abs(t[3].fac.ep - (80e8 / 1e7) / 10000) < 1e-12, String(t[3].fac.ep));
check('재무 값(매출·영업이익·순이익·FCF·EPS) 포함', t[3].fin.revenue === 1000e8 && t[3].fin.ebit === 100e8 && t[3].fin.fcf === 50e8 && t[3].fin.eps === 800);
const t2 = S.btStockTable({ rows: [row(2020, { price_next_day: null }), row(2021)] });
check('매수가 없으면 수익률 N/A + 사유(noP0), 가격 필요한 팩터는 N/A, 가격 무관 팩터는 계산', t2[1].f === null && t2[1].status === 'noP0' && t2[1].fac.ep === null && t2[1].fac.fscore === 7);
const t3 = S.btStockTable({ rows: [row(2020), row(2021, { price_next_day: 0 })] });
check('매도가 없으면 noP1', t3[1].status === 'noP1' && t3[1].f === null);
const t4 = S.btStockTable({ rows: [row(2020, { filing_date: '20210501' }), row(2021, { filing_date: '20210401' })] });
check('다음 해 공시일이 더 이르면(순서 이상) 수익률을 만들지 않음', t4[1].status === 'order' && t4[1].f === null);
check('오류 행은 제외', S.btStockTable({ rows: [row(2020, { error: 'x' })] }).length === 0 && S.btStockTable({ rows: [] }).length === 0);
check('표시 형식: F-Score 7/9, Graham은 양수 PER×PBR, BP 배', S.btFacTxt('fscore', 7) === '7/9' && S.btFacTxt('graham', -12.5) === '12.5' && S.btFacTxt('bp', 0.8) === '0.80배' && S.btFacTxt('ep', 0.05) === '5.0%' && S.btFacTxt('ep', null) === 'N/A');
check('날짜 표시 YYYYMMDD → YYYY-MM-DD', S.btFmtDate('20210320') === '2021-03-20' && S.btFmtDate(null) === 'N/A');
const csv = S.btStkCsv([{ name: '가,나 "다"', code: 'X1', table: t }]);
const lines = csv.replace(/^﻿/, '').split(String.fromCharCode(10));
check('CSV: 머리글+4행, BOM, 쉼표·따옴표가 있는 이름은 따옴표로 감쌈', csv.charCodeAt(0) === 0xFEFF && lines.length === 5 && lines[1].startsWith('"가,나 ""다""",X1,2023') && lines[0].startsWith('종목,종목코드,결산연도'), lines[1].slice(0, 60));
check('CSV: 수익률은 숫자 그대로(0.1), 없는 값은 빈칸', lines[4].split(',').includes('0.1') && /,,/.test(lines[1]));
const html = S.btStockHtml('테스트', t);
check('표 HTML: 12개월 수익률·매수가 머리글, 수익률 있는 해 3개 요약, 사용 표시', html.includes('12개월') && html.includes('수익률이 계산된 해 3개') && html.includes('✓ 수익률 사용') && html.includes('보유 중'));
console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
