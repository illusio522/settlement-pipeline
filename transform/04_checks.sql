-- 1단계 v3 검증 (결제수단별 정산 명세 + 차등 수수료)
.print ''
.print '════════ 1단계 v3 (결제수단별 명세 + 차등수수료) ════════'
.print ''
.print '── 1. 규모 ──'
SELECT (SELECT COUNT(*) FROM payment) payments,(SELECT COUNT(*) FROM payment_method) methods,
       (SELECT COUNT(*) FROM settlement_event) events,(SELECT COUNT(*) FROM settlement_agg) agg;

.print ''
.print '── 2. 결제수단별 집계 (gross/fee/payout) — 차등수수료 효과 ──'
SELECT mt.method_name, mt.category,
       SUM(sa.gross) gross, SUM(sa.fee) fee,
       ROUND(100.0*SUM(sa.fee)/NULLIF(SUM(sa.gross),0),3) AS eff_fee_pct, SUM(sa.payout) payout
FROM settlement_agg sa JOIN method_type mt ON mt.method_code=sa.method
GROUP BY 1,2 ORDER BY mt.method_name;

.print ''
.print '── 3. ★ 결제수단별 백데이터 명세 (partner1, 한 날짜) ──'
SELECT sa.settlement_date, mt.method_name, sa.gross, sa.cancel, sa.fee, sa.payout
FROM settlement_agg sa JOIN method_type mt ON mt.method_code=sa.method
WHERE sa.partner_id=1 AND sa.settlement_date=(SELECT MIN(settlement_date) FROM settlement_agg WHERE partner_id=1)
ORDER BY mt.method_name;

.print ''
.print '── 4. 차등수수료 as-of: 결제수단별 월별 적용 fee_rate (카드 5월 0.025/6월 0.023) ──'
SELECT mt.method_name, date_trunc('month',se.settlement_date) m, se.fee_rate, COUNT(*) events
FROM settlement_event se JOIN method_type mt ON mt.method_code=se.method
WHERE se.type=1 GROUP BY 1,2,3 ORDER BY 1,2;

.print ''
.print '── 5. [돈 보존] 파트너 정산: 주문총액=집계 (diff=0) ──'
SELECT CASE status WHEN 0 THEN 'MATCHED' ELSE 'UNMATCHED' END status, COUNT(*) cnt, SUM(diff) total_diff FROM recon_result GROUP BY status;

.print ''
.print '── 6. [대사] 주결제수단 MID별=PG (diff=0) ──'
SELECT CASE status WHEN 0 THEN 'MATCHED' ELSE 'UNMATCHED' END status, COUNT(*) mids, SUM(diff) total_diff FROM recon_card GROUP BY status;

.print ''
.print '── 7. 부분취소 예시 (PAY 전라인 vs CANCEL 일부) ──'
SELECT order_no, type, list(DISTINCT line_no ORDER BY line_no) lines FROM settlement_event
WHERE order_no=(SELECT c.order_no FROM cancel c JOIN order_item oi ON oi.order_no=c.order_no
  GROUP BY c.order_no HAVING COUNT(DISTINCT c.line_no)<COUNT(DISTINCT oi.line_no) LIMIT 1)
GROUP BY order_no,type ORDER BY type;
.print ''
