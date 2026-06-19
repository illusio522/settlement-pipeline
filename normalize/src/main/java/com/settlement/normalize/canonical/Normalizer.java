package com.settlement.normalize.canonical;

import com.settlement.normalize.cdc.ChangeEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 소스 7테이블(ChangeEvent) → 정산 canonical 4테이블(CanonicalRecord) 변환.
 *  ① 수단별 4원장(card/money/point/coupon) → 단일 payment_method + method 코드(1~4)
 *     - 주결제수단(card·money)만 payments_mid 유지, 보조(point·coupon)는 null
 *  ② payment/order_item/cancel → 정산 필요 컬럼만 projection (풍부한 소스컬럼 제거)
 * "수집 경계에서 canonical 검증/정규화"의 실체.
 */
@Component
public class Normalizer {

    public Optional<CanonicalRecord> normalize(ChangeEvent ev) {
        Map<String, Object> r = ev.row();
        return switch (ev.sourceTable()) {
            case "payment"        -> Optional.of(payment(ev, r));
            case "payment_card"   -> Optional.of(paymentMethod(ev, r, 1, true));   // CARD  주
            case "payment_money"  -> Optional.of(paymentMethod(ev, r, 2, true));   // MONEY 주
            case "payment_point"  -> Optional.of(paymentMethod(ev, r, 3, false));  // POINT 보조
            case "payment_coupon" -> Optional.of(paymentMethod(ev, r, 4, false));  // COUPON 보조
            case "order_item"     -> Optional.of(orderItem(ev, r));
            case "cancel"         -> Optional.of(cancel(ev, r));
            default               -> Optional.empty();  // 구독 외 테이블 방어
        };
    }

    private CanonicalRecord payment(ChangeEvent ev, Map<String, Object> r) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("txn_id", r.get("txn_id"));
        d.put("order_no", r.get("order_no"));
        d.put("brand", r.get("brand"));
        d.put("amount", r.get("amount"));
        d.put("type", r.get("type"));
        d.put("orig_txn_id", r.get("orig_txn_id"));
        d.put("event_time", r.get("event_time"));
        return new CanonicalRecord("payment", str(r.get("txn_id")), ev.op(), ev.seq(), d);
    }

    /** 수단별 4원장 공통 변환: method 코드 부여 + 보조수단 MID 제거. */
    private CanonicalRecord paymentMethod(ChangeEvent ev, Map<String, Object> r, int method, boolean hasMid) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("pm_id", r.get("pm_id"));
        d.put("txn_id", r.get("txn_id"));
        d.put("order_no", r.get("order_no"));
        d.put("method", method);                                   // ★ 이름→코드 정규화
        d.put("payments_mid", hasMid ? r.get("payments_mid") : null);
        d.put("amount", r.get("amount"));
        d.put("type", r.get("type"));
        d.put("event_time", r.get("event_time"));
        return new CanonicalRecord("payment_method", str(r.get("pm_id")), ev.op(), ev.seq(), d);
    }

    private CanonicalRecord orderItem(ChangeEvent ev, Map<String, Object> r) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("order_no", r.get("order_no"));
        d.put("line_no", r.get("line_no"));
        d.put("partner_id", r.get("partner_id"));
        d.put("brand", r.get("brand"));
        d.put("item_amount", r.get("item_amount"));
        return new CanonicalRecord("order_item", str(r.get("order_no")) + ":" + str(r.get("line_no")), ev.op(), ev.seq(), d);
    }

    private CanonicalRecord cancel(ChangeEvent ev, Map<String, Object> r) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("cancel_txn_id", r.get("cancel_txn_id"));
        d.put("order_no", r.get("order_no"));
        d.put("line_no", r.get("line_no"));
        return new CanonicalRecord("cancel",
                str(r.get("cancel_txn_id")) + ":" + str(r.get("order_no")) + ":" + str(r.get("line_no")),
                ev.op(), ev.seq(), d);
    }

    private static String str(Object o) {
        return o == null ? "null" : String.valueOf(o);
    }
}
