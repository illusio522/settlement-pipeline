package com.settlement.serving.web;

import com.settlement.serving.service.SettlementQueryService;
import jakarta.validation.constraints.Positive;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

/** 정산 조회 API. 인증은 ApiKeyFilter(전 엔드포인트), 입력은 타입·검증 애너테이션으로 강제. */
@RestController
@RequestMapping("/api/v1/settlements")
@Validated
public class SettlementController {

    private final SettlementQueryService service;

    public SettlementController(SettlementQueryService service) {
        this.service = service;
    }

    /** 일자별 파트너 정산 요약 (페이징). 예: GET /api/v1/settlements/2026-05-15?page=0&size=20 */
    @GetMapping("/{date}")
    public Page<PartnerDailySummary> dailySummary(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @PageableDefault(size = 20) Pageable pageable) {
        return service.dailySummary(date, pageable);
    }

    /** 특정 파트너의 일 정산 상세 (결제수단별). 없으면 404. */
    @GetMapping("/{date}/partners/{partnerId}")
    public ResponseEntity<List<MethodDetail>> partnerDetail(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @PathVariable @Positive Integer partnerId) {
        List<MethodDetail> detail = service.partnerDetail(date, partnerId);
        return detail.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(detail);
    }
}
