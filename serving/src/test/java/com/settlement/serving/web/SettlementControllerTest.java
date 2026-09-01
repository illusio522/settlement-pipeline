package com.settlement.serving.web;

import com.settlement.serving.config.ApiKeyFilter;
import com.settlement.serving.service.SettlementQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 컨트롤러 + API 키 필터 단위 테스트. 픽스처는 전부 합성 값(파트너 1, 금액 10,000 등).
 */
@WebMvcTest(SettlementController.class)
@Import(ApiKeyFilter.class)
@TestPropertySource(properties = "app.api-key=test-key-synthetic")
class SettlementControllerTest {

    private static final String KEY = "test-key-synthetic";   // 합성 테스트 키

    @Autowired
    private MockMvc mvc;

    @MockBean
    private SettlementQueryService service;

    // ---------- 인증 ----------

    @Test
    @DisplayName("API 키가 없으면 401을 반환한다")
    void missing_api_key_is_unauthorized() throws Exception {
        // given — 키 헤더 없는 요청

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("API 키가 틀리면 401을 반환한다")
    void wrong_api_key_is_unauthorized() throws Exception {
        // given — 잘못된 키

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15").header("X-API-Key", "wrong"))
                .andExpect(status().isUnauthorized());
    }

    // ---------- 요약 조회 ----------

    @Test
    @DisplayName("올바른 키와 날짜면 일자별 파트너 요약을 반환한다")
    void daily_summary_returns_page() throws Exception {
        // given
        given(service.dailySummary(eq(LocalDate.parse("2026-05-15")), any()))
                .willReturn(new PageImpl<>(List.of(
                        new PartnerDailySummary(1, 10_000L, 0L, 10_000L, 250L, 9_750L, 3L))));

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15").header("X-API-Key", KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].partnerId").value(1))
                .andExpect(jsonPath("$.content[0].payout").value(9_750));
    }

    @Test
    @DisplayName("날짜 형식이 잘못되면 400을 반환한다")
    void invalid_date_is_bad_request() throws Exception {
        // given — ISO 형식이 아닌 날짜

        // when / then
        mvc.perform(get("/api/v1/settlements/not-a-date").header("X-API-Key", KEY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("invalid request parameter"));
    }

    // ---------- 파트너 상세 ----------

    @Test
    @DisplayName("파트너 상세는 결제수단별 행과 수단 이름을 반환한다")
    void partner_detail_returns_method_rows() throws Exception {
        // given
        given(service.partnerDetail(LocalDate.parse("2026-05-15"), 1))
                .willReturn(List.of(new MethodDetail(
                        (short) 1, (short) 1, "CARD", 10_000L, 0L, 10_000L, 250L, 9_750L, 3L)));

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15/partners/1").header("X-API-Key", KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].methodName").value("CARD"))
                .andExpect(jsonPath("$[0].fee").value(250));
    }

    @Test
    @DisplayName("데이터 없는 파트너 상세는 404를 반환한다")
    void empty_partner_detail_is_not_found() throws Exception {
        // given
        given(service.partnerDetail(LocalDate.parse("2026-05-15"), 999))
                .willReturn(List.of());

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15/partners/999").header("X-API-Key", KEY))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("partnerId가 음수면 400을 반환한다")
    void negative_partner_id_is_bad_request() throws Exception {
        // given — @Positive 위반

        // when / then
        mvc.perform(get("/api/v1/settlements/2026-05-15/partners/-1").header("X-API-Key", KEY))
                .andExpect(status().isBadRequest());
    }
}
