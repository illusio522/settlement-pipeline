package com.settlement.serving.service;

import com.settlement.serving.web.MethodDetail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MethodDetailTest {

    @Test
    @DisplayName("method 코드는 마스터 코드 체계대로 이름에 매핑된다")
    void method_code_maps_to_name() {
        // given — 코드 1~4 + 미지 코드

        // when / then
        assertThat(MethodDetail.nameOf((short) 1)).isEqualTo("CARD");
        assertThat(MethodDetail.nameOf((short) 2)).isEqualTo("MONEY");
        assertThat(MethodDetail.nameOf((short) 3)).isEqualTo("POINT");
        assertThat(MethodDetail.nameOf((short) 4)).isEqualTo("COUPON");
        assertThat(MethodDetail.nameOf((short) 99)).isEqualTo("UNKNOWN");
    }
}
