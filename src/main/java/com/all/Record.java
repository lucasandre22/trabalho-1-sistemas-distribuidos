package com.all;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor @Getter
public enum Record {
    BOMBA_20L("20L, $100"),
    BOMBA_30L("30L, $150"),
    TANQUE_1500L("1500L"),
    TANQUE_500L("500L");

    private final String recordName;
}
