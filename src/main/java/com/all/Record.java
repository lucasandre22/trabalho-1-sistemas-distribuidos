package com.all;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor @Getter
public enum Record {
    HOUSE_AD("Casa em Curitiba no bairro santa candida com 100 metros quadrados"),
    APPARTMENT_AD("Apartamento em Pinhais"),
    TIGUAN_AD("Tiguan 2014 com 104 mil km"),
    VOYAGE_AD("Voyage impecavel 2012 com 12 mil km"),
    YATCH_AD("Iate preto"),
    BOAT_AD("Barco em matinhos");

    private String ad;
}
