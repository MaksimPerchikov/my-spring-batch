package ru.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import java.math.BigDecimal;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SalesReportItem {

    private Long regionId;
    private Long outletId;
    private BigDecimal smartphones;
    private BigDecimal memoryCards;
    private BigDecimal notebooks;
    private BigDecimal total;

}
