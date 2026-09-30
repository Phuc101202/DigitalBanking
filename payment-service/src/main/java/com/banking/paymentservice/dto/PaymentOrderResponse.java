package com.banking.paymentservice.dto;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentOrderResponse {
    private String paymentId;
    private String orderId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String keyId;
}
