package com.banking.frauddetectionservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class FraudCheckResult {
    private boolean fraud;
    private String reason;
}
