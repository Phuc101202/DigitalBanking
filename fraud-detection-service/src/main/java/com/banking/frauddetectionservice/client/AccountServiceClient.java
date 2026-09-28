package com.banking.frauddetectionservice.client;

import java.math.BigDecimal;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @GetMapping("/api/v1/accounts/{accountNumber}/balance")
    BigDecimal getBalance(@PathVariable String accountNumber);
}
