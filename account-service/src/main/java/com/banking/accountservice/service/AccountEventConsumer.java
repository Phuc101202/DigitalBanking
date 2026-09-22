package com.banking.accountservice.service;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AccountEventConsumer {

    private final AccountService accountService;

    AccountEventConsumer(AccountService accountService) {
        this.accountService = accountService;
    }

    /**
     * Consume transaction.completed event from kafka
     * Credits reciever account
     * 
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);

        } catch (Exception e) {
            log.error("Error crediting account: {}", e.getMessage());
        }
    }

}
