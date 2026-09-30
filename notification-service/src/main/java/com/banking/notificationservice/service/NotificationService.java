package com.banking.notificationservice.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload) {
        try {
            log.info("OTP Generated Event Received: {}", payload);
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = (String) payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                                    "Reason: %s " +
                                    "A transaction of %s is pending verification. " +
                                    "Your OTP is: %s. Valid for 5 minutes. " +
                                    "If this wasn't you - ignore this message.",
                            reason, transactionId, otp));
        } catch (Exception e) {
            log.error("Error processing OTP Generated event: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    private void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {
        try {

            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(senderAccount,
                    "DEBIT ALERT",
                    String.format("%s debited from account %s", amount, senderAccount));

            sendAlert(receiverAccount,
                    "CREDIT ALERT",
                    String.format("%s credited from account %s", amount, receiverAccount));
        } catch (Exception e) {
            log.error("Error sending transaction notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED", String.format("Your account %s has been blocked. " +
                            "Reason: %s. " +
                            "Please contact your bank immediately.", accountNumber, reason));
        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(senderAccount,
                    "REFUND PROCESSED",
                    String.format("Your transaction of %s was cancelled. " +
                            "Reason: %s " +
                            "%s has been refunded to account %s. ", amount, reason, amount, senderAccount));
        } catch (Exception e) {
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT SUCCESSFUL", String.format(
                            "Payment of %s completed. " +
                                    "Razorpay ID: %s",
                            amount, payload.get("razorpayPaymentId")));
        } catch (Exception e) {
            log.error("Error sending payment notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %s could not be processed. " +
                                    "Please try again or contact support.",
                            amount));
        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {
        log.info("--------------------------------");
        log.info("Account: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Message: {}", message);
        log.info("--------------------------------");
    }
}
