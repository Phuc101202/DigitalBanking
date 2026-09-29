package com.banking.transactionservice.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

        private final TransactionRepository transactionRepository;
        private final AccountServiceClient accountServiceClient;

        private final KafkaTemplate<String, Object> kafkaTemplate;
        private final RedisTemplate<String, String> redisTemplate;

        private static final String TRANSACTION_INITIATED_TOPIC = "transaction_initiated";
        private static final String TRANSACTION_COMPLETED_TOPIC = "transaction_completed";
        private static final String TRANSACTION_REFUNDED_TOPIC = "transaction_refunded";
        private static final String FRAUD_DETECTED_TOPIC = "fraud_detected";

        /**
         * SAGA STEP - 1: Initiate transfer
         * Deduct from sender via feign
         * Saves transaction as PROCESSING.
         * Publish event to kafka for fraud check
         * Returns.
         * 
         * @param request
         * @return
         */
        public TransactionResponse transfer(TransferRequest request) {
                log.info("SAGA START - Transfer: {} -> () amount: {}",
                                request.getSenderAccountNumber(),
                                request.getReceiverAmountNumber(),
                                request.getAmount());
                // SAGA STEP 1: Deduct from sender
                accountServiceClient.deductBalance(
                                request.getSenderAccountNumber(),
                                request.getAmount());
                Transaction transaction = new Transaction();
                transaction.setSenderAccountNumber(request.getSenderAccountNumber());
                transaction.setReceiverAccountNumber(request.getReceiverAmountNumber());
                transaction.setAmount(request.getAmount());
                transaction.setType(TransactionType.TRANSFER);
                transaction.setStatus(TransactionStatus.PROCESSING);
                transaction.setDescription(request.getDescription());
                transaction.setReferenceNumber(UUID.randomUUID().toString());

                Transaction savedTransaction = transactionRepository.save(transaction);
                log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

                // SAGA STEP - 2: Publish for fraud check
                TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                                savedTransaction.getId(),
                                savedTransaction.getReceiverAccountNumber(),
                                savedTransaction.getReceiverAccountNumber(),
                                savedTransaction.getAmount(),
                                savedTransaction.getDescription());
                kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
                log.info("SAGA STEP 2 - TransactionInitiatedEvent published: {}", savedTransaction.getId());

                return mapToResponse(savedTransaction);
        }

        public TransactionResponse getTransaction(String transactionId) {
                Transaction transaction = transactionRepository.findById(transactionId)
                                .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

                return mapToResponse(transaction);
        }

        public List<TransactionResponse> getTransactionHistory(String accountNumber) {
                return transactionRepository
                                .findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(accountNumber)
                                .stream()
                                .map(this::mapToResponse)
                                .collect(Collectors.toList());
        }

        public TransactionResponse verifyOTP(String transactionID, String otp) {
                log.info("OTP verification for the transaction: {}", transactionID);

                Transaction transaction = transactionRepository.findById(transactionID)
                                .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionID));
                String otpKey = "verification:otp:" + transactionID;
                String storedOtp = redisTemplate.opsForValue().get(otpKey);

                if (storedOtp == null) {
                        log.warn("OTP expired for transaction: {}", transactionID);
                        compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
                        return mapToResponse(transaction);
                }

                if (!storedOtp.equals(otp)) {
                        log.warn("Invalid OTP - blocking account and refunding for transaction: {}", transactionID);
                        redisTemplate.delete(otpKey);
                        blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction cancelled, " +
                                        "account blocked for security.");
                        return mapToResponse(transaction);
                }

                log.info("OTP verified - completing transaction: {}", transactionID);
                redisTemplate.delete(otpKey);
                completeTransaction(transaction);
                return mapToResponse(transaction);
        }

        private void compensateTransaction(Transaction transaction, String reason) {
                log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                                transaction.getSenderAccountNumber(),
                                transaction.getAmount());
                accountServiceClient.creditBalance(
                                transaction.getSenderAccountNumber(),
                                transaction.getAmount());
                transaction.setStatus(TransactionStatus.FLAGGED);
                transaction.setFailureReason(reason +
                                " - SAGA Compensation executed, amount refunded at " + LocalDateTime.now());

                transactionRepository.save(transaction);
                // PUBLISH refund event - Notification service will alert user
                Map<String, Object> refundEvent = new HashMap<>();
                refundEvent.put("transactionId", transaction.getId());
                refundEvent.put("accountNumber", transaction.getSenderAccountNumber());
                refundEvent.put("amount", transaction.getAmount());
                refundEvent.put("reason", reason);
                kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

                log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}", transaction.getId(),
                                transaction.getSenderAccountNumber());
        }

        private void blockAccountAndCompensate(Transaction transaction, String reason) {
                // Publish fraud.detected -> Account Service will block account
                Map<String, Object> fraudEvent = new HashMap<>();
                fraudEvent.put("transactionId", transaction.getId());
                fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
                fraudEvent.put("reason", reason);
                kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getId(), fraudEvent);
                log.warn("fraud.detected published - account: {} will be blocked, Kindly contact to the bank",
                                transaction.getSenderAccountNumber());

                // SAGA COMPENSATION - refund amount to sender
                compensateTransaction(transaction, reason);
        }

        private void completeTransaction(Transaction transaction) {

                transaction.setStatus(TransactionStatus.COMPLETED);
                transaction.setCompletedAt(LocalDateTime.now());
                transactionRepository.save(transaction);

                // PUBLISH completed event - Notification service will alert user
                TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                                transaction.getId(),
                                transaction.getSenderAccountNumber(),
                                transaction.getReceiverAccountNumber(),
                                transaction.getAmount(),
                                transaction.getDescription());
                kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

                log.info("SAGA COMPLETED - Transaction completed: {} amount: {}",
                                transaction.getSenderAccountNumber(),
                                transaction.getAmount());
        }

        public void processCleanResult(String transactionId) {
                Transaction transaction = transactionRepository.findById(transactionId)
                                .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

                if (transaction.getStatus() != TransactionStatus.PROCESSING) {
                        log.warn("Transaction: {} not PROCESSING - skipping", transactionId);
                        return;
                }

                log.info("Fraud check clean result for transaction: {} - completing transaction", transactionId);
                completeTransaction(transaction);
        }

        private TransactionResponse mapToResponse(Transaction transaction) {
                TransactionResponse response = new TransactionResponse();
                response.setId(transaction.getId());
                response.setSenderAccountNumber(transaction.getSenderAccountNumber());
                response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
                response.setAmount(transaction.getAmount());
                response.setType(transaction.getType());
                response.setStatus(transaction.getStatus());
                response.setDescription(transaction.getDescription());
                response.setReferenceNumber(transaction.getReferenceNumber());
                response.setFailureReason(transaction.getFailureReason());
                response.setCreatedAt(transaction.getCreatedAt());
                response.setCompletedAt(transaction.getCompletedAt());

                return response;
        }
}
