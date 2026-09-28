package com.banking.transactionservice.service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
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

        private static final String TRANSACTION_INITIATED_TOPIC = "transaction_initiated";
        private static final String TRANSACTION_COMPLETED_TOPIC = "transaction_completed";
        private static final String TRANSACTION_REFUNDED_TOPIC = "transaction_refunded";

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
