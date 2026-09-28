package com.banking.transactionservice.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.banking.transactionservice.entity.Transaction;

public interface TransactionRepository extends JpaRepository<Transaction, String> {

    List<Transaction> findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(String senderAccountNumber);
}
