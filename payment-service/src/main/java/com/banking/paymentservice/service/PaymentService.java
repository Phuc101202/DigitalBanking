package com.banking.paymentservice.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

@Service
@Slf4j
@RequiredArgsConstructor

public class PaymentService {

        private final PaymentRepository paymentRepository;
        private final KafkaTemplate<String, Object> kafkaTemplate;

        @Value("${razorpay.key.id}")
        private String keyId;

        @Value("${razorpay.key.secret}")
        private String keySecret;

        private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";

        private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

        /**
         * Create Razorpay payment order.
         * 
         * FLOW:
         * 1. Create order in razorpay
         * 2. Save Payment record in DB
         * 3. Return order details to frontend
         * 4. Frontend show Razorpay checkout
         * 5. User pays
         * 6. Razorpay calls webhook
         * 
         * @param request
         * @return
         */
        public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {

                log.info("Creating payment order for account: {} amount: {}",
                                request.getAccountNumber(), request.getAmount());

                RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

                // Converted Amount
                int convertedAmount = request.getAmount()
                                .multiply(BigDecimal.valueOf(100))
                                .intValue();
                JSONObject orderRequest = new JSONObject();
                orderRequest.put("amount", convertedAmount);
                orderRequest.put("currency", "USD/INR");
                orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                                .replace("-", "").substring(0, 10));
                Order razorpayOrder = razorpayClient.orders.create(orderRequest);

                log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());

                // Save payment record
                Payment payment = new Payment();
                payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
                payment.setAccountNumber(request.getAccountNumber());
                payment.setAmount(request.getAmount());
                payment.setCurrency("USD/INR");
                payment.setStatus(PaymentStatus.CREATED);
                payment.setDescription(request.getDescription());

                Payment savedPayment = paymentRepository.save(payment);

                return new PaymentOrderResponse(
                                savedPayment.getId(),
                                razorpayOrder.get("id").toString(),
                                request.getAmount(),
                                "USD/INR",
                                "CREATED",
                                keyId);
        }

        public void handleWebhook(Map<String, Object> payload) {
                log.info("Received Razorpay webhook: {}", payload.get("event"));

                String event = (String) payload.get("event");

                if ("payment.captured".equals(event)) {
                        handlePaymentSuccess(payload);
                } else if ("payment.failed".equals(event)) {
                        handlePaymentFailure(payload);
                }
        }

        private void handlePaymentSuccess(Map<String, Object> payload) {
                try {

                        Map<String, Object> paymentData = extractPaymentData(payload);
                        String orderId = (String) paymentData.get("order_id");
                        String paymentId = (String) paymentData.get("id");
                        Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                                        .orElseThrow(() -> new RuntimeException(
                                                        "Payment not found for order: " + orderId));
                        payment.setRazorpayOrderId(paymentId);
                        payment.setStatus(PaymentStatus.COMPLETED);

                        // Publish payment completed event
                        Map<String, Object> event = new HashMap<>();
                        event.put("paymentId", payment.getId());
                        event.put("accountNumber", payment.getAccountNumber());
                        event.put("amount", payment.getAmount());
                        event.put("razorpayPaymentId", paymentId);

                        kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
                        log.info("Payment completed: {}", payment.getId());

                } catch (Exception e) {
                        log.error("Error handling payment success: {}", e.getMessage());
                }
        }

        private void handlePaymentFailure(Map<String, Object> payload) {

        }

        private void extractPaymentData(Map<String, Object> payload) {
        }
}
