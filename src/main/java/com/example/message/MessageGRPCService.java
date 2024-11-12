/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example.message;

import com.example.MessageRequest;
import com.example.MessageResponse;
import com.example.MessageServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

@GrpcService
public class MessageGRPCService extends MessageServiceGrpc.MessageServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(MessageGRPCService.class);

    private final PulsarTemplate<String> pulsarSpringProducer;

    private final Consumer<String> pulsarConsumer;

     private final ExecutorService executor = Executors.newFixedThreadPool(2);

    public MessageGRPCService(PulsarTemplate<String> pulsarSpringProducer, PulsarClient pulsarClient) {
        this.pulsarSpringProducer = pulsarSpringProducer;
        this.pulsarConsumer = initPulsarConsumer(pulsarClient);
        receiveMessageAsync();
    }

    private Consumer<String> initPulsarConsumer(PulsarClient pulsarClient) {
        final Consumer<String> nativePulsarConsumer;
        try {
            nativePulsarConsumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic("persistent://public/default/tmc-att")
                    .subscriptionName("my-sub")
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        return nativePulsarConsumer;
    }

    private CompletableFuture<Void> receiveMessageAsync() {
        return pulsarConsumer.receiveAsync().thenAccept(message -> {
            log.info("Received message: {}", message.getValue());

            // Using other thread to send post request
            executor.execute(() -> sendPostRequest(message.getValue()));

            try {
                pulsarConsumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
            receiveMessageAsync();
        });
    }

    @Override
    public void publishMessage(MessageRequest request, StreamObserver<MessageResponse> responseObserver) {
        String message = request.getMessage();

        String topic = "persistent://public/default/tmc-att";
        MessageId messageId = pulsarSpringProducer.send(topic, message);
        log.info("Message send to tmc-att, messageID: {}", messageId);

        MessageResponse response = MessageResponse.newBuilder()
            .setMessageID("Message sent, messageID:" + messageId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void sendPostRequest(String message) {
        HttpPost httpPost = new HttpPost("http://localhost:8777/sms-status");
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(message + "-status", ContentType.APPLICATION_JSON));
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                String result = EntityUtils.toString(entity);
                log.info("Response from http server: {}", result);
                // Ensure that the stream is fully consumed
                EntityUtils.consume(entity);
            }

        } catch (IOException | ParseException e) {
            log.error("Failed to send post request", e);
        }
    }
}
