/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class KafkaDriver extends RemotingDriver<KafkaDriverConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDriver.class);
    @Nullable
    private static final Message NO_VALID_PAYLOAD = null;
    @Nullable
    private static final byte[] NO_SERIALIZED_PAYLOAD = null;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    @Nullable
    private Consumer<String, Message> kafkaConsumer;
    @Nullable
    private Producer<String, Message> kafkaProducer;
    public KafkaDriver(KafkaDriverConfig config) {
        super(config);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        this.kafkaConsumer = Objects.requireNonNull(createConsumer(getDriverConfig()));
        this.kafkaProducer = Objects.requireNonNull(createProducer(getDriverConfig()));
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> { Objects.requireNonNull(kafkaProducer).close();
                                                                        Objects.requireNonNull(kafkaConsumer).close();
                                                                       }));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> kafkaDriverLoop(Objects.requireNonNull(kafkaConsumer), this, getLocalReActorSystem());
    }

    @Override
    public ChannelId getChannelId() {
        return ChannelId.ChannelType.KAFKA.forChannelName(getDriverConfig().getChannelName());
    }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getChannelProperties(); }

    @Override
    public <PayloadT extends ReActedMessage>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long sequenceNumber, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                               PayloadT payload) {

        try {

            Objects.requireNonNull(kafkaProducer)
                   .send(new ProducerRecord<>(getDriverConfig().getTopic(),
                                              Message.of(source, destination, sequenceNumber,
                                                          reActorSystemId, ackingPolicy, payload)))
                   .get();
            return DeliveryStatus.SENT;
        } catch (Exception sendError) {
            getLocalReActorSystem().logError("Error sending message {}", payload.toString(),
                                             sendError);
            return DeliveryStatus.NOT_SENT;
        }
    }

    private static Consumer<String, Message> createConsumer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, driverConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDecoder.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, driverConfig.getMaxPollRecords());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        Consumer<String, Message> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(driverConfig.getTopic()));
        return consumer;
    }

    public static Producer<String, Message> createProducer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageEncoder.class.getName());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        return new KafkaProducer<>(props);
    }

    private static void kafkaDriverLoop(Consumer<String, Message> kafkaConsumer, KafkaDriver thisDriver,
                                        ReActorSystem localReActorSystem) {
        while(!Thread.currentThread().isInterrupted()) {
            try {
                kafkaConsumer.poll(POLL_TIMEOUT)
                             .forEach(record -> thisDriver.offerMessage(record.value().getSender(),
                                                                        record.value().getDestination(),
                                                                        record.value().getSequenceNumber(),
                                                                        record.value().getCreatingReactorSystemId(),
                                                                        record.value().getAckingPolicy(),
                                                                        record.value().getPayload()));
            } catch (InterruptException interruptException) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception exchetionWhileOffering) {
                localReActorSystem.logError("Unable to fetch messages from kafka", exchetionWhileOffering);
            }
        }
    }

    public static class MessageDecoder implements Deserializer<Message> {
        @Override
        @Nullable
        public Message deserialize(String topic, byte[] data) {
            try (var inputStream = new ObjectInputStream(new ByteArrayInputStream(data))) {
                return (Message)inputStream.readObject();
            } catch (Exception anyMessageDecodeError) {
                LOGGER.error("Unable to properly decode message", anyMessageDecodeError);
                return NO_VALID_PAYLOAD;
            }
        }
    }

    public static class MessageEncoder implements Serializer<Message> {
        @Override
        @Nullable
        public byte[] serialize(String topic, Message data) {
            try (var byteArrayOutputStream = new ByteArrayOutputStream();
                 var objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(data);
                return byteArrayOutputStream.toByteArray();
            } catch (Exception anyMessageEncodeError) {
                LOGGER.error("Unable to encode message", anyMessageEncodeError);
                return NO_SERIALIZED_PAYLOAD;
            }
        }
    }
}
