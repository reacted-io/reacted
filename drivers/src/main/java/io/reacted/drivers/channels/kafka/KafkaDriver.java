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
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
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
public class KafkaDriver extends RemotingDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDriver.class);
    @Nullable
    private static final Message NO_VALID_PAYLOAD = null;
    @Nullable
    private static final byte[] NO_SERIALIZED_PAYLOAD = null;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private final KafkaDriverConfig config;
    @Nullable
    private Consumer<Long, Message> kafkaConsumer;
    @Nullable
    private Producer<Long, Message> kafkaProducer;

    public KafkaDriver(KafkaDriverConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        this.kafkaConsumer = Objects.requireNonNull(createConsumer(config));
        this.kafkaProducer = Objects.requireNonNull(createProducer(config));
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> { Objects.requireNonNull(this.kafkaProducer).close();
                                                                        Objects.requireNonNull(this.kafkaConsumer).close();
                                                                      }));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> kafkaDriverLoop(Objects.requireNonNull(this.kafkaConsumer), this, getLocalReActorSystem());
    }

    @Override
    public ChannelId getChannelId() {
        return new ChannelId(ChannelId.ChannelType.KAFKA, this.config.getChannelName());
    }

    @Override
    public Properties getChannelProperties() { return this.config.getProperties(); }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        return Try.of(() -> Objects.requireNonNull(this.kafkaProducer)
                                   .send(new ProducerRecord<>(config.getTopic(), message)).get())
                  .map(metaData -> DeliveryStatus.DELIVERED);
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return true; }

    private static Consumer<Long, Message> createConsumer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, driverConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  MessageDecoder.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, driverConfig.getMaxPollRecords());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<Long, Message> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(driverConfig.getTopic()));
        return consumer;
    }

    public static Producer<Long, Message> createProducer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageEncoder.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void kafkaDriverLoop(Consumer<Long, Message> kafkaConsumer, KafkaDriver thisDriver,
                                        ReActorSystem localReActorSystem) {
        while(!Thread.currentThread().isInterrupted()) {
            Try.of(() -> kafkaConsumer.poll(POLL_TIMEOUT))
               .recover(InterruptException.class,
                        (Try.TryValueSupplier<ConsumerRecords<Long, Message>>) ConsumerRecords::empty)
               .ifSuccessOrElse(records -> records.forEach(record -> thisDriver.offerMessage(record.value())),
                                error -> localReActorSystem.logError("Unable to fetch messages from kafka",
                                                                     error))
               .ifError(error -> LOGGER.error("WUT!?!?!?", error));

        }
        Thread.currentThread().interrupt();
    }

    public static class MessageDecoder implements Deserializer<Message> {
        @Override
        public Message deserialize(String topic, byte[] data) {
            return (Message)
                    Try.withResources(() -> new ObjectInputStream(new ByteArrayInputStream(data)),
                                    ObjectInputStream::readObject)
                       .orElseGet(() -> NO_VALID_PAYLOAD,
                                  error -> LOGGER.error("Unable to properly decode message", error));
        }
    }

    public static class MessageEncoder implements Serializer<Message> {
        @Override
        public byte[] serialize(String topic, Message data) {
            return Try.withChainedResources(ByteArrayOutputStream::new, ObjectOutputStream::new, (byteArray,
                                                                                                  objectOutput) -> {
                objectOutput.writeObject(data);
                return byteArray.toByteArray();
            })
                      .orElseGet(() -> NO_SERIALIZED_PAYLOAD,
                                 error -> LOGGER.error("Unable to encode message", error));
        }
    }
}
