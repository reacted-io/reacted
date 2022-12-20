/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class KafkaDriver extends RemotingDriver<KafkaDriverConfig> {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    @Nullable
    private Consumer<String, io.reacted.drivers.channels.kafka.avro.Message> kafkaConsumer;
    @Nullable
    private Producer<String, io.reacted.drivers.channels.kafka.avro.Message> kafkaProducer;

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
    public <PayloadT extends Serializable>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long sequenceNumber, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                               PayloadT payload) {

        try {

            Objects.requireNonNull(kafkaProducer)
                   .send(new ProducerRecord<>(getDriverConfig().getTopic(),
                                              KafkaUtils.toAvroMessage(source, destination, sequenceNumber,
                                                                       reActorSystemId, ackingPolicy, payload)))
                   .get();
            return DeliveryStatus.SENT;
        } catch (Exception sendError) {
            getLocalReActorSystem().logError("Error sending message {}", payload.toString(),
                                             sendError);
            return DeliveryStatus.NOT_SENT;
        }
    }

    private static Consumer<String, io.reacted.drivers.channels.kafka.avro.Message> createConsumer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, driverConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, driverConfig.getMaxPollRecords());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        Consumer<String, io.reacted.drivers.channels.kafka.avro.Message> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(driverConfig.getTopic()));
        return consumer;
    }

    public static Producer<String, io.reacted.drivers.channels.kafka.avro.Message> createProducer(KafkaDriverConfig driverConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.getBootstrapEndpoint());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        props.put("schema.registry.url", driverConfig.getAvroSchemaRegistryUrl());
        return new KafkaProducer<>(props);
    }

    private static void kafkaDriverLoop(Consumer<String, io.reacted.drivers.channels.kafka.avro.Message> kafkaConsumer,
                                        KafkaDriver thisDriver, ReActorSystem localReActorSystem) {
        DriverCtx driverCtx = ReActorSystemDriver.getDriverCtx();
        while(!Thread.currentThread().isInterrupted()) {
            try {
                for(var record : kafkaConsumer.poll(POLL_TIMEOUT)) {
                    thisDriver.offerMessage(KafkaUtils.fromAvroReActorRef(record.value().getSource(), driverCtx),
                                            KafkaUtils.fromAvroReActorRef(record.value().getDestination(), driverCtx),
                                            record.value().getSequenceNumber(),
                                            KafkaUtils.fromAvroReActorSystemId(record.value().getCreatorReactorSystem()),
                                            KafkaUtils.fromAvroAckingPolicy(record.value().getAckingPolicy()),
                                            KafkaUtils.fromSerializedPayload(record.value().getPayload()));

                }
            } catch (InterruptException interruptException) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception exceptionWhileOffering) {
                localReActorSystem.logError("Unable to fetch messages from kafka", exceptionWhileOffering);
            }
        }
    }
}
