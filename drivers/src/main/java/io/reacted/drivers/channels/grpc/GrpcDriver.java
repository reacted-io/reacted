/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@NonNullByDefault
public class GrpcDriver extends RemotingDriver {
    private final GrpcDriverConfig grpcDriverConfig;
    private final Map<String, SystemLinkContainer<ReActedLinkProtocol.ReActedDatagram>> gatesStubs;
    private final ChannelId channelId;
    @Nullable
    private Server grpcServer;
    @Nullable
    private ExecutorService grpcExecutor;

    public GrpcDriver(GrpcDriverConfig grpcDriverConfig) {
        this.grpcDriverConfig = Objects.requireNonNull(grpcDriverConfig);
        this.gatesStubs = new ConcurrentHashMap<>(1000, 0.5f);
        this.channelId = new ChannelId(ChannelId.ChannelType.GRPC, grpcDriverConfig.getChannelName());
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        DriverCtx grpcDriverCtx = RemotingDriver.REACTOR_SYSTEM_CTX.get();
        this.grpcExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, throwable) -> localReActorSystem.logError("Uncaught exception in {}",
                                                                                                thread.getName(), throwable))
                .setNameFormat("Grpc-Executor-" + grpcDriverCtx.getLocalReActorSystem().getLocalReActorSystemId()
                                                               .getReActorSystemName() + "-%d")
                .build());
        this.grpcExecutor.submit(() -> RemotingDriver.REACTOR_SYSTEM_CTX.set(grpcDriverCtx));
        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(this.grpcDriverConfig.getHostName(),
                                                        this.grpcDriverConfig.getPort()))
                                            .executor(this.grpcExecutor)
                                            .addService(new HealthStatusManager().getHealthService())
                                            .addService(new GrpcServer(this))
                                            .build();
    }

    @Override
    public CompletableFuture<Try<Void>> cleanDriverLoop() {
            Objects.requireNonNull(this.grpcServer).shutdown();

            Try.of(() -> this.grpcServer.awaitTermination(5, TimeUnit.SECONDS))
               .ifError(error -> Thread.currentThread().interrupt());

            this.grpcServer.shutdownNow();

            Objects.requireNonNull(this.grpcExecutor).shutdownNow();
            this.gatesStubs.clear();
            return CompletableFuture.completedFuture(Try.ofSuccess(null));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> Objects.requireNonNull(this.grpcServer).start();
    }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        Properties dstChannelIdProperties = message.getDestination().getReActorSystemRef().getGateProperties();
        String dstChannelIdName = dstChannelIdProperties.getProperty(ReActedDriverCfg.CHANNEL_ID_PROPERTY_NAME);

        var grpcLink = this.gatesStubs.computeIfAbsent(dstChannelIdName,
                                                       channelName -> SystemLinkContainer.ofChannel(getNewChannel(dstChannelIdProperties),
                                                                                                    ReActedLinkGrpc::newStub,
                                                                                                    stub -> stub.link(getEmptyMessageHandler(getLocalReActorSystem()))));
        var byteArray = new ByteArrayOutputStream();

        try(ObjectOutputStream oos = new ObjectOutputStream(byteArray)) {
            oos.writeObject(message);
            var payload = ReActedLinkProtocol.ReActedDatagram.newBuilder()
                                                             .setBinaryPayload(ByteString.copyFrom(byteArray.toByteArray()))
                                                             .build();
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (grpcLink) {
                grpcLink.link.onNext(payload);
            }
            return Try.ofSuccess(DeliveryStatus.DELIVERED);

        } catch (Exception error) {
            this.gatesStubs.remove(dstChannelIdName, grpcLink);
            grpcLink.channel.shutdownNow();
            getLocalReActorSystem().logError("Error sending message {}", message.toString(), error);
            return Try.ofFailure(error);
        }
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return grpcDriverConfig.isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return grpcDriverConfig.getProperties(); }

    private static ManagedChannel getNewChannel(Properties channelIdProperties) {
        int port = Integer.parseInt(channelIdProperties.getProperty(GrpcDriverConfig.PORT_PROPERTY_NAME));
        String host = channelIdProperties.getProperty(GrpcDriverConfig.HOST_PROPERTY_NAME);
        return ManagedChannelBuilder.forAddress(host, port)
                                    .keepAliveTime(5, TimeUnit.SECONDS)
                                    .keepAliveWithoutCalls(true)
                                    .enableRetry()
                                    .usePlaintext()
                                    .build();
    }

    private static class GrpcServer extends ReActedLinkGrpc.ReActedLinkImplBase {
        private final GrpcDriver thisDriver;

        public GrpcServer(GrpcDriver thisDriver) {
            this.thisDriver = thisDriver;
        }

        @Override
        public StreamObserver<ReActedLinkProtocol.ReActedDatagram> link(StreamObserver<Empty> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(ReActedLinkProtocol.ReActedDatagram reActedDatagram) {
                    Try.withResources(() -> new ObjectInputStream(new ByteArrayInputStream(reActedDatagram.getBinaryPayload()
                                                                                                          .toByteArray())),
                                      ObjectInputStream::readObject)
                       .ifSuccessOrElse(payload -> thisDriver.offerMessage((Message) payload),
                                        this::onError);
                }

                @Override
                public void onError(Throwable throwable) {
                    thisDriver.getLocalReActorSystem()
                              .logError(GrpcDriver.class.getSimpleName() + " grpc error:", throwable);
                }

                @Override
                public void onCompleted() { }
            };
        }
    }

    private static StreamObserver<Empty> getEmptyMessageHandler(ReActorSystem localReActorSystem) {
        return new StreamObserver<>() {
            @Override
            public void onNext(Empty empty) { }

            @Override
            public void onError(Throwable throwable) {
                localReActorSystem.logError("Unable to communicate with the remote host", throwable);
            }

            @Override
            public void onCompleted() { }
        };
    }

    private static final class SystemLinkContainer<InputTypeT> {
        private final ManagedChannel channel;
        private final StreamObserver<InputTypeT> link;

        private SystemLinkContainer(ManagedChannel channel,
                                    StreamObserver<InputTypeT> link) {
            this.channel = channel;
            this.link = link;
        }

        private static <StubT, InputTypeT>
        SystemLinkContainer<InputTypeT> ofChannel(ManagedChannel channel, Function<ManagedChannel, StubT> toStub,
                                      Function<StubT, StreamObserver<InputTypeT>> toLink) {
            return new SystemLinkContainer<>(channel, toLink.apply(toStub.apply(channel)));
        }
    }
}
