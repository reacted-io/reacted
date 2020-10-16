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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@NonNullByDefault
public class GrpcDriver extends RemotingDriver<GrpcDriverConfig> {
    private final Map<String, SystemLinkContainer<ReActedLinkProtocol.ReActedDatagram>> gatesStubs;
    private final ChannelId channelId;
    @Nullable
    private Server grpcServer;
    @Nullable
    private ExecutorService grpcExecutor;
    @Nullable
    private EventLoopGroup workerEventLoopGroup;
    @Nullable
    private EventLoopGroup bossEventLoopGroup;


    public GrpcDriver(GrpcDriverConfig grpcDriverConfig) {
        super(grpcDriverConfig);
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
        this.workerEventLoopGroup = new NioEventLoopGroup(5);
        this.bossEventLoopGroup = new NioEventLoopGroup(1);
        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(getDriverConfig().getHostName(),
                                                                              getDriverConfig().getPort()))
                                            .channelType(NioServerSocketChannel.class)
                                            .executor(this.grpcExecutor)
                                            .bossEventLoopGroup(this.bossEventLoopGroup)
                                            .workerEventLoopGroup(this.workerEventLoopGroup)
                                            .addService(new HealthStatusManager().getHealthService())
                                            .addService(new GrpcServer(this))
                                            .build();
    }

    @Override
    public CompletableFuture<Try<Void>> cleanDriverLoop() {
        Objects.requireNonNull(this.grpcServer).shutdown();

        Try.of(() -> this.grpcServer.awaitTermination(5, TimeUnit.SECONDS))
           .ifError(error -> Thread.currentThread().interrupt());
        if (this.grpcServer != null) {
            this.grpcServer.shutdown();
        }
        if (this.bossEventLoopGroup != null) {
            this.bossEventLoopGroup.shutdownGracefully();
        }
        if (this.workerEventLoopGroup != null) {
            this.workerEventLoopGroup.shutdownGracefully();
        }
        if (this.grpcExecutor != null) {
            this.grpcExecutor.shutdown();
        }
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
        String dstChannelIdName = dstChannelIdProperties.getProperty(ChannelDriverConfig.CHANNEL_ID_PROPERTY_NAME);

        var grpcLink = this.gatesStubs.computeIfAbsent(dstChannelIdName,
                                                       channelName -> SystemLinkContainer.ofChannel(getNewChannel(dstChannelIdProperties,
                                                                                                                  Objects.requireNonNull(this.grpcExecutor)),
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
    public boolean channelRequiresDeliveryAck() { return getDriverConfig().isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getProperties(); }

    private static ManagedChannel getNewChannel(Properties channelIdProperties, Executor grpcExecutor) {
        int port = Integer.parseInt(channelIdProperties.getProperty(GrpcDriverConfig.GRPC_PORT));
        String host = channelIdProperties.getProperty(GrpcDriverConfig.GRPC_HOST);
        return ManagedChannelBuilder.forAddress(host, port)
                                    .keepAliveTime(6, TimeUnit.MINUTES)
                                    .keepAliveWithoutCalls(true)
                                    .enableRetry()
                                    .usePlaintext()
                                    .executor(grpcExecutor)
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
                    try (ObjectInputStream msgSource = new ObjectInputStream(new ByteArrayInputStream(reActedDatagram.getBinaryPayload()
                                                                                                          .toByteArray()))) {
                        thisDriver.offerMessage((Message)msgSource.readObject());
                    } catch (Exception deserializationError) {
                        throw new RuntimeException(deserializationError);
                    }
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
