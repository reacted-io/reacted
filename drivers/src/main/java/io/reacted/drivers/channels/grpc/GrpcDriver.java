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
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
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

@NonNullByDefault
public class GrpcDriver extends RemotingDriver {
    private final GrpcDriverConfig grpcDriverConfig;
    private final Map<String, StreamObserver<ReActedLinkProtocol.ReActedDatagram>> gatesStubs;
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
                                                             .setNameFormat("Grpc-Executor-" + grpcDriverCtx.getLocalReActorSystem()
                                                                                                            .getLocalReActorSystemId()
                                                                                                            .getReActorSystemName() +
                                                                            "-%d").build());
        this.grpcExecutor.submit(() -> RemotingDriver.REACTOR_SYSTEM_CTX.set(grpcDriverCtx));
        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(this.grpcDriverConfig.getHostName(),
                                                        this.grpcDriverConfig.getPort()))
                                            .executor(this.grpcExecutor)
                                            .addService(new HealthStatusManager().getHealthService())
                                            .addService(ServerInterceptors.intercept(new GrpcServer(this),
                                                        new ServerInterceptor() {
                                                            @Override
                                                            public <ReqT, RespT> ServerCall.Listener<ReqT>
                                                            interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata,
                                                                          ServerCallHandler<ReqT, RespT> serverCallHandler) {
                                                                return serverCallHandler.startCall(serverCall, metadata);
                                                            }
                                                        }))
                                            .build();
    }

    @Override
    public CompletableFuture<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> {
            Objects.requireNonNull(grpcServer).shutdownNow();
            Objects.requireNonNull(grpcExecutor).shutdownNow();
            gatesStubs.clear(); }));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> { Objects.requireNonNull(grpcServer).start();
                       try {
                           grpcServer.awaitTermination();
                       } catch (InterruptedException shutdown) {
                           Thread.currentThread().interrupt();
                       }};
    }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        Properties dstChannelIdProperties = message.getDestination().getReActorSystemRef().getGateProperties();
        String dstChannelIdName = dstChannelIdProperties.getProperty(ReActedDriverCfg.CHANNEL_ID_PROPERTY_NAME);

        var grpcLink = this.gatesStubs.computeIfAbsent(dstChannelIdName,
                                                       channelName -> getNewLink(dstChannelIdProperties)
                                                                            .link(GrpcServer.EMPTY_MESSAGE_HANDLER));
        var byteArray = new ByteArrayOutputStream();

        try(ObjectOutputStream oos = new ObjectOutputStream(byteArray)) {
            oos.writeObject(message);
            var payload = ReActedLinkProtocol.ReActedDatagram.newBuilder()
                                                             .setBinaryPayload(ByteString.copyFrom(byteArray.toByteArray()))
                                                             .build();
            synchronized (grpcLink) {
                grpcLink.onNext(payload);
            }
            return Try.ofSuccess(DeliveryStatus.DELIVERED);

        } catch (Exception error) {
            this.gatesStubs.remove(dstChannelIdName);
            getLocalReActorSystem().logError("Error sending message %s", error, message.toString());
            return Try.ofFailure(error);
        }
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return grpcDriverConfig.isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return grpcDriverConfig.getProperties(); }

    private static ReActedLinkGrpc.ReActedLinkStub getNewLink(Properties channelIdProperties) {
        int port = Integer.parseInt(channelIdProperties.getProperty(GrpcDriverConfig.PORT_PROPERTY_NAME));
        String host = channelIdProperties.getProperty(GrpcDriverConfig.HOST_PROPERTY_NAME);
        var managedChannel = ManagedChannelBuilder.forAddress(host, port)
                                                  .keepAliveWithoutCalls(true)
                                                  .usePlaintext()
                                                  .build();
        return ReActedLinkGrpc.newStub(managedChannel);
    }

    private static class GrpcServer extends ReActedLinkGrpc.ReActedLinkImplBase {
        public static final StreamObserver<Empty> EMPTY_MESSAGE_HANDLER = getEmptyMessageHandler();
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

    private static StreamObserver<Empty> getEmptyMessageHandler() {
        return new StreamObserver<>() {
            @Override
            public void onNext(Empty empty) { }

            @Override
            public void onError(Throwable throwable) { }

            @Override
            public void onCompleted() { }
        };
    }
}
