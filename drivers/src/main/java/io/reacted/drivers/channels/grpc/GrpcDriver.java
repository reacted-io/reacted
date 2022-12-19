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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

@NonNullByDefault
public class GrpcDriver extends RemotingDriver<GrpcDriverConfig> {
    private final Map<String, SystemLinkContainer<ReActedLinkProtocol.ReActedDatagram>> gatesStubs;
    private final ChannelId channelId;
    @Nullable
    private Server grpcServer;
    @Nullable
    private ExecutorService grpcServerExecutor;
    @Nullable
    private ExecutorService grpcClientExecutor;
    @Nullable
    private EventLoopGroup workerEventLoopGroup;
    @Nullable
    private EventLoopGroup bossEventLoopGroup;
    private DriverCtx grpcDriverCtx = REACTOR_SYSTEM_CTX.get();

    public GrpcDriver(GrpcDriverConfig grpcDriverConfig) {
        super(grpcDriverConfig);
        this.gatesStubs = new ConcurrentHashMap<>(1000, 0.5f);
        this.channelId = ChannelId.ChannelType.GRPC.forChannelName(grpcDriverConfig.getChannelName());
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        this.grpcDriverCtx = REACTOR_SYSTEM_CTX.get();
        this.grpcServerExecutor = Executors.newFixedThreadPool(3, new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, throwable) -> localReActorSystem.logError("Uncaught exception in {}",
                                                                                                thread.getName(), throwable))
                .setNameFormat("Grpc-Server-Executor-" + grpcDriverCtx.getLocalReActorSystem().getLocalReActorSystemId()
                                                                      .getReActorSystemName() + "-%d")
                .build());
        this.grpcClientExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, throwable) -> localReActorSystem.logError("Uncaught exception in {}",
                                                                                                thread.getName(), throwable))
                .setNameFormat("Grpc-Client-Executor-" + grpcDriverCtx.getLocalReActorSystem().getLocalReActorSystemId()
                                                                      .getReActorSystemName() + "-%d")
                .build());
        this.grpcClientExecutor.submit(() -> REACTOR_SYSTEM_CTX.set(grpcDriverCtx));
        this.workerEventLoopGroup = new NioEventLoopGroup(2);
        this.bossEventLoopGroup = new NioEventLoopGroup(1);
        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(getDriverConfig().getHostName(),
                                                                              getDriverConfig().getPort()))
                                            .channelType(NioServerSocketChannel.class)
                                            .executor(grpcServerExecutor)
                                            .bossEventLoopGroup(bossEventLoopGroup)
                                            .workerEventLoopGroup(workerEventLoopGroup)
                                            .permitKeepAliveWithoutCalls(true)
                                            .permitKeepAliveTime(6, TimeUnit.MINUTES)
                                            .addService(new HealthStatusManager().getHealthService())
                                            .addService(new GrpcServer(this))
                                            .build();
    }

    @Override
    public CompletableFuture<Try<Void>> cleanDriverLoop() {
        Objects.requireNonNull(grpcServer).shutdown();
        Try.of(() -> grpcServer.awaitTermination(5, TimeUnit.SECONDS))
           .ifError(error -> Thread.currentThread().interrupt());
        gatesStubs.values()
                  .forEach(linkContainer -> Try.of(() -> linkContainer.channel.shutdown()
                                                                              .awaitTermination(5, TimeUnit.SECONDS))
                                               .ifError(error -> Thread.currentThread().interrupt()));
        if (bossEventLoopGroup != null) {
            bossEventLoopGroup.shutdownGracefully();
        }
        if (workerEventLoopGroup != null) {
            workerEventLoopGroup.shutdownGracefully();
        }
        if (grpcServerExecutor != null) {
            grpcServerExecutor.shutdown();
        }
        if (grpcClientExecutor != null) {
            grpcClientExecutor.shutdown();
        }
        gatesStubs.clear();
        return CompletableFuture.completedFuture(Try.ofSuccess(null));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> Objects.requireNonNull(grpcServer).start();
    }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public <PayloadT extends Serializable>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long seqNum, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                               PayloadT payload) {
        Properties dstChannelIdProperties = destination.getReActorSystemRef().getGateProperties();
        String dstChannelIdName = dstChannelIdProperties.getProperty(ChannelDriverConfig.CHANNEL_ID_PROPERTY_NAME);
        /*
            Fact 1: GRPC links are not bidirectional.
            Fact 2: Every couple Channel Type - Channel Name (Aka channel id) has a dedicated driver instance.
                    To communicate with another reacted node, you need to know the channel id and the channel properties.
                    If some nodes communicate on a channel using the same setup that can be safely stored within
                    the driver instance (i.e. think about a kafka channel:  all the nodes will use the same kafka
                    coordinates) this is not true for GRPC where every node might be on a different ip address / port
            Fact 3: Give the above 2 facts, it means that in order to send a message to a GRPC node, you not only
                    need a driver instance, but also the specific properties of the remote peer.
            Fact 4: On network failures, one route might be canceled. Canceling a route means canceling from the
                    reactor system the information about how to reach a given peer. This information include the
                    channel properties for the remote peer

            Scenario: a message that requires an ACK arrives at this GRPC driver, but a network failure triggered
            the route cancellation before we can send back the ACK.

            ACKs has to be sent using the same driver that processed the incoming message and have to be sent to the
            GENERATING reactor system from datalink layer, not from the nominal sender. If a nominal sender can be
            overridden, a generating reactor system id cannot. This means that an ACK has to be sent to the
            generating reactor system, using the same driver from where the message came from and using the channel id
            managed by the receiving driver.

            For GRPC this is not enough, because we need the properties containing the specific info of the remote
            peer. These info are not sent within the datalink message, so the peer that wants to reply to a message
            (such as sending an ACK) has to retrieve them from the reactor system.

            If a network outage triggers a route unregister, we might still have some reactor references pointing
            to the supposedly dead reactor system or some messages coming from there might still need to be ACKed.

            A route unregister deletes the info regarding how to reach a given reactor system via a specific channel.
            If the route that gets unregistered is grpc, it means that a reactor system does not hold anymore any
            information about how to reach the remote peer using grpc.

            ReActed when a reply has to be sent, sets properly the ReActorSystemRef with the gate and the channel id
            that should be used (that are the same of this driver instance because of what described above), but
            if it cannot find the properties for the remote host (because the channel went offline and got unregistered)
            it simply cannot fill them.

            That said, if an ACK has to be sent back with grpc, the details about Channel Type (the driver type) and
            the channel name are still valid, but the properties are missing. Without the properties there is not much
            that can be done except returning an error
         */
        if (dstChannelIdName == null) {
            getLocalReActorSystem().logDebug("Not sending message. Destination channel is no longer available for message {}",
                                             payload.toString());
            return DeliveryStatus.NOT_SENT;
        }

        SystemLinkContainer<ReActedLinkProtocol.ReActedDatagram> grpcLink;
        var peerChannelKey = getChannelPeerKey(dstChannelIdProperties.getProperty(GrpcDriverConfig.GRPC_HOST),
                                               dstChannelIdProperties.getProperty(GrpcDriverConfig.GRPC_PORT));
        grpcLink = gatesStubs.computeIfAbsent(peerChannelKey,
                                              newPeerChannelKey -> SystemLinkContainer.ofChannel(getNewChannel(dstChannelIdProperties,
                                                                                                               Objects.requireNonNull(grpcClientExecutor),
                                                                                                               () -> removeStaleChannel(newPeerChannelKey)),
                                                                                                 ReActedLinkGrpc::newStub,
                                                                                                 stub -> stub.link(getEmptyMessageHandler(getLocalReActorSystem()))));

        try(ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(byteArray)) {
            oos.writeObject(payload);
            var datagram = toReActedDatagram(source, destination, seqNum, reActorSystemId,
                                             ackingPolicy, ByteString.copyFrom(byteArray.toByteArray()));
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (grpcLink) {
                grpcLink.link.onNext(datagram);
            }
            return DeliveryStatus.SENT;

        } catch (Exception error) {
            removeStaleChannel(peerChannelKey);
            getLocalReActorSystem().logError("Error sending message {}", payload.toString(), error);
            return DeliveryStatus.NOT_SENT;
        }
    }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getChannelProperties(); }

    private void removeStaleChannel(String peerChannelKey) {
        ObjectUtils.ifNotNull(this.gatesStubs.remove(peerChannelKey),
                              linkContainer -> linkContainer.channel.shutdownNow());
    }
    private static ManagedChannel getNewChannel(Properties channelIdProperties, Executor grpcExecutor,
                                                Runnable onCloseCleanup) {
        int port = Integer.parseInt(channelIdProperties.getProperty(GrpcDriverConfig.GRPC_PORT));
        String host = channelIdProperties.getProperty(GrpcDriverConfig.GRPC_HOST);
        return ManagedChannelBuilder.forAddress(host, port)
                                    .keepAliveTime(6, TimeUnit.MINUTES)
                                    .keepAliveWithoutCalls(true)
                                    .enableRetry()
                                    .usePlaintext()
                                    .executor(grpcExecutor)
                                    .intercept(newStreamClosureDetector(onCloseCleanup))
                                    .build();
    }
    private static String getChannelPeerKey(String peerHostname, String peerPort) {
        return peerHostname + "|" + peerPort;
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
                    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(reActedDatagram.getBinaryPayload().toByteArray());
                         ObjectInputStream msgSource = new ObjectInputStream(byteArrayInputStream)) {
                        thisDriver.offerMessage(fromReActorRef(reActedDatagram.getSource(), thisDriver.grpcDriverCtx),
                                                fromReActorRef(reActedDatagram.getDestination(), thisDriver.grpcDriverCtx),
                                                reActedDatagram.getSequenceNumber(),
                                                fromReActorSystemId(reActedDatagram.getGeneratorSystem()),
                                                AckingPolicy.forOrdinal(reActedDatagram.getAckingPolicyOrdinal()),
                                                (Serializable)msgSource.readObject());
                    } catch (Exception deserializationError) {
                        thisDriver.getLocalReActorSystem()
                                  .logError("Error decoding message", deserializationError);
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
    private static ClientInterceptor newStreamClosureDetector(Runnable onStreamClosed) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT>
            interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(channel.newCall(methodDescriptor,
                                                                                             callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        delegate().start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                super.onClose(status, trailers);
                                onStreamClosed.run();
                            }
                        }, headers);
                    }
                };
            }
        };
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

    private record SystemLinkContainer<InputTypeT>(ManagedChannel channel, StreamObserver<InputTypeT> link) {
        private static <StubT, InputTypeT>
            SystemLinkContainer<InputTypeT> ofChannel(ManagedChannel channel, Function<ManagedChannel, StubT> toStub,
                                                      Function<StubT, StreamObserver<InputTypeT>> toLink) {
                return new SystemLinkContainer<>(channel, toLink.apply(toStub.apply(channel)));
            }
    }

    private static ReActedLinkProtocol.ReActedDatagram toReActedDatagram(ReActorRef source, ReActorRef destination,
                                                                         long seqNum,
                                                                         ReActorSystemId localReActorSystemId,
                                                                         AckingPolicy ackingPolicy,
                                                                         ByteString payload) {
        return ReActedLinkProtocol.ReActedDatagram.newBuilder()
                .setSource(toReActorRef(source))
                .setDestination(toReActorRef(destination))
                .setGeneratorSystem(toReActorSystemId(localReActorSystemId))
                .setSequenceNumber(seqNum)
                .setAckingPolicyOrdinal(ackingPolicy.ordinal())
                .setBinaryPayload(payload)
                .build();
    }
    private static ReActorRef fromReActorRef(ReActedLinkProtocol.ReActorRef reActorRef,
                                             DriverCtx driverCtx) {
        if (reActorRef == reActorRef.getDefaultInstanceForType()) {
            return ReActorRef.NO_REACTOR_REF;
        }
        return new ReActorRef(fromReActorId(reActorRef.getReActorId()),
                              fromReActorSystemRef(reActorRef.getReActorSystemRef(), driverCtx));
    }
    private static ReActedLinkProtocol.ReActorRef toReActorRef(ReActorRef reActorRef) {
        if (reActorRef == ReActorRef.NO_REACTOR_REF) {
            return ReActedLinkProtocol.ReActorRef.getDefaultInstance();
        }
        return ReActedLinkProtocol.ReActorRef.newBuilder()
                                             .setReActorSystemRef(toReActorSystemRef(reActorRef.getReActorSystemRef()))
                                             .setReActorId(toReActorId(reActorRef.getReActorId()))
                                             .build();
    }
    private static ReActorSystemRef fromReActorSystemRef(ReActedLinkProtocol.ReActorSystemRef reActorSystemRef,
                                                         DriverCtx ctx) {
        ReActorSystemRef newReActorSystemRef = new ReActorSystemRef();
        ReActorSystemId reActorSystemId = fromReActorSystemId(reActorSystemRef.getReActorSystemId());
        ChannelId channelId = fromChannelId(reActorSystemRef.getSourceChannelId());
        ReActorSystemRef.setGateForReActorSystem(newReActorSystemRef, reActorSystemId, channelId, ctx);
        return newReActorSystemRef;
    }
    private static ReActedLinkProtocol.ReActorSystemRef toReActorSystemRef(ReActorSystemRef reActorSystemRef) {
        return ReActedLinkProtocol.ReActorSystemRef.newBuilder()
                .setReActorSystemId(toReActorSystemId(reActorSystemRef.getReActorSystemId()))
                .setSourceChannelId(toChannelId(reActorSystemRef.getChannelId()))
                .build();
    }
    private static ReActorSystemId fromReActorSystemId(ReActedLinkProtocol.ReActorSystemId reActorSystemId) {
        return reActorSystemId == ReActedLinkProtocol.ReActorSystemId.getDefaultInstance()
               ? ReActorSystemId.NO_REACTORSYSTEM_ID
               : new ReActorSystemId(reActorSystemId.getReactorSystemIdName());
    }
    private static ReActedLinkProtocol.ReActorSystemId toReActorSystemId(ReActorSystemId reActorSystemId) {
        if (reActorSystemId == ReActorSystemId.NO_REACTORSYSTEM_ID) {
            return ReActedLinkProtocol.ReActorSystemId.getDefaultInstance();
        }
        return ReActedLinkProtocol.ReActorSystemId.newBuilder()
                .setReactorSystemIdName(reActorSystemId.getReActorSystemName())
                .build();
    }
    private static ChannelId fromChannelId(ReActedLinkProtocol.ChannelId channelId) {
        return ChannelId.ChannelType.forOrdinal(channelId.getChannelTypeOrdinal())
                                    .forChannelName(channelId.getChannelName());
    }
    private static ReActedLinkProtocol.ChannelId toChannelId(ChannelId channelId) {
        return ReActedLinkProtocol.ChannelId.newBuilder()
                .setChannelTypeOrdinal(channelId.getChannelType().ordinal())
                .setChannelName(channelId.getChannelName())
                .build();
    }
    private static ReActorId fromReActorId(ReActedLinkProtocol.ReActorId reActorId) {
        if (reActorId.getDefaultInstanceForType().equals(reActorId)) {
            return ReActorId.NO_REACTOR_ID;
        }
        var uuid = fromUUID(reActorId.getUuid());
        return new ReActorId().setReActorName(reActorId.getReactorName())
                              .setReActorUUID(uuid)
                              .setHashCode(Objects.hash(uuid, reActorId.getReactorName()));
    }
    private static ReActedLinkProtocol.ReActorId toReActorId(ReActorId reActorId) {
        return reActorId == ReActorId.NO_REACTOR_ID
               ? ReActedLinkProtocol.ReActorId.getDefaultInstance()
               : ReActedLinkProtocol.ReActorId.newBuilder()
                       .setReactorName(reActorId.getReActorName())
                       .setUuid(toUUID(reActorId.getReActorUUID()))
                       .build();
    }
    private static UUID fromUUID(ReActedLinkProtocol.UUID uuid) {
        return ReActorId.NO_REACTOR_ID_UUID.getLeastSignificantBits() == uuid.getLeastSignificantBits() &&
               ReActorId.NO_REACTOR_ID_UUID.getMostSignificantBits() == uuid.getMostSignificantBits()
               ? ReActorId.NO_REACTOR_ID_UUID
               : new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
    private static ReActedLinkProtocol.UUID toUUID(UUID uuid) {
        return ReActedLinkProtocol.UUID.newBuilder()
                .setLeastSignificantBits(uuid.getLeastSignificantBits())
                .setMostSignificantBits(uuid.getMostSignificantBits())
                .build();
    }
}
