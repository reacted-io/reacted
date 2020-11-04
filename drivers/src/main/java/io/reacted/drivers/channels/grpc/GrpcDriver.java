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
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerStreamTracer;
import io.grpc.ServerTransportFilter;
import io.grpc.Status;
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
import io.reacted.core.exceptions.ChannelUnavailableException;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
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
                                            .executor(grpcExecutor)
                                            .bossEventLoopGroup(bossEventLoopGroup)
                                            .workerEventLoopGroup(workerEventLoopGroup)
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
        if (grpcExecutor != null) {
            grpcExecutor.shutdown();
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
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        Properties dstChannelIdProperties = message.getDestination().getReActorSystemRef().getGateProperties();
        String dstChannelIdName = dstChannelIdProperties.getProperty(ChannelDriverConfig.CHANNEL_ID_PROPERTY_NAME);
        /*
            Fact 1: GRPC links are not bidirectional.
            Fact 2: Every couple Channel Type - Channel Name (Aka channel id) has a dedicate driver instance.
                    To communicate with another reacted node, you need to know the channel id and the channel properties.
                    If some nodes communicate on a channel using the same setup that can be safely stored within
                    the driver instance (i.e. think about a kafka channel:  all the nodes will use the same kafka
                    coordinates) this is not true for GRPC where every node might be on a different ip address / port
            Fact 3: Give the above 2 facts, it means that in order to send a message to a GRPC node, you not only
                    need a driver instance, but also the specific properties of the remote peer.
            Fact 4: On network failures, one route might be canceled. Canceling a route means canceling from the
                    reactor system the information about how to reach a given peer. These information include the
                    channel properties for the remote peer

            Scenario: a message that requires an ACK arrives on this GRPC driver, but a network failure triggered
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
            return Try.ofFailure(new ChannelUnavailableException());
        }
        SystemLinkContainer<ReActedLinkProtocol.ReActedDatagram> grpcLink;
        var peerChannelKey = getChannelPeerKey(dstChannelIdProperties.getProperty(GrpcDriverConfig.GRPC_HOST),
                                               dstChannelIdProperties.getProperty(GrpcDriverConfig.GRPC_PORT));
        grpcLink = gatesStubs.computeIfAbsent(peerChannelKey,
                                              newPeerChannelKey -> SystemLinkContainer.ofChannel(getNewChannel(dstChannelIdProperties,
                                                                                                               Objects.requireNonNull(grpcExecutor),
                                                                                                               () -> removeStaleChannel(newPeerChannelKey)),
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
            removeStaleChannel(peerChannelKey);
            getLocalReActorSystem().logError("Error sending message {}", message.toString(), error);
            return Try.ofFailure(error);
        }
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return getDriverConfig().isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getProperties(); }

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

    private static final class SystemLinkContainer<InputTypeT> {
        private final ManagedChannel channel;
        private final StreamObserver<InputTypeT> link;
        private SystemLinkContainer(ManagedChannel channel, StreamObserver<InputTypeT> link) {
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
