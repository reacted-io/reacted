package exampletests;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.Service;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.drivers.channels.grpc.GrpcDriver;
import io.reacted.drivers.channels.grpc.GrpcDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.Try;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class AtellExampleTests {

    @Test
    void messageStormTest() throws InterruptedException {
        Properties zooKeeperProps = new Properties();
        var clientSystemCfg = getDefaultReActorSystemCfg("Client",
                SystemLocalDrivers.DIRECT_COMMUNICATION,
                List.of(new ZooKeeperDriver(ZooKeeperDriverConfig.newBuilder()
                        .setTypedSubscriptions(TypedSubscription.LOCAL.forType(ServiceDiscoveryRequest.class))
                        .setSessionTimeout(Duration.ofSeconds(10))
                        .setReActorName("ZooKeeperDriver")
                        .setAsyncExecutor(new ForkJoinPool())
                        .setServiceRegistryProperties(zooKeeperProps)
                        .build())),
                List.of(new GrpcDriver(GrpcDriverConfig.newBuilder()
                        .setHostName("localhost")
                        .setPort(12345)
                        .setChannelRequiresDeliveryAck(true)
                        .setChannelName("TestChannel")
                        .build())));
        var serverSystemCfg = getDefaultReActorSystemCfg("Server",
                //SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/server"),
                SystemLocalDrivers.DIRECT_COMMUNICATION,
                List.of(new ZooKeeperDriver(ZooKeeperDriverConfig.newBuilder()
                        .setTypedSubscriptions(TypedSubscription.LOCAL.forType(ServiceDiscoveryRequest.class))
                        .setSessionTimeout(Duration.ofSeconds(10))
                        .setAsyncExecutor(new ForkJoinPool(2))
                        .setReActorName("ZooKeeperDriver")
                        .setServiceRegistryProperties(zooKeeperProps)
                        .build())),
                List.of(new GrpcDriver(GrpcDriverConfig.newBuilder()
                        .setHostName("localhost")
                        .setPort(54321)
                        .setChannelRequiresDeliveryAck(true)
                        .setChannelName("TestChannel")
                        .build())));
        var clientSystem = new ReActorSystem(clientSystemCfg).initReActorSystem();
        var serverSystem = new ReActorSystem(serverSystemCfg).initReActorSystem();

        TimeUnit.SECONDS.sleep(10);

        serverSystem.spawnService(ServiceConfig.newBuilder()
                .setRouteeProvider(ServerReActor::new)
                .setLoadBalancingPolicy(Service.LoadBalancingPolicy.ROUND_ROBIN)
                .setReActorName("ServerService")
                .setRouteesNum(1)
                .setIsRemoteService(true)
                .build())
                .orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(5);
        var remoteService = clientSystem.serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                .setServiceName("ServerService")
                .build())
                .toCompletableFuture().join();
        var serviceGate = remoteService.filter(gates -> !gates.getServiceGates().isEmpty())
                .orElseSneakyThrow()
                .getServiceGates()
                .iterator().next();

        clientSystem.spawn(new ClientReActor(serviceGate)).orElseSneakyThrow();

        //The reactors are executing now
        TimeUnit.SECONDS.sleep(350);
        //The game is finished, shut down
        clientSystem.shutDown();
        serverSystem.shutDown();
    }

    private static class ServerReActor implements ReActor {
        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.NO_REACTIONS;
        }

        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return ReActorConfig.newBuilder()
                    .setReActorName(ServerReActor.class.getSimpleName())
                    .build();
        }
    }

    private static class ClientReActor implements ReActor {
        private final ReActorRef serverReference;

        public ClientReActor(ReActorRef serverReference) {
            this.serverReference = Objects.requireNonNull(serverReference);
        }

        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return ReActorConfig.newBuilder()
                    .setReActorName(ClientReActor.class.getSimpleName())
                    .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                    .reAct(ReActorInit.class, (ctx, init) -> onInit(ctx))
                    .reAct(ReActions::noReAction)
                    .build();
        }

        private void onInit(ReActorContext raCtx) {
            long start = System.nanoTime();
            AsyncUtils.asyncLoop(noval -> serverReference.atell("Not received"),
                    Try.of(() -> DeliveryStatus.DELIVERED),
                    (Try<DeliveryStatus>) null, 1_000_000L)
                    .thenAccept(status -> System.err.printf("Async loop finished. Time %s Thread %s%n",
                            Duration.ofNanos(System.nanoTime() - start)
                                    .toString(),
                            Thread.currentThread().getName()));
            long end = System.nanoTime();
            System.out.println("Finished storm: " + Duration.ofNanos(end - start).toString());
        }
    }


    public static ReActorSystemConfig getDefaultReActorSystemCfg(
            String reActorSystemName,
            LocalDriver<? extends ChannelDriverConfig<?, ?>> localDriver, Collection<ServiceRegistryDriver<?, ?>> serviceRegistryDrivers,
            Collection<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> remotingDrivers) {
        var configBuilder = ReActorSystemConfig.newBuilder()
                //Tunable parameter for purging java timers from
                // canceled tasks
                //How messages are delivered within a Reactor System
                .setLocalDriver(localDriver)
                //Fan out pool to message type subscribers
                .setMsgFanOutPoolSize(1)
                //Generate extra information for replaying if required
                .setRecordExecution(true)
                .setReactorSystemName(reActorSystemName);
        serviceRegistryDrivers.forEach(configBuilder::addServiceRegistryDriver);
        remotingDrivers.forEach(configBuilder::addRemotingDriver);
        return configBuilder.build();
    }

    public static GrpcDriverConfig getGrpcDriverCfg(int gatePort) {
        return GrpcDriverConfig.newBuilder()
                .setHostName("localhost")
                .setPort(gatePort)
                .setChannelName("TestGrpcChannel")
                .setChannelRequiresDeliveryAck(false)
                .build();
    }
}
