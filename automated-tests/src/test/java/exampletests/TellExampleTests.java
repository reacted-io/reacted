package exampletests;

import exampletests.utils.PingReActor;
import exampletests.utils.PongReActor;
import exampletests.utils.PreparationRequest;
import exampletests.utils.SimpleTestReActor;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class TellExampleTests {
    private static final Collection<ServiceRegistryDriver<?, ?>> NO_SERVICE_REGISTRIES = List.of();
    private static final Collection<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> NO_REMOTING_DRIVERS = List.of();
    public static final String DELIMITER = ":";

    @Test
    void pingTest() {
        var simpleReActorSystem = getDefaultStartedReActorSystem("ReactorSystem");

        var messagesToSend = 20;
        var newReActorInstance = new SimpleTestReActor(DELIMITER, messagesToSend);
        var newReActorReference = simpleReActorSystem.spawn(newReActorInstance.getReActions(),
                newReActorInstance.getConfig())
                .orElse(ReActorRef.NO_REACTOR_REF, error -> {
                    error.printStackTrace();
                    simpleReActorSystem.shutDown();
                });

        var count = new AtomicInteger();
        newReActorReference.tell(ReActorRef.NO_REACTOR_REF, new PreparationRequest())
                .toCompletableFuture()
                .join()
                .filter(DeliveryStatus::isDelivered);
        IntStream.range(0, messagesToSend).parallel()
                .forEach(msgNum -> {
                    newReActorReference.atell("Ping Request" + DELIMITER + msgNum);
                    count.getAndIncrement();
                });

        Assertions.assertEquals(messagesToSend, count.intValue());
    }

    @Test
    void pingPongTest() throws InterruptedException {
        var reactorSystem = getDefaultStartedReActorSystem("ReactorSystem");

        var pongReActor = reactorSystem.spawn(new PongReActor());
        pongReActor.map(PingReActor::new)
                .ifSuccess(reactorSystem::spawn);
        TimeUnit.SECONDS.sleep(10);
        reactorSystem.shutDown();
    }

    public static ReActorSystem getDefaultStartedReActorSystem(String reActorSystemName) {
        return new ReActorSystem(getDefaultReActorSystemCfg(reActorSystemName)).initReActorSystem();
    }

    public static ReActorSystemConfig getDefaultReActorSystemCfg(String reActorSystemName) {
        var configBuilder = ReActorSystemConfig.newBuilder()
                .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                .setMsgFanOutPoolSize(1)
                .setRecordExecution(true)
                .setReactorSystemName(reActorSystemName);
        NO_SERVICE_REGISTRIES.forEach(configBuilder::addServiceRegistryDriver);
        NO_REMOTING_DRIVERS.forEach(configBuilder::addRemotingDriver);
        return configBuilder.build();
    }
}
