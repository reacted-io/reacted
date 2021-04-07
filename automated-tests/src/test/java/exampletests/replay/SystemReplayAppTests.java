package exampletests.replay;

import exampletests.utils.ExampleUtils;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.drivers.channels.replay.ReplayLocalDriver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class SystemReplayAppTests {
    //NOTE: you may need to provide --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
    //as jvm option
    @Test
            void systemReplayAppTest() throws InterruptedException {
        String dumpDirectory = "/tmp";
        var dumpingLocalDriverCfg = CQDriverConfig.newBuilder()
                                                  .setChronicleFilesDir(dumpDirectory)
                                                  //We are asserting that the channel is so reliable that we do not
                                                  // need to send
                                                  //acks even if requested
                                                  .setChannelRequiresDeliveryAck(true)
                                                  .setTopicName("ReplayTest")
                                                  .setChannelName("ReplayableChannel")
                                                  .build();
        var dumpingLocalDriver = new CQLocalDriver(dumpingLocalDriverCfg);
        var recordedReactorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayAppTests.class.getSimpleName(),
                                                                          dumpingLocalDriver,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                        .initReActorSystem();
        //Every message sent within the reactor system is going to be saved now
        var echoReActions = ReActions.newBuilder()
                                     .reAct((ctx, payload) -> System.out.printf("Received %s from %s @ %s%n",
                                                                                payload.toString(),
                                                                                ctx.getSender().getReActorId().getReActorName(),
                                                                                ctx.getReActorSystem().getSystemConfig()
                                                                                   .getReActorSystemName()))
                                     .build();
        var echoReActorConfig = ReActorConfig.newBuilder()
                                             .setReActorName("EchoReActor")
                                             .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                             .setMailBoxProvider(ctx -> new BasicMbox())
                                             .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                             .build();

        var echoReference = recordedReactorSystem.spawn(echoReActions, echoReActorConfig)
                                                 .orElseSneakyThrow();

        IntStream.range(0, 5)
                 .mapToObj(cycle -> "Message number " + cycle)
                 .forEachOrdered(echoReference::atell);
        TimeUnit.SECONDS.sleep(1);
        recordedReactorSystem.shutDown();
        TimeUnit.SECONDS.sleep(1);
        //Everything has been recorded, now let's try to replay it
        System.out.println("Replay engine setup");
        var replayDriverCfg = new ReplayLocalDriver(dumpingLocalDriverCfg);
        var replayedReActorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayAppTests.class.getSimpleName(),
                                                                          replayDriverCfg,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                        .initReActorSystem();
        System.out.println("Replay Start!");
        //Once the reactor will be created, the system will notify that and will begin its replay
        replayedReActorSystem.spawn(echoReActions, echoReActorConfig).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(1);
        replayedReActorSystem.shutDown();
    }
}
