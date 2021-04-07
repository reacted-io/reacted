package exampletests.communication;

import exampletests.utils.ExampleUtils;
import exampletests.communication.utils.TimeRequest;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.Try;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AskExampleTests {

    @Test
    void brokenClockApp() {
        var reActorSystem = ExampleUtils.getDefaultInitedReActorSystem("BrokenClockApp");

        var reactiveClockConfig = ReActorConfig.newBuilder()
                //Accept at maximum 5 messages in the mailbox at the same time,
                //drop the ones in excess causing the delivery to fail
                .setMailBoxProvider(ctx -> new BoundedBasicMbox(5))
                .setReActorName("Reactive Clock")
                .build();
        //We did not set any reaction, this clock is not going to reply to anything
        var brokenReactiveClock = reActorSystem.spawn(ReActions.NO_REACTIONS, reactiveClockConfig)
                .orElseSneakyThrow();
        //Note: we do not need another reactor to intercept the answer
        brokenReactiveClock.ask(new TimeRequest(), Instant.class, Duration.ofSeconds(2), "What's the time?").
                thenApply(timeReply -> timeReply.map(instant -> "Wow, unexpected")
                        .orElse("Clock did not reply as expected"))
                .thenAccept(System.out::println)
                .thenAccept(nullValue -> reActorSystem.shutDown());
    }

    @Test
    void reactiveClockApp() {
            var reActorSystem = ExampleUtils.getDefaultInitedReActorSystem("ReactiveClockApp");

            var reactiveClockReactions = ReActions.newBuilder()
                    .reAct(TimeRequest.class,
                            (raCtx, request) -> raCtx.getSender()
                                    .tell(Instant.now()))
                    //For any other message type simply ignore the message
                    .reAct((ctx, any) -> {})
                    .build();
            var reactiveClockConfig = ReActorConfig.newBuilder()
                    .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                    //Accept at maximum 5 messages in the mailbox at the same time,
                    //drop the ones in excess causing the delivery to fail
                    .setMailBoxProvider(ctx -> new BoundedBasicMbox(5))
                    .setReActorName("Reactive Clock")
                    .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                    .build();
            var reactiveClock = reActorSystem.spawn(reactiveClockReactions, reactiveClockConfig)
                    .orElseSneakyThrow();
            //Note: we do not need another reactor to intercept the answer
            reactiveClock.ask(new TimeRequest(), Instant.class, "What's the time?")
                    //Ignore the exception, it's just an example
                    .thenApply(Try::orElseSneakyThrow)
                    .thenAccept(time -> System.out.printf("It's %s%n",
                            ZonedDateTime.ofInstant(time, ZoneId.systemDefault())))
                    .thenAcceptAsync(nullValue -> reActorSystem.shutDown());
        }
}
