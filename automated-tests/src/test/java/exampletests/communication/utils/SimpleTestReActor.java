package exampletests.communication.utils;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.util.Objects;

public class SimpleTestReActor implements ReActor {
    private final String splitter;
    private int receivedPings = 0;
    private final int expectedMessages;

    public SimpleTestReActor(String splitter, int expectedMessages) {
        this.splitter = Objects.requireNonNull(splitter);
        this.expectedMessages = ObjectUtils.requiredInRange(expectedMessages, 0, Integer.MAX_VALUE,
                IllegalArgumentException::new);
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                .reAct(ReActorInit.class, (ctx, init) -> ctx.logInfo("ReActor Started!"))
                .reAct(ReActorStop.class, (ctx, stop) -> ctx.logInfo("ReActor Stopped! Received {}/{} pings",
                        receivedPings, expectedMessages))
                .reAct(PreparationRequest.class, (ctx, prepReq) -> ctx.logInfo("ReActor is ready!"))
                .reAct(String.class, this::onPing)
                .build();
    }

    private void onPing(ReActorContext raCtx, String ping) {
        Try.ofRunnable(() -> raCtx.logInfo("Received ping {} on dispatcher {}", ping.split(splitter)[1].trim(),
                Thread.currentThread().getName()))
                .ifError(error -> raCtx.logError("Illegal ping format received", error));
        receivedPings++;
    }

    @Nonnull
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                .setMailBoxProvider(ctx -> new BasicMbox())
                .setReActorName(SimpleTestReActor.class.getSimpleName())
                .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                .build();
    }
}
