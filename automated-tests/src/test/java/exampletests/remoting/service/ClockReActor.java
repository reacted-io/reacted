package exampletests.remoting.service;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.time.ZonedDateTime;

@NonNullByDefault
@Immutable
public class ClockReActor implements ReActor {
    private final String serviceName;

    ClockReActor(String serviceName) {
        this.serviceName = serviceName;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(TimeRequest.class,
                               (raCtx, timeRequest) -> raCtx.reply(raCtx.getParent(), ZonedDateTime.now()))
                        .reAct(ReActions::noReAction)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(serviceName)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }
}
