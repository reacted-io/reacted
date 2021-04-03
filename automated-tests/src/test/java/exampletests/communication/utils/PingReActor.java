package exampletests.communication.utils;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;

public class PingReActor implements ReActor {
    private int pongReceived;
    private int pingSent;
    private final ReActorRef ponger;

    public PingReActor(ReActorRef ponger) {
        this.pongReceived = 0;
        this.pingSent = 0;
        this.ponger = ponger;
    }

    @Override
    @Nonnull
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(Pong.class, this::onPong)
                        .reAct((raCtx, payload) -> {
                        })
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(PingReActor.class.getSimpleName())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        sendPing(raCtx.getSelf(), pingSent++);
    }

    private void onPong(ReActorContext raCtx, Pong pong) {
        System.out.printf("Ping sent %d Pong received %d%n", pingSent, pongReceived++);
        sendPing(raCtx.getSelf(), pingSent++);
    }

    private void sendPing(ReActorRef sender, int pingSeq) {
        ponger.tell(sender, new Ping(pingSeq));
    }
}
