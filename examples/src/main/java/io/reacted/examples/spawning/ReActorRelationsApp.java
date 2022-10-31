package io.reacted.examples.spawning;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

public class ReActorRelationsApp {
    public static void main(String[] args) {

        ReActorSystem exampleReActorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                                  .setReactorSystemName("ExampleSystem")
                                                                                  .build()).initReActorSystem();
        try {
            var father = exampleReActorSystem.spawn(new Father(),
                                                    ReActorConfig.newBuilder()
                                                                 .setReActorName("Father")
                                                                 .build()).orElseSneakyThrow();
            var uncle = exampleReActorSystem.spawn(new Uncle(),
                                                   ReActorConfig.newBuilder()
                                                                .setReActorName("Uncle")
                                                                .build()).orElseSneakyThrow();
            father.tell(uncle, new BreedRequest(3));
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exampleReActorSystem.shutDown();
        }
    }

    private static void onStop(ReActorContext raCtx, ReActorStop stop) {
        raCtx.logInfo(raCtx.getSelf().getReActorId().getReActorName() + " is terminating");
    }

    @NonNullByDefault
    private static class Father implements ReActiveEntity {
        private final ReActions fatherReactions;
        private long requestedChildren;

        private Father() {
            this.fatherReactions = ReActions.newBuilder()
                                            .reAct(BreedRequest.class, this::onBreedRequest)
                                            .reAct(ThankYouFather.class, this::onThankYou)
                                            .reAct(ReActorStop.class, ReActorRelationsApp::onStop)
                                            .reAct(ReActions::noReAction)
                                            .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() { return fatherReactions; }

        private void onBreedRequest(ReActorContext raCtx, BreedRequest breedRequest) {
            this.requestedChildren = breedRequest.requestedChildren();

            raCtx.logInfo("{} received a {} for {} from {}",
                           raCtx.getSelf().getReActorId().getReActorName(),
                           breedRequest.getClass().getSimpleName(), breedRequest.requestedChildren(),
                           raCtx.getSender().getReActorId().getReActorName());

            LongStream.range(0, breedRequest.requestedChildren())
                      .forEachOrdered(childNum -> raCtx.spawnChild(new Child(childNum, raCtx.getSender())));
        }

        private void onThankYou(ReActorContext raCtx, ThankYouFather thanks) {
            if (--requestedChildren == 0) {
                raCtx.stop()
                     .thenAcceptAsync(voidVal -> raCtx.reply(ReActorRef.NO_REACTOR_REF, new ByeByeUncle()));
            }
        }
    }

    @NonNullByDefault
    private static class Uncle implements ReActiveEntity {
        private static final ReActions UNCLE_REACTIONS = ReActions.newBuilder()
                                                                  .reAct(Greetings.class, Uncle::onGreetingsFromChild)
                                                                  .reAct(ByeByeUncle.class, Uncle::onByeByeUncle)
                                                                  .reAct(ReActorStop.class, ReActorRelationsApp::onStop)
                                                                  .reAct(ReActions::noReAction)
                                                                  .build();
        @Nonnull
        @Override
        public ReActions getReActions() { return UNCLE_REACTIONS; }

        private static void onGreetingsFromChild(ReActorContext raCtx, Greetings greetingsMessage) {
            raCtx.logInfo("{} received {}. Sending thank you to {}", raCtx.getSelf().getReActorId().getReActorName(),
                          greetingsMessage.greetingsMessage(), raCtx.getSender().getReActorId().getReActorName());
            raCtx.reply(new ThankYouFather());
        }

        private static void onByeByeUncle(ReActorContext raCtx, ByeByeUncle timeToDie) { raCtx.stop(); }
    }

    @NonNullByDefault
    private static class Child implements ReActor {
        private final ReActorRef breedRequester;
        private final ReActorConfig childConfig;
        private Child(long childId, ReActorRef breedRequester) {
            this.breedRequester = breedRequester;
            this.childConfig = ReActorConfig.newBuilder()
                                            .setReActorName(Child.class.getSimpleName() + "-" + childId)
                                            .build();
        }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return childConfig; }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, this::onInit)
                            .reAct(ReActorStop.class, ReActorRelationsApp::onStop)
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext raCtx, ReActorInit init) {
            breedRequester.tell(raCtx.getParent(),
                                new Greetings("Hello from " + childConfig.getReActorName()));
        }
    }

    @Immutable
        private record BreedRequest(long requestedChildren) implements Serializable {
            private BreedRequest(long requestedChildren) {
                this.requestedChildren = ObjectUtils.requiredInRange(requestedChildren, 1L, Long.MAX_VALUE,
                        IllegalArgumentException::new);
            }
        }

    @NonNullByDefault
        @Immutable
        private record Greetings(String greetingsMessage) implements Serializable {
    }

    @Immutable
    private static final class ThankYouFather implements Serializable { private ThankYouFather() { } }

    @Immutable
    private static final class ByeByeUncle implements Serializable { private ByeByeUncle() { } }
}
