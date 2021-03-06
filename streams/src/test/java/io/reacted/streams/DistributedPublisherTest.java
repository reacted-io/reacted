package io.reacted.streams;

import static org.junit.jupiter.api.Assertions.fail;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.streams.ReactedSubmissionPublisher.ReActedSubscription;
import java.io.Serializable;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DistributedPublisherTest {

  private static ReActorSystem system;

  //A Publisher can be sent over the network and used for receiving AND publishing data
  @BeforeAll
  public static void initTests() {
    var cqCfg = CQDriverConfig.newBuilder()
        .setChannelName("TestChannel")
        .setTopicName("DistributedPubSubTest_" +  Runtime.getRuntime().freeMemory())
        .setChronicleFilesDir("/tmp")
        .build();
    system = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                       .setLocalDriver(new CQLocalDriver(cqCfg))
                                                       .setReactorSystemName(DistributedPublisherTest.class.getSimpleName())
                                                       .build()).initReActorSystem();
  }

  @AfterAll
  public static void  stopTests() {
    ObjectUtils.runIfNotNull(system, ReActorSystem::shutDown);
  }

  @Test
  public void testRemotePublishing() throws InterruptedException {
    var publisher = new ReactedSubmissionPublisher<>(system, "TestPublisher");
    var testSubscriber = new TestSubscription();
    publisher.subscribe(testSubscriber);
    var remoteSubscriber = system.spawn(new RemoteReactor("RemoteSubscriber"))
                                 .orElseSneakyThrow();
    remoteSubscriber.tell(publisher);
    TimeUnit.SECONDS.sleep(2);
    publisher.backpressurableSubmit("First Message")
             .toCompletableFuture()
             .join();
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(2L, testSubscriber.getMessagesCount());
  }

  private static class TestSubscription implements Subscriber<Serializable> {
    private Subscription subscription;
    private LongAdder receivedMessages = new LongAdder();
    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(2);
    }

    @Override
    public void onNext(Serializable item) {
      receivedMessages.increment();
    }

    @Override
    public void onError(Throwable throwable) { fail(throwable); }

    @Override
    public void onComplete() { subscription.cancel(); }

    public long getMessagesCount() { return receivedMessages.sum(); }
  }
  private static class RemoteReactor implements ReActor {
    private final ReActorConfig cfg;
    private ReactedSubmissionPublisher<Serializable> publisher;
    private Subscription subscription;

    public RemoteReactor(String reactorName) {
      this.cfg = ReActorConfig.newBuilder()
                              .setReActorName(reactorName)
                              .build();
    }
    @Nonnull
    @Override
    public ReActorConfig getConfig() {
      return cfg;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
      return ReActions.newBuilder()
                      .reAct(ReActorInit.class, ReActions::noReAction)
                      .reAct(ReactedSubmissionPublisher.class, this::onPublisher)
                      .reAct(this::onMessage)
                      .build();
    }

    private void onMessage(ReActorContext raCtx, Serializable message) {
      publisher.backpressurableSubmit(message)
               .thenAccept(noVal -> subscription.request(1));
    }
    private void onPublisher(ReActorContext raCtx,ReactedSubmissionPublisher<Serializable> publisher) {
      this.publisher = publisher;
      publisher.subscribe(new Subscriber<>() {
        @Override
        public void onSubscribe(Subscription subscription) {
          RemoteReactor.this.subscription = subscription;
          subscription.request(1);
        }

        @Override
        public void onNext(Serializable item) { raCtx.selfTell(item); }

        @Override
        public void onError(Throwable throwable) { fail(throwable); }

        @Override
        public void onComplete() { subscription.cancel(); raCtx.stop(); }
      });
    }
  }
}
