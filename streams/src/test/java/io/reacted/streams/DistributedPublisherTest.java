package io.reacted.streams;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriverConfig;
import io.reacted.patterns.ObjectUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.fail;

public class DistributedPublisherTest {

  private static ReActorSystem system;

  //A Publisher can be sent over the network and used for receiving AND publishing data
  @BeforeAll
  public static void initTests() {
    var cqCfg = CQLocalDriverConfig.newBuilder()
                                   .setChannelName("TestChannel")
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
    var publisher = new ReactedSubmissionPublisher<>(system, 10_000, "TestPublisher");
    var testSubscriber = new TestSubscription();
    publisher.subscribe(testSubscriber);
    var remoteSubscriber = system.spawn(new RemoteReactor("RemoteSubscriber"))
                                 .orElseSneakyThrow();
    remoteSubscriber.publish(publisher);
    TimeUnit.SECONDS.sleep(2);
    publisher.submit(ReActedMessage.of("First Message"));
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(2L, testSubscriber.getMessagesCount());
  }

  private static class TestSubscription implements Subscriber<ReActedMessage> {
    private Subscription subscription;
    private LongAdder receivedMessages = new LongAdder();
    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(2);
    }

    @Override
    public void onNext(ReActedMessage item) {
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
    private ReactedSubmissionPublisher<ReActedMessage> publisher;
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

    private void onMessage(ReActorContext ctx, ReActedMessage message) {
      publisher.submit(message);
      subscription.request(1);
    }
    private void onPublisher(ReActorContext ctx,ReactedSubmissionPublisher<ReActedMessage> publisher) {
      this.publisher = publisher;
      publisher.subscribe(new Subscriber<>() {
        @Override
        public void onSubscribe(Subscription subscription) {
          RemoteReactor.this.subscription = subscription;
          subscription.request(1);
        }

        @Override
        public void onNext(ReActedMessage item) { ctx.selfPublish(item); }

        @Override
        public void onError(Throwable throwable) { fail(throwable); }

        @Override
        public void onComplete() { subscription.cancel(); ctx.stop(); }
      });
    }
  }
}
