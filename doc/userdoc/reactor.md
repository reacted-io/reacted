# ReActor

A replayable actor or `reactor`, is a reactive entity that has a state, a [mailbox](mailboxes.md) and some 
*reactions* that map a received *message* type to a certain action. ReActors communicate with each other using messages:
when a message is received by a reactor, it is saved within its mailbox waiting for the proper `reaction` to be called. 
The messages are processed strictly sequentially and in the order specified by the mailbox. From the programmer perspective,
this cycle is completely single threaded, removing any need for synchronization or thread safe operations.

## Hierarchy and Life Cycle

Every reactor is part of a hierarchy: it has a single and immutable father during all its lifecycle and can have as many
children as required. If a child terminates it has not impact on the father reactor, but if a father terminates then
the termination will be propagated to all its children, covering the whole hierarchy.

Every reactor has a lifecycle divided into three phases: Init, Main, Stop. 

### Init Phase

On reactor creation, the ReActed automatically sends a `ReActorInit` message to the newborn. This message is always
the first message that will be received by any reactor. The behavior associated with the `ReActorInit` message
can be specified within the reactor's reactions.

### Main Phase

Once that a reactor has been inited, the main phase begins. In this phase the reactions for any message that is found
in the mailbox are sequentially called. 

### Stop Phase

In any of the above phases, a reactor can be requested to stop. The stop request is *dispatched* immediately,
without processing any further message present in the mailbox. On termination `ReActorStop` message is processed by the 
terminating reactor, so the appropriate behavior can be customized providing the appropriate reaction in the reactor's
configuration 

>NOTE: A reactor termination has a top down behavior: once a reactor has been terminated, all its children will be
>recursively terminated as well. The ratio behind this is that a child can be terminated when there is nothing waiting 
>for it, otherwise we could kill something required by a still operating father reactor. *The termination will be 
>considered and marked as completed when the whole hierarchy has been terminated*.  

## Creating a ReActor

Let's see the first example of a reactor.
The below reactor prints a statement every time a message is processed.  Every time a message is received, we check
if the time is a multiple of the already received messages num and in that case we request a termination of the
reactor. Once the termination is completed, a line is printed containing the name of the reactor that was just terminated.

```java
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.time.Instant;

public class FirstReActor implements ReActor {
```
Any class can become a reactor simply implementing the `ReActor` interface. 
Every ReActor is defined by some `ReActions` and a `ReActorConfig`. 

```java
    private static final Logger LOGGER = LoggerFactory.getLogger(FirstReActor.class);
    private int receivedMsgs;

    @NotNull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onReActorInit)
                        .reAct(ReActorStop.class, this::onReActorStop)
                        .reAct(this::onAnyOtherMessageType)
```
Defining a reaction for a given **message type** means
defining which behavior should be used when a *message* of that type is executed.
It's possible to specify reactions for a specific message type or providing a wildcard reaction. 
Pattern matching for message types are done on the exact type.
```java
                        .build();
    }

    @NotNull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(FirstReActor.class.getSimpleName())
```
It's mandatory *naming* a reactor. We can have default values for the other configuration properties, but
a reactor always needs a unique name among its alive siblings. If interested in using the [replay](replay.md) feature,
a reactor name should always be non randomic

```java
                            .build();
    }
    private void onReActorInit(ReActorContext raCtx, ReActorInit init) {
        raCtx.logInfo("Init received");
    }

    private void onReActorStop(ReActorContext raCtx, ReActorStop stop) {
        LOGGER.debug("Termination requested after {} messages", receivedMsgs);
    }

    private void onAnyOtherMessageType(ReActorContext raCtx, Serializable message) {
```
Messages **must** implement the `Serializable` interface. This because the communication between reactors is location
and technology agnostic, so a message can always virtually go over some other media than shared memory

```java

        this.receivedMsgs++;
```
No synchronization primitives are required to ensure the correct updated of the counter, regardless of how many threads 
are sending messages as the same time
```java
        System.out.printf("Received message type %s%n", message.getClass().getSimpleName());
        if (Instant.now().toEpochMilli() % this.receivedMsgs == 0) {
            raCtx.stop()
```
`ReActorContext` provides a set of method useful to control the reactor behavior and to access its internal properties
```java
                 .toCompletableFuture()
```
We can attach an async operation to be executed after that the whole hierarchy has been terminated chaining to the
completion stage returned by the stop operation
```java
                 .thenAcceptAsync(noVal -> raCtx.logInfo("ReActor {} hierarchy is terminated",
                                                          raCtx.getSelf().getReActorId()
                                                                         .getReActorName()));
        }
    }
}
```
ReActed offers a [centralized logging system](centralized_logger.md)

## ReActors communication

ReActors talk to each other through [messaging](messaging.md). Let's create now a couple of reactors that talk to each
other performing a simple ping pong. 

```java
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public class PingPongExample {
    public static void main(String[] args) throws InterruptedException {
        ReActorSystem exampleReActorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                                  .setReactorSystemName("ExampleSystem")
                                                                                  .build()).initReActorSystem();
```
ReActors exist within a [ReActorSystem](reactor_system.md). As JVM is the runtime environment for a java program,
a `ReActorSystem` is the runtime environment for a reactor.
```java

        ReActorRef pongReactor = exampleReActorSystem.spawn(ReActions.newBuilder()
                                                                     .reAct(String.class,
                                                                            PingPongExample::onPing)
                                                                     .reAct(ReActions::noReAction)
                                                                     .build(),
```
In this example we did not use a *class* as a reactor, instead we provided on once case just the behaviors and 
the config for the reactor. The effect of using `ReActions::noReAction` as argument for the wildcard reaction, 
is to silently  ignore all the messages but the ones for which has been specified an explicit reaction. 
In the above example it meansthat the `ReActorInit` and the `ReActorStop` messages will be silently ignored.
Inside a reaction we saw that we can interact with a reactor through the `ReActorContext` object that is always passed
as an argument. Outside the scope of a reaction, we can do that sending a **message**. 
It's possible to [send a message](messaging.md) to a reactor using do that its  `ReActorRef`. 
A `ReActorRef` is a **location and technology agnostic** reference that uniquely address a reactor across a ReActed [cluster](clustering.md).

```java
                                                                   ReActorConfig.newBuilder()
                                                                                .setReActorName("Pong")
                                                                                .build())
                                                     //To be more compact and less redundant we could simply flatMap here
                                                     .peekFailure(error -> exampleReActorSystem.shutDown())
                                                     .orElseSneakyThrow();
```
Through a `ReActorSystem` we can **spawn** a new reactor.
Once a reactor has been *spawned* a `ReActorInit` message will be immediately delivered to trigger the *Init* phase.
```java

        exampleReActorSystem.spawn(new ReActiveEntity() {
```
Instead of providing a full `ReActor` or just a context free set of reactions, we can use the `ReActiveEntity` interface
to inline reactions and their context 
```java
                                            private long pingNum = 0;
                                            @Nonnull
                                            @Override
                                            public ReActions getReActions() {
                                                return ReActions.newBuilder()
                                                                .reAct(ReActorInit.class, 
                                                                       ((ractx, init) -> pongReactor.tell(ractx.getSelf(),
                                                                                                          "FirstPing")))
```
The `Ping` reactor has a specific `ReActorInit` reaction. It takes `Pong`'s `ReActorRef` and `tell`s the first message.
This will initiate a ping-pong of messages between the two reactors for some milliseconds and after that we [shutdown](reactor_system.md#Shutdown) the
`ReActorSystem`.
```java
                                                                .reAct(String.class, 
                                                                       (raCtx, pongPayload) -> onPong(raCtx, pongPayload, 
                                                                                                      ++pingNum))
                                                                .reAct(ReActions::noReAction)
                                                                .build();
                                                
                                            };
                                          },
                                          ReActorConfig.newBuilder()
                                                       .setReActorName("Ping")
                                                       .build())
                            .peekFailure(error -> exampleReActorSystem.shutDown())
                            .orElseSneakyThrow();

        TimeUnit.MILLISECONDS.sleep(3);
        exampleReActorSystem.shutDown();
    }

    private static void onPing(ReActorContext raCtx, String pingMessage) {
        raCtx.getSender().tell(raCtx.getSelf(), "Pong " + pingMessage)
```
With `getSender()` obtain the `ReActorRef` of the reactor that sent the current message and we [tell](messaging.md#tell)
another message to reply. `tell` returns a completion stage that is going to be completed with the outcome of the operation.
It's not mandatory providing a check for the outcome of the operation, it highly depends on the logic of the application.
ReActed guarantees that no message can be lost on `tell`, but this topic is covered in [messaging](messaging.md) and
[drivers](drivers.md) chapters.
```java
                         .toCompletableFuture()
                         .thenAcceptAsync(deliveryStatus -> deliveryStatus.filter(DeliveryStatus::isDelivered)
                                                                          .ifError(deliveryError -> raCtx.logError("Delivery Error",
                                                                                                                   deliveryError)));
    }

    private static void onPong(ReActorContext raCtx, String pongMessage, long pingId) {
        raCtx.logInfo("{} from reactor {}", pongMessage, raCtx.getSender().getReActorId().getReActorName());
        raCtx.reply("PingRequest " + pingId);
    }
}
```
`reply` is a shortcut for `raCtx.getSender().tell(raCtx.getSelf(), ...` It is used to `tell` a message to the *sender*
of the last message received setting the current reactor as source of the reply.
In the output we can see 
```text
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-3] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Pong FirstPing from reactor Pong
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-3] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Pong PingRequest 1 from reactor Pong
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-2] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Pong PingRequest 2 from reactor Pong
```

## ReActors Hierarchies

Let's see a toy example involving hierarchies. In the following example we have `Uncle` who asks the `Father` to
create a variable number of children. Once created a `Child` greets the `Uncle` who issued the breed request.
When `Uncle` receives a greeting, it sends a `ThankYou` to `Father` for its job. After that `Father` has been thanked
for once per `Child` created, it kills itself and all its children and then a last request is asynchronously sent to
`Uncle` to allow it to terminate itself.

```java
import io.reacted.core.config.ConfigUtils;
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
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

public class FamilyExample {
    public static void main(String[] args) {

        ReActorSystem exampleReActorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                                  .setReactorSystemName("ExampleSystem")
                                                                                  .build()).initReActorSystem();
        try {
            var father = exampleReActorSystem.spawn(new Father(), ReActorConfig.newBuilder()
                                                                               .setReActorName("Father")
                                                                               .build()).orElseSneakyThrow();
            var uncle = exampleReActorSystem.spawn(new Uncle(), ReActorConfig.newBuilder()
                                                                             .setReActorName("Uncle")
                                                                             .build()).orElseSneakyThrow();
            father.tell(uncle, new BreedRequest(3));
```
A `BreedRequest` is sent to father and a `ReActorRef` to `Uncle` is used to set the source (*sender*) of that message
```java
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
                                            .reAct(ReActorStop.class, FamilyExample::onStop)
                                            .reAct(ReActions::noReAction)
                                            .build();
        }

        @Override
        public ReActions getReActions() { return fatherReactions; }

        private void onBreedRequest(ReActorContext raCtx, BreedRequest breedRequest) {
            this.requestedChildren = breedRequest.getRequestedChildren();

            raCtx.logInfo("{} received a {} for {} from {}",
                          raCtx.getSelf().getReActorId().getReActorName(),
                          breedRequest.getClass().getSimpleName(), breedRequest.getRequestedChildren(),
                          raCtx.getSender().getReActorId().getReActorName());

            LongStream.range(0, breedRequest.getRequestedChildren())
                      .forEachOrdered(childNum -> raCtx.spawnChild(new Child(childNum, raCtx.getSender())));
```
Here we *spawn* the requested number of children. The `ReActorRef` to the sender of the last message received by the
ReActor is passed as argument. In this case, we use the sender of the `BreedRequest`. Since `Uncle` has been used
as sender, all the children will receive a reference to him

```java
        }

        private void onThankYou(ReActorContext raCtx, ThankYouFather thanks) {
            if (--this.requestedChildren == 0) {
```
When the expected number of `ThankYou` is received by uncle, a termination for the `Father` reactor and all its children
is triggered. When all the hierarchy has been terminated, a message is asynchronously sent to `Uncle` to trigger its
demise 
```java
                raCtx.stop().thenAcceptAsync(voidVal -> raCtx.reply(ReActorRef.NO_REACTOR_REF, new ByeByeUncle()));
```
`ReActorRef.NO_REACTOR_REF` is a constant that represents an invalid `ReActorRef`. Every attempt to send a message to
that reference ends up in a failure, but it can used as source of a message when no reply is required and should not be
sent
```java
            }
        }
    }

    @NonNullByDefault
    private static class Uncle implements ReActiveEntity {
        private static final ReActions UNCLE_REACTIONS = ReActions.newBuilder()
                                                                  .reAct(Greetings.class, Uncle::onGreetingsFromChild)
                                                                  .reAct(ByeByeUncle.class, Uncle::onByeByeUncle)
                                                                  .reAct(ReActorStop.class, FamilyExample::onStop)
                                                                  .reAct(ReActions::noReAction)
                                                                  .build();
        @Override
        public ReActions getReActions() { return UNCLE_REACTIONS; }

        private static void onGreetingsFromChild(ReActorContext raCtx, Greetings greetingsMessage) {
            raCtx.logInfo("{} received {}. Sending thank you to {}",
                          raCtx.getSelf().getReActorId().getReActorName(), greetingsMessage.getGreetingsMessage(), 
                          raCtx.getSender().getReActorId().getReActorName());
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

        @Override
        public ReActorConfig getConfig() { return childConfig; }

        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, this::onInit)
                            .reAct(ReActorStop.class, FamilyExample::onStop)
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext raCtx, ReActorInit init) {
            this.breedRequester.tell(raCtx.getParent(),
                                     new Greetings("Hello from " + childConfig.getReActorName()));
        }
    }

    @Immutable
    private static final class BreedRequest implements Serializable {
        private final long requestedChildren;
        private BreedRequest(long requestedChildren) {
            this.requestedChildren = ConfigUtils.requiredInRange(requestedChildren, 1L, Long.MAX_VALUE,
                                                                 IllegalArgumentException::new);
        }
        private long getRequestedChildren() { return requestedChildren; }
    }

    @NonNullByDefault
    @Immutable
    private static final class Greetings implements Serializable {
        private final String greetingsMessage;
        private Greetings(String greetingsMessage) { this.greetingsMessage = greetingsMessage; }
        private String getGreetingsMessage() { return greetingsMessage; }
    }

    @Immutable
    private static final class ThankYouFather implements Serializable { private ThankYouFather() { } }

    @Immutable
    private static final class ByeByeUncle implements Serializable { private ByeByeUncle() { } }
}
```
In the output below all the message pattern can be seen. It can also be noticed the termination pattern: top down 
(from father towards the descendants) and only when all the hierarchy has been terminated, the termination process is
marked as complete
```text
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-2] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Father received a BreedRequest for 3 from Uncle
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-3] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Uncle received Hello from Child-1. Sending thank you to Father
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-1] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Uncle received Hello from Child-0. Sending thank you to Father
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-3] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Uncle received Hello from Child-2. Sending thank you to Father
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-0] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Father is terminating
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-0] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Child-0 is terminating
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-1] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Child-1 is terminating
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-2] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Child-2 is terminating
[ReActed-Dispatcher-Thread-ReactorSystemDispatcher-1] INFO io.reacted.core.reactors.systemreactors.SystemLogger - Uncle is terminating
```






