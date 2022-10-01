/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.ping;

import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.examples.ExampleUtils;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

class SendMessagesApp {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        //ReActor system configuration
        var simpleReActorSystem = ExampleUtils.getDefaultInitedReActorSystem(SendMessagesApp.class.getSimpleName());

        var pingDelim = ":";
        int messagesToSend = 20;
        //Body/state of the reactor
        var newReActorInstance = new SimpleTestReActor(pingDelim, messagesToSend);
        //Reference of the reactor within the ReActorSystem (cluster)
        var newReActorReference = simpleReActorSystem.spawn(newReActorInstance.getReActions(),
                                                            newReActorInstance.getConfig())
                                                     .orElse(ReActorRef.NO_REACTOR_REF, error -> {
                                                         error.printStackTrace();
                                                         simpleReActorSystem.shutDown();
                                                     });
        //Let's ping our new reactor
        if ( newReActorReference.tell(ReActorRef.NO_REACTOR_REF, new PreparationRequest())
                                .isDelivered()) {
          System.out.println("Preparation request has been delivered");
        } else {
          System.err.println("Error communicating with reactor");
        }

        IntStream.range(0, messagesToSend)
                 .parallel()
                 //Tell and atell are functionally the same if we do not care about the return value
                 //atell brings more overhead because of the acking system
                 .forEach(msgNum -> newReActorReference.atell("Ping Request" + pingDelim + msgNum));
        TimeUnit.SECONDS.sleep(1);
        simpleReActorSystem.shutDown();
    }
}
