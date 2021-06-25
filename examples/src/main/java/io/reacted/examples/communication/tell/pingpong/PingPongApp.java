/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.pingpong;

import io.reacted.examples.ExampleUtils;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

class PingPongApp {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        var reactorSystem = ExampleUtils.getDefaultInitedReActorSystem(PingPongApp.class.getSimpleName());

        var pongReActor = reactorSystem.spawn(new PongReActor());
        pongReActor.map(PingReActor::new)
                   .ifSuccess(reactorSystem::spawn);
        //The reactors are executing now
        TimeUnit.SECONDS.sleep(10);
        //The game is finished, shut down
        reactorSystem.shutDown();
    }
}
