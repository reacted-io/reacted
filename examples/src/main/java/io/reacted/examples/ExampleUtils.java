/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples;

import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.grpc.GrpcDriverConfig;
import io.reacted.patterns.NonNullByDefault;

import java.util.Collection;
import java.util.List;

@NonNullByDefault
public final class ExampleUtils {
    public static final Collection<ServiceRegistryDriver<?, ?>> NO_SERVICE_REGISTRIES = List.of();
    public static final Collection<RemotingDriver> NO_REMOTING_DRIVERS = List.of();

    private ExampleUtils() {
    }

    public static ReActorSystem getDefaultInitedReActorSystem(String reActorSystemName) {
        return new ReActorSystem(getDefaultReActorSystemCfg(reActorSystemName)).initReActorSystem();
    }

    public static ReActorSystemConfig getDefaultReActorSystemCfg(String reActorSystemName) {
        return getDefaultReActorSystemCfg(reActorSystemName, SystemLocalDrivers.DIRECT_COMMUNICATION,
                                          NO_SERVICE_REGISTRIES, NO_REMOTING_DRIVERS);
    }

    public static ReActorSystemConfig getDefaultReActorSystemCfg(String reActorSystemName, LocalDriver localDriver,
                                                                 Collection<ServiceRegistryDriver<?, ?>> serviceRegistryDrivers,
                                                                 Collection<RemotingDriver> remotingDrivers) {
        var configBuilder = ReActorSystemConfig.newBuilder()
                                  //Tunable parameter for purging java timers from
                                  // canceled tasks
                                  //How messages are delivered within a Reactor System
                                  .setLocalDriver(localDriver)
                                  //Fan out pool to message type subscribers
                                  .setMsgFanOutPoolSize(1)
                                  //Generate extra informations for replaying if required
                                  .setRecordExecution(true)
                                  .setReactorSystemName(reActorSystemName);
       serviceRegistryDrivers.forEach(configBuilder::addServiceRegistryDriver);
       remotingDrivers.forEach(configBuilder::addRemotingDriver);
       return  configBuilder.build();
    }

    public static GrpcDriverConfig getGrpcDriverCfg(int gatePort) {
        return GrpcDriverConfig.newBuilder()
                               .setHostName("localhost")
                               .setPort(gatePort)
                               .setChannelName("TestGrpcChannel")
                               .setChannelRequiresDeliveryAck(false)
                               .build();
    }
}
