/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.serviceregistries;

import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public enum ServiceRegistryConfigKey {
    CHANNEL_ID("CHANNEL_ID"),
    CHANNEL_CONFIG_DATA("CHANNEL_CONFIG_DATA");
    private final String keyValue;
    ServiceRegistryConfigKey(String configKey) {
        this.keyValue = configKey;
    }
    public String getKeyValue() { return keyValue; }
}
