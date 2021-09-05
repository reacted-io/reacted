/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.flow.operators.reduce.ReducingOperator;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class ZipOperator extends ReducingOperator<Builder, ZipOperatorConfig> {
    protected ZipOperator(ZipOperatorConfig config) {
        super(config);
    }
}
