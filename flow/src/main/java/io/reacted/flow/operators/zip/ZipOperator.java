/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.reduce.ReducingOperator;
import io.reacted.flow.operators.reduce.ReducingOperatorConfig;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZipOperator extends ReducingOperator<Builder, ZipOperatorConfig> {
    protected ZipOperator(ZipOperatorConfig config) {
        super(config);
    }
}
