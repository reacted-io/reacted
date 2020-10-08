/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.utils;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

@NonNullByDefault
public final class ConfigUtils {
    private ConfigUtils() { /* No instances allowed */ }
    public static Properties toProperties(InheritableBuilder<?, ?> config, Set<String> skipFields) {
        Properties cfgProperties = new Properties();
        Class<?> configLevel = Objects.requireNonNull(config).getClass();
        do {
            Field[] configFields = configLevel.getDeclaredFields();
            Arrays.stream(configFields)
                  .filter(Predicate.not(field -> Objects.requireNonNull(skipFields).contains(field.getName())))
                  .filter(Predicate.not(field -> Modifier.isStatic(field.getModifiers())))
                  .forEach(field -> {
                      field.setAccessible(true);
                      Optional.ofNullable(UnChecked.function(field::get).apply(config))
                              .map(Object::toString)
                              .ifPresent(fieldValue -> cfgProperties.setProperty(field.getName(),
                                                                                 fieldValue));
                  });
            configLevel = configLevel.getSuperclass();
        } while (configLevel != Object.class);
        return cfgProperties;
    }
}