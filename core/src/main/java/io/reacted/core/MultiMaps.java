/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core;

import io.reacted.patterns.NonNullByDefault;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

@NonNullByDefault
public class MultiMaps {
    public static class CopyOnWriteHashMapOfEnumMaps<KeyT,SubKeyT extends Enum<SubKeyT>, PayloadT> {
        private final Class<SubKeyT> subKeyType;
        private final Map<SubKeyT, List<PayloadT>> emptySubMap;
        private final ConcurrentHashMap<KeyT, Map<SubKeyT, List<PayloadT>>> multiMap;
        public CopyOnWriteHashMapOfEnumMaps(int initialCapacity, float loadFactor, Class<SubKeyT> subKeyType) {
            this.subKeyType = subKeyType;
            this.multiMap = new ConcurrentHashMap<>(initialCapacity, loadFactor);
            this.emptySubMap = getNewSubMap(subKeyType, CopyOnWriteArrayList::new);
        }

        private Map<SubKeyT, List<PayloadT>> getNewSubMap(Class<SubKeyT> keyType,
                                                          Supplier<List<PayloadT>> collisionList) {
            var newMap = new EnumMap<SubKeyT, List<PayloadT>>(keyType);
            for(SubKeyT enumValue : keyType.getEnumConstants()) {
                newMap.put(enumValue, collisionList.get());
            }
            return newMap;
        }

        public List<PayloadT> get(KeyT key, SubKeyT subKey) {
            return this.multiMap.getOrDefault(key, emptySubMap)
                                .get(subKey);
        }

        public Map<SubKeyT, List<PayloadT>> getKeyGroup(KeyT key) {
            return this.multiMap.getOrDefault(key, emptySubMap);
        }

        @SuppressWarnings("UnusedReturnValue")
        public boolean add(KeyT key, SubKeyT subKeyT, PayloadT payload) {
            return this.multiMap.computeIfAbsent(key, missingArg -> getNewSubMap(subKeyType, CopyOnWriteArrayList::new))
                                .get(subKeyT)
                                .add(payload);
        }

        @SuppressWarnings("UnusedReturnValue")
        public boolean remove(KeyT key, SubKeyT subKeyT, PayloadT payload) {
            return this.multiMap.getOrDefault(key, emptySubMap)
                                .get(subKeyT)
                                .remove(payload);
        }
    }
}
