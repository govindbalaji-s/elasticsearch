/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.breakingcollections;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

public class CircuitBreakingTreeMap<K, V> extends CircuitBreakingMap<K, V> {
    private long imaginaryCapacity;
    private long perElementSize = -1;

    public CircuitBreakingTreeMap(CircuitBreaker circuitBreaker) {
        super(circuitBreaker, new TreeMap<>());
        updateBreaker();
    }

    public CircuitBreakingTreeMap(CircuitBreaker circuitBreaker, Comparator<? super K> comparator) {
        super(circuitBreaker, new TreeMap<>(comparator));
        updateBreaker();
    }

    public CircuitBreakingTreeMap(CircuitBreaker circuitBreaker, Map<? extends K, ? extends V> m) {
        super(circuitBreaker, new TreeMap<>(m));
        updateBreaker();
    }

    public CircuitBreakingTreeMap(CircuitBreaker circuitBreaker, SortedMap<K, ? extends V> m) {
        super(circuitBreaker, new TreeMap<>(m));
        updateBreaker();
    }

    @Override
    protected void resizeIfRequired() {
        while (size() > imaginaryCapacity) {
            if(imaginaryCapacity == 0) {
                imaginaryCapacity = Math.max(10, size());
            } else {
                imaginaryCapacity += imaginaryCapacity >> 1;
            }
        }
    }

    @Override
    protected long bytesRequired() {
        if (perElementSize == -1) {
            calculatePerElementSizes();
        }
        return RamUsageEstimator.shallowSizeOf(map) + imaginaryCapacity * perElementSize;
    }

    protected void calculatePerElementSizes() {
        Optional<Entry<K, V>> optionalEntry = map.entrySet().stream().findAny();
        assert this.size() > 0 && optionalEntry.isPresent(): "Size should have changed from 0";
        Entry<K, V> entry = optionalEntry.get();

        perElementSize = RamUsageEstimator.shallowSizeOf(entry) + RamUsageEstimator.sizeOfObject(entry.getKey(), 0)
            + RamUsageEstimator.sizeOfObject(entry.getValue(), 0);
    }
}
