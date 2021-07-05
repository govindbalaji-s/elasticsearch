/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.breakingcollections;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

public abstract class CircuitBreakingMap<K, V> implements Map<K, V>, Releasable {
    private final CircuitBreaker circuitBreaker;
    protected final Map<K, V> map;
    private long requestBytesAdded = 0;
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingMap(CircuitBreaker circuitBreaker, Map<K, V> map) {
        this.circuitBreaker = circuitBreaker;
        this.map = map;
    }

    protected void addToBreaker(long bytes, boolean checkBreaker) {
        // Since this method is called after collection already grew, update reservedSize even if breaking.
        this.requestBytesAdded += bytes;
        if (bytes >= 0 && checkBreaker) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<CircuitBreakingCollection>");
        } else {
            circuitBreaker.addWithoutBreaking(bytes);
        }
    }

    protected abstract void resizeIfRequired();
    protected abstract long bytesRequired();

    protected void updateBreaker() {
        resizeIfRequired();
        updateBreaker(bytesRequired());
    }

    protected void updateBreaker(long bytesRequired) {
        long bytesDiff = bytesRequired - requestBytesAdded;
        if (bytesDiff == 0) {
            return;
        }
        // If it breaks, then the already created data will not be accounted for.
        // So we first add without breaking, and then check.
        addToBreaker(bytesDiff, false);
        addToBreaker(0, true);
    }

    @Override
    public void close() {
        map.clear();
        updateBreaker(0);
    }
    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return map.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return map.get(o);
    }

    @Override
    public V put(K k, V v) {
        try {
            return map.put(k, v);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public V remove(Object o) {
        try {
            return map.remove(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        try {
            this.map.putAll(map);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void clear() {
        try {
            map.clear();
        } finally {
            updateBreaker();
        }
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakingMap<?, ?> that = (CircuitBreakingMap<?, ?>) o;
        return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }
}
