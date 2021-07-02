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

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

public class CircuitBreakingHashMap<K,V> extends CircuitBreakingMap<K, V> {
    private long reservedSize = 0;
    private long reservedThreshold = 0;
    private long perElementSize = -1;
    private long perElementObjectSize = -1;
    protected int capacity;
    protected int threshold;
    protected float loadFactor = DEFAULT_LOAD_FACTOR;
    /**
     * Copied from HashMap Coretto-1.8.0_292
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingHashMap(CircuitBreaker circuitBreaker) {
        super(circuitBreaker, new HashMap<>());
    }

    public CircuitBreakingHashMap(CircuitBreaker circuitBreaker, int initialCapacity, float loadFactor) {
        super(circuitBreaker, new HashMap<>(initialCapacity, loadFactor));
        // Copied from HashMap Coretto-1.8.0_292
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                loadFactor);
        this.loadFactor = loadFactor;
        this.threshold = tableSizeFor(initialCapacity);
    }

    public CircuitBreakingHashMap(CircuitBreaker circuitBreaker, int initialCapacity) {
        this(circuitBreaker, initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Returns a power of two size for the given target capacity.
     */
    static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    @Override
    protected void resizeIfRequired() {
        if(size() > threshold) {
            // Copy pasted from HashMap
            int newCapacity, newThreshold = 0;
            if (capacity > 0) {
                if (capacity >= MAXIMUM_CAPACITY) {
                    threshold = Integer.MAX_VALUE;
                    return;
                } else if ((newCapacity = capacity << 1) < MAXIMUM_CAPACITY && capacity >= DEFAULT_INITIAL_CAPACITY) {
                    newThreshold = threshold << 1; // double threshold
                }
            } else if (threshold > 0) {// initial capacity was placed in threshold
                newCapacity = threshold;
            } else {               // zero initial threshold signifies using defaults
                newCapacity = DEFAULT_INITIAL_CAPACITY;
                newThreshold = (int) (DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
            }

            if (newThreshold == 0) {
                float ft = (float) newCapacity * loadFactor;
                newThreshold = (newCapacity < MAXIMUM_CAPACITY && ft < (float) MAXIMUM_CAPACITY ?
                    (int) ft : Integer.MAX_VALUE);
            }
            this.threshold = newThreshold;
            this.capacity = newCapacity;
        }
    }

    @Override
    protected long bytesRequired() {
        if (perElementSize == -1) {
            calculatePerElementSizes();
        }
        return RamUsageEstimator.sizeOfObject(map) + capacity * perElementSize + threshold * perElementObjectSize;
    }

    protected void calculatePerElementSizes() {
        Optional<Entry<K, V>> optionalEntry = map.entrySet().stream().findAny();
        assert this.size() > 0 && optionalEntry.isPresent(): "Size should have changed from 0";
        Entry<K, V> entry = optionalEntry.get();
        // there will never be elements for capacity - threshold elements
        perElementSize = RamUsageEstimator.shallowSizeOf(entry);
        perElementObjectSize = RamUsageEstimator.sizeOfObject(entry.getKey(), 0)
            + RamUsageEstimator.sizeOfObject(entry.getValue(), 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CircuitBreakingHashMap<?, ?> that = (CircuitBreakingHashMap<?, ?>) o;
        return reservedSize == that.reservedSize && reservedThreshold == that.reservedThreshold && perElementSize == that.perElementSize && perElementObjectSize == that.perElementObjectSize && capacity == that.capacity && threshold == that.threshold && Float.compare(that.loadFactor, loadFactor) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), reservedSize, reservedThreshold, perElementSize, perElementObjectSize, capacity, threshold, loadFactor);
    }
}
