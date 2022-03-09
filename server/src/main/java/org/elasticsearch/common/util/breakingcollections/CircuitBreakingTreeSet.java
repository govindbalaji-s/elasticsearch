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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

public class CircuitBreakingTreeSet<E> extends CircuitBreakingSet<E>{

    private long imaginaryCapacity;
    private long perElementSize = 0;
    private long baseSize;

    protected CircuitBreakingTreeSet(CircuitBreaker circuitBreaker, TreeSet<E> treeSet) {
        super(circuitBreaker, treeSet);
        updateBreaker();
    }

    public CircuitBreakingTreeSet(CircuitBreaker circuitBreaker) {
        this(circuitBreaker, new TreeSet<>());
    }

    public CircuitBreakingTreeSet(CircuitBreaker circuitBreaker, Collection<? extends E> c) {
        this(circuitBreaker, new TreeSet<>(c));
    }

    public CircuitBreakingTreeSet(CircuitBreaker circuitBreaker, Comparator<? super E> comparator) {
        this(circuitBreaker, new TreeSet<>(comparator));
    }

    public CircuitBreakingTreeSet(CircuitBreaker circuitBreaker, SortedSet<E> s) {
        this(circuitBreaker, new TreeSet<>(s));
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
        if (perElementSize == 0) {
            calculatePerElementSizes();
            baseSize = RamUsageEstimator.shallowSizeOf(set);
        }
        return baseSize + imaginaryCapacity * perElementSize;
    }

    protected void calculatePerElementSizes() {
        if (size() == 0) {
            return;
        }
        Optional<E> optionalElement = set.stream().findAny();
        //estimate size of inner map in the hash set
        E element = optionalElement.get();
        Map<E, Object> innerMap = new HashMap<>();
        innerMap.put(element, new Object());

        Optional<Map.Entry<E, Object>> optionalEntry = innerMap.entrySet().stream().findAny();
        Map.Entry<E, Object> entry = optionalEntry.get();

        perElementSize = RamUsageEstimator.shallowSizeOf(entry) + RamUsageEstimator.sizeOfObject(entry.getKey(), 0)
            + RamUsageEstimator.sizeOfObject(entry.getValue(), 0);
    }
}
