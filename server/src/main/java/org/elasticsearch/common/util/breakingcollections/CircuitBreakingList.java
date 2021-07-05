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

import java.util.*;

/**
 * A wrapper around a List that updates a CircuitBreaker, every time the list's size changes.
 * This uses ArrayList internally.
 */
public class CircuitBreakingList<E> extends CircuitBreakingCollection<E> implements List<E> {

    List<E> list;
    private static final int DEFAULT_CAPACITY = -1;
    int capacity = DEFAULT_CAPACITY;
    private long perElementSize = -1;

    public CircuitBreakingList(CircuitBreaker circuitBreaker) {
        super(circuitBreaker, new ArrayList<>());
        list = (List<E>) super.collection;
        updateBreaker();
    }

    public CircuitBreakingList(CircuitBreaker circuitBreaker, int initialCapacity) {
        super(circuitBreaker, new ArrayList<>(initialCapacity));
        list = (List<E>) super.collection;
        capacity = initialCapacity;
        updateBreaker();
    }

    public CircuitBreakingList(CircuitBreaker circuitBreaker, Collection<? extends E> collection) {
        this(circuitBreaker, collection.size());
        addAll(collection);
        updateBreaker();
    }

    @Override
    protected void resizeIfRequired() {
        while (size() > capacity) {
            // Copy pasted from ArrayList.java(JBR - 11) so that capacity grows same as ArrayList's internal capacity
            int minCapacity = size();
            if (capacity == DEFAULT_CAPACITY) {
                capacity = Math.max(10, minCapacity);
            } else {
                int newCapacity = capacity + (capacity >> 1);
                if (newCapacity - minCapacity <= 0) {
                    capacity = minCapacity;
                } else {
                    capacity = newCapacity - 2147483639 <= 0 ? newCapacity : (minCapacity > 2147483639 ? 2147483647 : 2147483639);
                }
            }
        }
    }

    @Override
    protected long bytesRequired() {
        if (perElementSize == -1) {
            calculatePerElementSize();
        }
        return RamUsageEstimator.shallowSizeOf(list) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + capacity * perElementSize;
    }

    private void calculatePerElementSize() {
        assert this.size() > 0 : "Size should have changed from 0";
        perElementSize = RamUsageEstimator.sizeOfObject(collection.toArray()[0], 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    public void shrinkReservationToSize() {
        if (list instanceof ArrayList) {
            ((ArrayList<E>) list).trimToSize();
            capacity = size();
            updateBreaker();
        } else {
            throw new UnsupportedOperationException("Can not shrink internal list");
        }
    }

    @Override
    public boolean addAll(int i, Collection<? extends E> collection) {
        try {
            return list.addAll(i, collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public E get(int i) {
        return list.get(i);
    }

    @Override
    public E set(int i, E e) {
        return list.set(i, e);
    }

    @Override
    public void add(int i, E e) {
        try {
            list.add(i, e);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public E remove(int i) {
        try {
            return list.remove(i);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return list.listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int i) {
        return list.listIterator(i);
    }

    @Override
    public List<E> subList(int i, int i1) {
        return list.subList(i, i1);
    }
}
