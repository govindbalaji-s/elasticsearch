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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

public abstract class CircuitBreakingCollection<E> implements Collection<E>, Releasable {
    private final CircuitBreaker circuitBreaker;
    protected final Collection<E> collection;
    private long requestBytesAdded = 0;
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingCollection(CircuitBreaker circuitBreaker, Collection<E> collection) {
        this.circuitBreaker = circuitBreaker;
        this.collection = collection;
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
    public int size() {
        return collection.size();
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collection.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return collection.iterator();
    }

    @Override
    public Object[] toArray() {
        return collection.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return collection.toArray(ts);
    }

    @Override
    public boolean add(E e) {
        try {
            return collection.add(e);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean remove(Object o) {
        try {
            return collection.remove(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return this.collection.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        try {
            return this.collection.addAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        try {
            return this.collection.removeAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        try {
            return collection.removeIf(filter);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        try {
            return this.collection.retainAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void clear() {
        try {
            collection.clear();
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakingCollection<?> that = (CircuitBreakingCollection<?>) o;
        return Objects.equals(collection, that.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collection);
    }

    @Override
    public void close() {
        clear();
        updateBreaker(0);
    }
}
