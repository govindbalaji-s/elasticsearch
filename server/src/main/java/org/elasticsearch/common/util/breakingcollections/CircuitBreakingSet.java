/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.breakingcollections;

import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Collection;
import java.util.Set;

public abstract class CircuitBreakingSet<E> extends CircuitBreakingCollection<E> implements Set<E> {
    Set<E> set;
    public CircuitBreakingSet(CircuitBreaker circuitBreaker, Set<E> set) {
        super(circuitBreaker, set);
        this.set = set;
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return set.toArray(ts);
    }

    @Override
    public boolean add(E o) {
        try {
            return set.add(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        try {
            return set.addAll(collection);
        } finally {
            updateBreaker();
        }
    }
}
