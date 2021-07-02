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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class CBUtilsFactory {
    private final CircuitBreaker circuitBreaker;

    public CBUtilsFactory(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public <E> List<E> newArrayList() {
        return new CircuitBreakingList<>(circuitBreaker);
    }

    public <E> List<E> newArrayList(int initialCapacity) {
        return new CircuitBreakingList<>(circuitBreaker, initialCapacity);
    }

    public <E> List<E> newArrayList(Collection<? extends E> collection) {
        return new CircuitBreakingList<>(circuitBreaker, collection);
    }

    public <E> List<E> newFinalizingArrayList() {
        return new CircuitBreakingList<E>(circuitBreaker) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <E> List<E> newFinalizingArrayList(int initialCapacity) {
        return new CircuitBreakingList<E>(circuitBreaker, initialCapacity) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <E> List<E> newFinalizingArrayList(Collection<? extends E> collection) {
        return new CircuitBreakingList<E>(circuitBreaker, collection) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newHashMap() {
        return new CircuitBreakingHashMap<>(circuitBreaker);
    }

    public <K, V> Map<K, V> newHashMap(int initialCapacity, float loadFactor) {
        return new CircuitBreakingHashMap<>(circuitBreaker, initialCapacity, loadFactor);
    }

    public <K, V> Map<K, V> newHashMap(int initialCapacity) {
        return new CircuitBreakingHashMap<>(circuitBreaker, initialCapacity);
    }

    public <K, V> Map<K, V> newFinalizingHashMap() {
        return new CircuitBreakingHashMap<K, V>(circuitBreaker) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newFinalizingHashMap(int initialCapacity, float loadFactor) {
        return new CircuitBreakingHashMap<K, V>(circuitBreaker, initialCapacity, loadFactor) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newFinalizingHashMap(int initialCapacity) {
        return new CircuitBreakingHashMap<K, V>(circuitBreaker, initialCapacity) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newTreeMap() {
        return new CircuitBreakingTreeMap<>(circuitBreaker);
    }

    public <K, V> Map<K, V> newTreeMap(Comparator<? super K> comparator) {
        return new CircuitBreakingTreeMap<>(circuitBreaker, comparator);
    }

    public <K, V> Map<K, V> newTreeMap(Map<? extends K, ? extends V> m) {
        return new CircuitBreakingTreeMap<>(circuitBreaker, m);
    }

    public <K, V> Map<K, V> newTreeMap(SortedMap<K, ? extends V> m) {
        return new CircuitBreakingTreeMap<>(circuitBreaker, m);
    }

    public <K, V> Map<K, V> newFinalizingTreeMap() {
        return new CircuitBreakingTreeMap<K, V>(circuitBreaker) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newFinalizingTreeMap(Comparator<? super K> comparator) {
        return new CircuitBreakingTreeMap<K, V>(circuitBreaker, comparator) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newFinalizingTreeMap(Map<? extends K, ? extends V> m) {
        return new CircuitBreakingTreeMap<K, V>(circuitBreaker, m) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }

    public <K, V> Map<K, V> newFinalizingTreeMap(SortedMap<K, ? extends V> m) {
        return new CircuitBreakingTreeMap<K, V>(circuitBreaker, m) {
            @Override
            public void finalize() {
                this.close();
            }
        };
    }
}
