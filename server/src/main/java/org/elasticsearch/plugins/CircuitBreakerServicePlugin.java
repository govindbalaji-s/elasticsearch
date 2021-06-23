/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 * Plugins that need a hook of the HierarchyCircuitBreakerService
 */
public interface CircuitBreakerServicePlugin {

    /**
     * When HierarchyCircuitBreakerService is initialized, this method is invoked.
     */
    default void setCircuitBreakerService(CircuitBreakerService circuitBreakerService) {
    }

    /**
     * When BigArrays is initialized, this method is invoked
     *
     * @param bigArrays the circuit breaking instance of BigArrays
     */
    default void setBigArrays(BigArrays bigArrays) {
    }
}
