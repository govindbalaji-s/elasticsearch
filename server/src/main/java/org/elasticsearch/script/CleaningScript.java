/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.lang.ref.Cleaner;
import java.security.AccessController;
import java.security.PrivilegedAction;

public interface CleaningScript {
    Cleaner cleaner = AccessController.doPrivileged((PrivilegedAction<Cleaner>) Cleaner::create);


    default Cleaner.Cleanable registerCleaningAction(Runnable cleaningAction) {
        return cleaner.register(this, cleaningAction);
    }

    default Cleaner.Cleanable releaseOnClean(Releasable releasable) {
        return cleaner.register(this, () -> {
            Releasables.close(releasable);
        });
    }
}
