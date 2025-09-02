/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.translog.listener;

import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.test.DensityTestCase;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TranslogListenerTests extends DensityTestCase {

    public void testCompositeTranslogEventListener() {
        AtomicInteger onTranslogSyncInvoked = new AtomicInteger();
        AtomicInteger onTranslogRecoveryInvoked = new AtomicInteger();
        AtomicInteger onBeginTranslogRecoveryInvoked = new AtomicInteger();
        AtomicInteger onFailureInvoked = new AtomicInteger();

        TranslogEventListener listener = new TranslogEventListener() {
            @Override
            public void onAfterTranslogSync() {
                onTranslogSyncInvoked.incrementAndGet();
            }

            @Override
            public void onAfterTranslogRecovery() {
                onTranslogRecoveryInvoked.incrementAndGet();
            }

            @Override
            public void onBeginTranslogRecovery() {
                onBeginTranslogRecoveryInvoked.incrementAndGet();
            }

            @Override
            public void onFailure(String reason, Exception ex) {
                onFailureInvoked.incrementAndGet();
            }
        };

        final List<TranslogEventListener> translogEventListeners = new ArrayList<>(Arrays.asList(listener, listener));
        Collections.shuffle(translogEventListeners, random());
        TranslogEventListener compositeListener = new CompositeTranslogEventListener(
            translogEventListeners,
            new ShardId(new Index("indexName", "indexUuid"), 123)
        );
        compositeListener.onAfterTranslogRecovery();
        compositeListener.onAfterTranslogSync();
        compositeListener.onBeginTranslogRecovery();
        compositeListener.onFailure("reason", new RuntimeException("reason"));

        assertEquals(2, onBeginTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogSyncInvoked.get());
        assertEquals(2, onFailureInvoked.get());
    }

    public void testCompositeTranslogEventListenerOnExceptions() {
        AtomicInteger onTranslogSyncInvoked = new AtomicInteger();
        AtomicInteger onTranslogRecoveryInvoked = new AtomicInteger();
        AtomicInteger onBeginTranslogRecoveryInvoked = new AtomicInteger();
        AtomicInteger onFailureInvoked = new AtomicInteger();

        TranslogEventListener listener = new TranslogEventListener() {
            @Override
            public void onAfterTranslogSync() {
                onTranslogSyncInvoked.incrementAndGet();
            }

            @Override
            public void onAfterTranslogRecovery() {
                onTranslogRecoveryInvoked.incrementAndGet();
            }

            @Override
            public void onBeginTranslogRecovery() {
                onBeginTranslogRecoveryInvoked.incrementAndGet();
            }

            @Override
            public void onFailure(String reason, Exception ex) {
                onFailureInvoked.incrementAndGet();
            }
        };

        TranslogEventListener throwingListener = (TranslogEventListener) Proxy.newProxyInstance(
            TranslogEventListener.class.getClassLoader(),
            new Class[] { TranslogEventListener.class },
            (a, b, c) -> {
                throw new RuntimeException();
            }
        );

        final List<TranslogEventListener> translogEventListeners = new LinkedList<>(Arrays.asList(listener, throwingListener, listener));
        Collections.shuffle(translogEventListeners, random());
        TranslogEventListener compositeListener = new CompositeTranslogEventListener(
            translogEventListeners,
            new ShardId(new Index("indexName", "indexUuid"), 123)
        );
        expectThrows(RuntimeException.class, () -> compositeListener.onAfterTranslogRecovery());
        expectThrows(RuntimeException.class, () -> compositeListener.onAfterTranslogSync());
        expectThrows(RuntimeException.class, () -> compositeListener.onBeginTranslogRecovery());
        expectThrows(RuntimeException.class, () -> compositeListener.onFailure("reason", new RuntimeException("reason")));

        assertEquals(2, onBeginTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogSyncInvoked.get());
        assertEquals(2, onFailureInvoked.get());
    }
}
