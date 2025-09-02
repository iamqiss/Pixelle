/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.density.ExceptionsHelper;
import org.density.action.ActionListenerResponseHandler;
import org.density.action.support.RetryableAction;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.unit.TimeValue;
import org.density.common.util.CancellableThreads;
import org.density.common.util.concurrent.ConcurrentCollections;
import org.density.core.action.ActionListener;
import org.density.core.common.breaker.CircuitBreakingException;
import org.density.core.common.io.stream.Writeable;
import org.density.core.concurrency.DensityRejectedExecutionException;
import org.density.core.transport.TransportResponse;
import org.density.threadpool.ThreadPool;
import org.density.transport.ConnectTransportException;
import org.density.transport.RemoteTransportException;
import org.density.transport.SendRequestTransportException;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestOptions;
import org.density.transport.TransportService;

import java.util.Map;

/**
 * Client that implements retry functionality for transport layer requests.
 *
 * @density.internal
 */
public final class RetryableTransportClient {

    private final ThreadPool threadPool;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private volatile boolean isCancelled = false;
    private final TransportService transportService;
    private final TimeValue retryTimeout;
    private final DiscoveryNode targetNode;

    private final Logger logger;

    public RetryableTransportClient(TransportService transportService, DiscoveryNode targetNode, TimeValue retryTimeout, Logger logger) {
        this.threadPool = transportService.getThreadPool();
        this.transportService = transportService;
        this.retryTimeout = retryTimeout;
        this.targetNode = targetNode;
        this.logger = logger;
    }

    /**
     * Execute a retryable action.
     * @param action {@link String} Action Name.
     * @param request {@link TransportRequest} Transport request to execute.
     * @param actionListener {@link ActionListener} Listener to complete
     * @param reader {@link Writeable.Reader} Reader to read the response stream.
     * @param <T> {@link TransportResponse} type.
     */
    public <T extends TransportResponse> void executeRetryableAction(
        String action,
        TransportRequest request,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(retryTimeout).build();
        executeRetryableAction(action, request, options, actionListener, reader);
    }

    public <T extends TransportResponse> void executeRetryableAction(
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final RetryableAction<T> retryableAction = new RetryableAction<T>(logger, threadPool, initialDelay, retryTimeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(
                    targetNode,
                    action,
                    request,
                    options,
                    new ActionListenerResponseHandler<>(listener, reader, ThreadPool.Names.GENERIC)
                );
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("retryable action was cancelled"));
        }
    }

    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("retryable action was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException || cause instanceof DensityRejectedExecutionException;
        }
        return false;
    }
}
