/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.common.CheckedSupplier;
import org.density.common.lease.Releasable;
import org.density.common.lease.Releasables;
import org.density.common.network.CloseableChannel;
import org.density.common.transport.NetworkExceptionHelper;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.NotifyOnceListener;
import org.density.core.common.bytes.BytesReference;
import org.density.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Outbound data handler
 *
 * @density.internal
 */
public final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;

    public OutboundHandler(StatsTracker statsTracker, ThreadPool threadPool) {
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        SendContext sendContext = new SendContext(statsTracker, channel, () -> bytes, listener);
        try {
            sendBytes(channel, sendContext);
        } catch (IOException e) {
            // This should not happen as the bytes are already serialized
            throw new AssertionError(e);
        }
    }

    public void sendBytes(long requestId, TcpChannel channel, SendContext sendContext) throws IOException {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        BytesReference reference = sendContext.get();
        // stash thread context so that channel event loop is not polluted by thread context
        try (ThreadContext.StoredContext existing = threadPool.getThreadContext().stashContext()) {
            if (requestId == -1) {
                channel.sendMessage(reference, sendContext);
            } else {
                channel.sendMessage(requestId, reference, sendContext);
            }
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }

    }

    public void sendBytes(TcpChannel channel, SendContext sendContext) throws IOException {
        sendBytes(-1, channel, sendContext);
    }

    /**
     * Internal message serializer
     *
     * @density.internal
     */
    public static class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {
        private final StatsTracker statsTracker;
        private final TcpChannel channel;
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private long messageSize = -1;

        SendContext(
            StatsTracker statsTracker,
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener
        ) {
            this(statsTracker, channel, messageSupplier, listener, null);
        }

        public SendContext(
            StatsTracker statsTracker,
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener,
            Releasable optionalReleasable
        ) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
            this.statsTracker = statsTracker;
        }

        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                message = messageSupplier.get();
                messageSize = message.length();
                TransportLogger.logOutboundMessage(channel, message);
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            statsTracker.markBytesWritten(messageSize);
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            if (NetworkExceptionHelper.isCloseConnectionException(e)) {
                logger.debug(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            }
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}
