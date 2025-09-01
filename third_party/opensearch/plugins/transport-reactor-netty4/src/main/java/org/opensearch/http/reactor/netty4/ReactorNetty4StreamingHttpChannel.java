/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.reactor.netty4;

import org.density.common.concurrent.CompletableContext;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.http.HttpChunk;
import org.density.http.HttpResponse;
import org.density.http.StreamingHttpChannel;
import org.density.transport.reactor.netty4.Netty4Utils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class ReactorNetty4StreamingHttpChannel implements StreamingHttpChannel {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final Publisher<HttpChunk> receiver;
    private final StreamingHttpContentSender sender;
    private volatile FluxSink<HttpChunk> producer;
    private volatile boolean lastChunkReceived = false;

    ReactorNetty4StreamingHttpChannel(HttpServerRequest request, HttpServerResponse response, StreamingHttpContentSender sender) {
        this.request = request;
        this.response = response;
        this.sender = sender;
        this.receiver = Flux.create(producer -> this.producer = producer);
        this.request.withConnection(connection -> Netty4Utils.addListener(connection.channel().closeFuture(), closeContext));
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {
        request.withConnection(connection -> connection.channel().close());
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void sendChunk(HttpChunk chunk, ActionListener<Void> listener) {
        sender.send(createContent(chunk), listener, chunk.isLast());
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        sender.send(createContent(response), listener, true);
    }

    @Override
    public void prepareResponse(int status, Map<String, List<String>> headers) {
        this.response.status(status);
        headers.forEach((k, vs) -> vs.forEach(v -> this.response.addHeader(k, v)));
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) response.remoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) response.hostAddress();
    }

    @Override
    public void receiveChunk(HttpChunk message) {
        try {
            if (lastChunkReceived) {
                return;
            }

            producer.next(message);
            if (message.isLast()) {
                lastChunkReceived = true;
                producer.complete();
            }
        } catch (final Exception ex) {
            producer.error(ex);
        } finally {
            message.close();
        }
    }

    @Override
    public boolean isReadable() {
        return producer != null;
    }

    @Override
    public boolean isWritable() {
        return sender.isReady();
    }

    @Override
    public void subscribe(Subscriber<? super HttpChunk> subscriber) {
        receiver.subscribe(subscriber);
    }

    private static HttpContent createContent(HttpResponse response) {
        final FullHttpResponse fullHttpResponse = (FullHttpResponse) response;
        return new DefaultHttpContent(fullHttpResponse.content());
    }

    private static HttpContent createContent(HttpChunk chunk) {
        return new DefaultHttpContent(Unpooled.copiedBuffer(BytesReference.toByteBuffers(chunk.content())));
    }
}
