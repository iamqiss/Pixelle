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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.test.rest;

import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.rest.RestController;
import org.density.rest.RestRequest;
import org.density.tasks.Task;
import org.density.tasks.TaskListener;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.transport.client.node.NodeClient;
import org.density.usage.UsageService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * A common base class for Rest*ActionTests. Provides access to a {@link RestController}
 * that can be used to register individual REST actions, and test request handling.
 */
public abstract class RestActionTestCase extends DensityTestCase {
    private RestController controller;
    protected VerifyingClient verifyingClient;

    @Before
    public void setUpController() {
        verifyingClient = new VerifyingClient(this.getTestName());
        controller = new RestController(Collections.emptySet(), null, verifyingClient, new NoneCircuitBreakerService(), new UsageService());
    }

    @After
    public void tearDownController() {
        verifyingClient.close();
    }

    /**
     * A test {@link RestController}. This controller can be used to register and delegate
     * to handlers, but uses a mock client and cannot carry out the full request.
     */
    protected RestController controller() {
        return controller;
    }

    /**
     * Sends the given request to the test controller in {@link #controller()}.
     */
    protected void dispatchRequest(RestRequest request) {
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller.dispatchRequest(request, channel, threadContext);
    }

    /**
     * A mocked {@link NodeClient} which can be easily reconfigured to verify arbitrary verification
     * functions, and can be reset to allow reconfiguration partway through a test without having to construct a new object.
     * <p>
     * By default, will throw {@link AssertionError} when any execution method is called, unless configured otherwise using
     * {@link #setExecuteVerifier(BiFunction)} or {@link #setExecuteLocallyVerifier(BiFunction)}.
     */
    public static class VerifyingClient extends NoOpNodeClient {
        AtomicReference<BiFunction> executeVerifier = new AtomicReference<>();
        AtomicReference<BiFunction> executeLocallyVerifier = new AtomicReference<>();

        public VerifyingClient(String testName) {
            super(testName);
            reset();
        }

        /**
         * Clears any previously set verifier functions set by {@link #setExecuteVerifier(BiFunction)} and/or
         * {@link #setExecuteLocallyVerifier(BiFunction)}. These functions are replaced with functions which will throw an
         * {@link AssertionError} if called.
         */
        public void reset() {
            executeVerifier.set((arg1, arg2) -> { throw new AssertionError(); });
            executeLocallyVerifier.set((arg1, arg2) -> { throw new AssertionError(); });
        }

        /**
         * Sets the function that will be called when {@link #doExecute(ActionType, ActionRequest, ActionListener)} is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #doExecute(ActionType, ActionRequest, ActionListener)}
         */
        public <Request extends ActionRequest, Response extends ActionResponse> void setExecuteVerifier(
            BiFunction<ActionType<Response>, Request, Response> verifier
        ) {
            executeVerifier.set(verifier);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) executeVerifier.get().apply(action, request));
        }

        /**
         * Sets the function that will be called when {@link #executeLocally(ActionType, ActionRequest, TaskListener)}is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #executeLocally(ActionType, ActionRequest, TaskListener)}
         */
        public <Request extends ActionRequest, Response extends ActionResponse> void setExecuteLocallyVerifier(
            BiFunction<ActionType<Response>, Request, Response> verifier
        ) {
            executeLocallyVerifier.set(verifier);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) executeLocallyVerifier.get().apply(action, request));
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            TaskListener<Response> listener
        ) {
            listener.onResponse(null, (Response) executeLocallyVerifier.get().apply(action, request));
            return null;
        }

    }
}
