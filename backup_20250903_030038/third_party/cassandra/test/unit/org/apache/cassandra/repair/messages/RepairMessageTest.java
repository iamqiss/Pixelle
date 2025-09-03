/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair.messages;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.service.WaitStrategy;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.repair.messages.RepairMessage.always;
import static org.apache.cassandra.repair.messages.RepairMessage.sendMessageWithRetries;
import static org.apache.cassandra.test.asserts.ExtendedAssertions.assertThat;

public class RepairMessageTest
{
    private static final TimeUUID SESSION = new TimeUUID(0, 0);
    private static final InetAddressAndPort ADDRESS = FBUtilities.getBroadcastAddressAndPort();
    private static final Answer REJECT_ALL = ignore -> {
        throw new UnsupportedOperationException();
    };
    private static final int[] retries = { 1, 2, 10 };
    // Tests may use verb / message pairs that do not make sense... that is due to the fact that the message sending logic does not validate this and delegates such validation to messaging, which is mocked within the class...
    // By using messages with simpler state it makes the test easier to read, even though the verb -> message mapping is incorrect.
    private static final Verb VERB = Verb.PREPARE_MSG;
    private static final RepairMessage PAYLOAD = new CleanupMessage(SESSION);
    public static final int[] NO_RETRY_ATTEMPTS = { 0 };

    static
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
        RepairMetrics.init();
    }

    @Before
    public void before()
    {
        RepairMetrics.unsafeReset();
    }

    @Test
    public void noRetries()
    {
        test(NO_RETRY_ATTEMPTS, (ignore, callback) -> {
            callback.onResponse(Message.out(VERB, PAYLOAD));
            assertNoRetries();
        });
    }

    @Test
    public void noRetriesRequestFailed()
    {
        test(NO_RETRY_ATTEMPTS, ((ignore, callback) -> {
            callback.onFailure(ADDRESS, RequestFailure.UNKNOWN);
            assertNoRetries();
        }));
    }

    @Test
    public void retryWithSuccess()
    {
        test((maxAttempts, callback) -> {
            callback.onResponse(Message.out(VERB, PAYLOAD));
            assertMetrics(maxAttempts, false, false);
        });
    }

    @Test
    public void retryWithTimeout()
    {
        test((maxRetries, callback) -> {
            callback.onFailure(ADDRESS, RequestFailure.TIMEOUT);
            assertMetrics(maxRetries, true, false);
        });
    }

    @Test
    public void retryWithFailure()
    {
        test((maxRetries, callback) -> {
            callback.onFailure(ADDRESS, RequestFailure.UNKNOWN);
            assertMetrics(maxRetries, false, true);
        });
    }

    private void assertNoRetries()
    {
        assertMetrics(0, false, false);
    }

    private void assertMetrics(long retries, boolean timeout, boolean failure)
    {
        if (retries == 0)
        {
            assertThat(RepairMetrics.retries).isEmpty();
            assertThat(RepairMetrics.retriesByVerb.get(VERB)).isEmpty();
            assertThat(RepairMetrics.retryTimeout).isEmpty();
            assertThat(RepairMetrics.retryTimeoutByVerb.get(VERB)).isEmpty();
            assertThat(RepairMetrics.retryFailure).isEmpty();
            assertThat(RepairMetrics.retryFailureByVerb.get(VERB)).isEmpty();
        }
        else
        {
            assertThat(RepairMetrics.retries).hasCount(1).hasMax(retries);
            assertThat(RepairMetrics.retriesByVerb.get(VERB)).hasCount(1).hasMax(retries);
            assertThat(RepairMetrics.retryTimeout).hasCount(timeout ? 1 : 0);
            assertThat(RepairMetrics.retryTimeoutByVerb.get(VERB)).hasCount(timeout ? 1 : 0);
            assertThat(RepairMetrics.retryFailure).hasCount(failure ? 1 : 0);
            assertThat(RepairMetrics.retryFailureByVerb.get(VERB)).hasCount(failure ? 1 : 0);
        }
    }

    private static WaitStrategy backoff(int maxRetries)
    {
        return RetryStrategy.parse("0 <= 100ms * 2^attempts <= 1000ms,retries=" + maxRetries, LatencySourceFactory.none());
    }

    private static SharedContext ctx()
    {
        SharedContext ctx = Mockito.mock(SharedContext.class, REJECT_ALL);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class, REJECT_ALL);
        // allow all retry methods and send with callback
        Mockito.doCallRealMethod().when(messaging).sendWithRetries(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doNothing().when(messaging).sendWithCallback(Mockito.any(), Mockito.any(), Mockito.any());
        IGossiper gossiper = Mockito.mock(IGossiper.class, REJECT_ALL);
        Mockito.doReturn(RepairMessage.SUPPORTS_RETRY).when(gossiper).getReleaseVersion(Mockito.any());
        ScheduledExecutorPlus executor = Mockito.mock(ScheduledExecutorPlus.class, REJECT_ALL);
        Mockito.doAnswer(invocationOnMock -> {
            Runnable fn = invocationOnMock.getArgument(0);
            fn.run();
            return null;
        }).when(executor).schedule(Mockito.<Runnable>any(), Mockito.anyLong(), Mockito.any());

        Mockito.doReturn(messaging).when(ctx).messaging();
        Mockito.doReturn(gossiper).when(ctx).gossiper();
        Mockito.doReturn(executor).when(ctx).optionalTasks();
        return ctx;
    }

    private static <T extends RepairMessage> RequestCallback<T> callback(MessageDelivery messaging)
    {
        ArgumentCaptor<Message<?>> messageCapture = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<InetAddressAndPort> endpointCapture = ArgumentCaptor.forClass(InetAddressAndPort.class);
        ArgumentCaptor<RequestCallback<T>> callbackCapture = ArgumentCaptor.forClass(RequestCallback.class);

        Mockito.verify(messaging).sendWithCallback(messageCapture.capture(), endpointCapture.capture(), callbackCapture.capture());
        Mockito.clearInvocations(messaging);

        Assertions.assertThat(endpointCapture.getValue()).isEqualTo(ADDRESS);

        return callbackCapture.getValue();
    }

    private interface TestCase
    {
        void test(int maxAttempts, RequestCallback<RepairMessage> callback);
    }

    private void test(TestCase fn)
    {
        test(retries, fn);
    }

    private void test(int[] retries, TestCase fn)
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        for (int maxRetries : retries)
        {
            before();

            sendMessageWithRetries(ctx, backoff(maxRetries), always(), PAYLOAD, VERB, ADDRESS, RepairMessage.NOOP_CALLBACK);
            for (int i = 0; i < maxRetries; i++)
                callback(messaging).onFailure(ADDRESS, RequestFailure.TIMEOUT);
            fn.test(maxRetries, callback(messaging));
            Mockito.verifyNoInteractions(messaging);
        }
    }
}