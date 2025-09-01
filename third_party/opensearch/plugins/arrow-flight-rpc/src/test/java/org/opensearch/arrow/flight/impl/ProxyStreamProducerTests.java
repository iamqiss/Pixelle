/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.impl;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.density.arrow.spi.StreamProducer;
import org.density.common.unit.TimeValue;
import org.density.test.DensityTestCase;
import org.junit.After;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyStreamProducerTests extends DensityTestCase {

    private FlightStream mockRemoteStream;
    private BufferAllocator mockAllocator;
    private ProxyStreamProducer proxyStreamProducer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockRemoteStream = mock(FlightStream.class);
        mockAllocator = mock(BufferAllocator.class);
        proxyStreamProducer = new ProxyStreamProducer(new FlightStreamReader(mockRemoteStream));
    }

    public void testCreateRoot() throws Exception {
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(mockRemoteStream.getRoot()).thenReturn(mockRoot);

        VectorSchemaRoot result = proxyStreamProducer.createRoot(mockAllocator);

        assertEquals(mockRoot, result);
        verify(mockRemoteStream).getRoot();
    }

    public void testDefaults() {
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(mockRoot.getRowCount()).thenReturn(100);
        when(mockRemoteStream.getRoot()).thenReturn(mockRoot);
        assertEquals(100, proxyStreamProducer.estimatedRowCount());
        try {
            proxyStreamProducer.getAction();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertEquals("Not implemented yet", e.getMessage());
        }
    }

    public void testCreateJob() {
        StreamProducer.BatchedJob<VectorSchemaRoot> job = proxyStreamProducer.createJob(mockAllocator);

        assertNotNull(job);
        assertTrue(job instanceof ProxyStreamProducer.ProxyBatchedJob);
    }

    public void testProxyBatchedJob() throws Exception {
        StreamProducer.BatchedJob<VectorSchemaRoot> job = proxyStreamProducer.createJob(mockAllocator);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);

        when(mockRemoteStream.next()).thenReturn(true, true, false);

        job.run(mockRoot, mockFlushSignal);

        verify(mockRemoteStream, times(3)).next();
        verify(mockFlushSignal, times(2)).awaitConsumption(TimeValue.timeValueMillis(1000));
    }

    public void testProxyBatchedJobWithException() throws Exception {
        StreamProducer.BatchedJob<VectorSchemaRoot> job = proxyStreamProducer.createJob(mockAllocator);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);

        doThrow(new RuntimeException("Test exception")).when(mockRemoteStream).next();

        try {
            job.run(mockRoot, mockFlushSignal);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("Test exception", e.getMessage());
        }

        verify(mockRemoteStream, times(1)).next();
    }

    public void testProxyBatchedJobOnCancel() throws Exception {
        StreamProducer.BatchedJob<VectorSchemaRoot> job = proxyStreamProducer.createJob(mockAllocator);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);
        when(mockRemoteStream.next()).thenReturn(true, true, false);

        // cancel the job
        job.onCancel();
        job.run(mockRoot, mockFlushSignal);
        verify(mockRemoteStream, times(0)).next();
        verify(mockFlushSignal, times(0)).awaitConsumption(TimeValue.timeValueMillis(1000));
        assertTrue(job.isCancelled());
    }

    @After
    public void tearDown() throws Exception {
        if (proxyStreamProducer != null) {
            proxyStreamProducer.close();
        }
        super.tearDown();
    }
}
