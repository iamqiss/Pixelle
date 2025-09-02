/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test.telemetry;

import org.density.telemetry.metrics.Counter;
import org.density.telemetry.metrics.Histogram;
import org.density.telemetry.metrics.MetricsRegistry;
import org.density.telemetry.metrics.TaggedMeasurement;
import org.density.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * This is a simple implementation of MetricsRegistry which can be utilized by Unit Tests.
 * It just initializes and stores counters/histograms within a map, once created.
 * The maps can then be used to get the counters/histograms by their names.
 */
public class TestInMemoryMetricsRegistry implements MetricsRegistry {

    private ConcurrentHashMap<String, TestInMemoryCounter> counterStore = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, TestInMemoryHistogram> histogramStore = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public TestInMemoryMetricsRegistry() {}

    /**
     * Returns counterStore
     * @return
     */
    public ConcurrentHashMap<String, TestInMemoryCounter> getCounterStore() {
        return this.counterStore;
    }

    /**
     * Returns the histogramStore.
     * @return
     */
    public ConcurrentHashMap<String, TestInMemoryHistogram> getHistogramStore() {
        return this.histogramStore;
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        TestInMemoryCounter counter = new TestInMemoryCounter();
        counterStore.putIfAbsent(name, counter);
        return counter;
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        /**
         * ToDo: To be implemented when required.
         */
        return null;
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        TestInMemoryHistogram histogram = new TestInMemoryHistogram();
        histogramStore.putIfAbsent(name, histogram);
        return histogram;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        /**
         * ToDo: To be implemented when required.
         */
        return null;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
