/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.common.lifecycle.AbstractLifecycleComponent;
import org.density.common.unit.TimeValue;
import org.density.common.util.MovingAverage;
import org.density.threadpool.Scheduler;
import org.density.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for sliding window resource usage trackers
 */
public abstract class AbstractAverageUsageTracker extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(AbstractAverageUsageTracker.class);

    protected final ThreadPool threadPool;
    protected final TimeValue pollingInterval;
    private TimeValue windowDuration;
    private final AtomicReference<MovingAverage> observations = new AtomicReference<>();

    protected volatile Scheduler.Cancellable scheduledFuture;

    public AbstractAverageUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        this.threadPool = threadPool;
        this.pollingInterval = pollingInterval;
        this.windowDuration = windowDuration;
        this.setWindowSize(windowDuration);
    }

    public abstract long getUsage();

    /**
     * Returns the moving average of the datapoints
     */
    public double getAverage() {
        return observations.get().getAverage();
    }

    /**
     * Checks if we have datapoints more than or equal to the window size
     */
    public boolean isReady() {
        return observations.get().isReady();
    }

    /**
     * Creates a new instance of MovingAverage with a new window size based on WindowDuration
     */
    public void setWindowSize(TimeValue windowDuration) {
        this.windowDuration = windowDuration;
        int windowSize = (int) (windowDuration.nanos() / pollingInterval.nanos());
        LOGGER.debug("updated window size: {}", windowSize);
        observations.set(new MovingAverage(windowSize));
    }

    public TimeValue getPollingInterval() {
        return pollingInterval;
    }

    public TimeValue getWindowDuration() {
        return windowDuration;
    }

    public long getWindowSize() {
        return observations.get().getCount();
    }

    public void recordUsage(long usage) {
        observations.get().record(usage);
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            long usage = getUsage();
            recordUsage(usage);
        }, pollingInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}
}
