/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.node.resource.tracker;

import org.apache.lucene.util.Constants;
import org.density.common.lifecycle.AbstractLifecycleComponent;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.monitor.fs.FsService;
import org.density.node.IoUsageStats;
import org.density.threadpool.ThreadPool;

/**
 * This tracks the usage of node resources such as CPU, IO and memory
 */
public class NodeResourceUsageTracker extends AbstractLifecycleComponent {
    private ThreadPool threadPool;
    private final ClusterSettings clusterSettings;
    private AverageCpuUsageTracker cpuUsageTracker;
    private AverageMemoryUsageTracker memoryUsageTracker;
    private AverageIoUsageTracker ioUsageTracker;

    private ResourceTrackerSettings resourceTrackerSettings;

    private final FsService fsService;

    public NodeResourceUsageTracker(FsService fsService, ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        this.fsService = fsService;
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.resourceTrackerSettings = new ResourceTrackerSettings(settings);
        initialize();
    }

    /**
     * Return CPU utilization average if we have enough datapoints, otherwise return 0
     */
    public double getCpuUtilizationPercent() {
        if (cpuUsageTracker.isReady()) {
            return cpuUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return memory utilization average if we have enough datapoints, otherwise return 0
     */
    public double getMemoryUtilizationPercent() {
        if (memoryUsageTracker.isReady()) {
            return memoryUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return io stats average if we have enough datapoints, otherwise return 0
     */
    public IoUsageStats getIoUsageStats() {
        return ioUsageTracker.getIoUsageStats();
    }

    /**
     * Checks if all of the resource usage trackers are ready
     */
    public boolean isReady() {
        if (Constants.LINUX) {
            return memoryUsageTracker.isReady() && cpuUsageTracker.isReady() && ioUsageTracker.isReady();
        }
        return memoryUsageTracker.isReady() && cpuUsageTracker.isReady();
    }

    void initialize() {
        cpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            resourceTrackerSettings.getCpuPollingInterval(),
            resourceTrackerSettings.getCpuWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setCpuWindowDuration
        );

        memoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            resourceTrackerSettings.getMemoryPollingInterval(),
            resourceTrackerSettings.getMemoryWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setMemoryWindowDuration
        );

        ioUsageTracker = new AverageIoUsageTracker(
            fsService,
            threadPool,
            resourceTrackerSettings.getIoPollingInterval(),
            resourceTrackerSettings.getIoWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setIoWindowDuration
        );
    }

    private void setMemoryWindowDuration(TimeValue windowDuration) {
        memoryUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setMemoryWindowDuration(windowDuration);
    }

    private void setCpuWindowDuration(TimeValue windowDuration) {
        cpuUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setCpuWindowDuration(windowDuration);
    }

    private void setIoWindowDuration(TimeValue windowDuration) {
        ioUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setIoWindowDuration(windowDuration);
    }

    /**
     * Visible for testing
     */
    ResourceTrackerSettings getResourceTrackerSettings() {
        return resourceTrackerSettings;
    }

    @Override
    protected void doStart() {
        cpuUsageTracker.doStart();
        memoryUsageTracker.doStart();
        ioUsageTracker.doStart();
    }

    @Override
    protected void doStop() {
        cpuUsageTracker.doStop();
        memoryUsageTracker.doStop();
        ioUsageTracker.doStop();
    }

    @Override
    protected void doClose() {
        cpuUsageTracker.doClose();
        memoryUsageTracker.doClose();
        ioUsageTracker.doClose();
    }
}
