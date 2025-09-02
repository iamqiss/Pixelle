/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.concurrency.DensityRejectedExecutionException;
import org.density.node.NodeResourceUsageStats;
import org.density.node.ResourceUsageCollectorService;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.density.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;

import java.util.Locale;
import java.util.Optional;

/**
 *  Class for CPU Based Admission Controller in Density, which aims to provide CPU utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class CpuBasedAdmissionController extends AdmissionController {
    public static final String CPU_BASED_ADMISSION_CONTROLLER = "global_cpu_usage";
    private static final Logger LOGGER = LogManager.getLogger(CpuBasedAdmissionController.class);
    public CpuBasedAdmissionControllerSettings settings;

    /**
     * @param admissionControllerName       Name of the admission controller
     * @param resourceUsageCollectorService Instance used to get node resource usage stats
     * @param clusterService                ClusterService Instance
     * @param settings                      Immutable settings instance
     */
    public CpuBasedAdmissionController(
        String admissionControllerName,
        ResourceUsageCollectorService resourceUsageCollectorService,
        ClusterService clusterService,
        Settings settings
    ) {
        super(admissionControllerName, resourceUsageCollectorService, clusterService);
        this.settings = new CpuBasedAdmissionControllerSettings(clusterService.getClusterSettings(), settings);
    }

    /**
     * Apply admission control based on process CPU usage
     * @param action is the transport action
     */
    @Override
    public void apply(String action, AdmissionControlActionType admissionControlActionType) {
        if (this.isEnabledForTransportLayer(this.settings.getTransportLayerAdmissionControllerMode())) {
            this.applyForTransportLayer(action, admissionControlActionType);
        }
    }

    /**
     * Apply transport layer admission control if configured limit has been reached
     */
    private void applyForTransportLayer(String actionName, AdmissionControlActionType admissionControlActionType) {
        if (isLimitsBreached(actionName, admissionControlActionType)) {
            this.addRejectionCount(admissionControlActionType.getType(), 1);
            if (this.isAdmissionControllerEnforced(this.settings.getTransportLayerAdmissionControllerMode())) {
                throw new DensityRejectedExecutionException(
                    String.format(
                        Locale.ROOT,
                        "CPU usage admission controller rejected the request for action [%s] as CPU limit reached for action-type [%s]",
                        actionName,
                        admissionControlActionType.name()
                    )
                );
            }
        }
    }

    /**
     * Check if the configured resource usage limits are breached for the action
     */
    private boolean isLimitsBreached(String actionName, AdmissionControlActionType admissionControlActionType) {
        // check if cluster state is ready
        if (clusterService.state() != null && clusterService.state().nodes() != null) {
            long maxCpuLimit = this.getCpuRejectionThreshold(admissionControlActionType);
            Optional<NodeResourceUsageStats> nodePerformanceStatistics = this.resourceUsageCollectorService.getNodeStatistics(
                this.clusterService.state().nodes().getLocalNodeId()
            );
            if (nodePerformanceStatistics.isPresent()) {
                double cpuUsage = nodePerformanceStatistics.get().getCpuUtilizationPercent();
                if (cpuUsage >= maxCpuLimit) {
                    LOGGER.warn(
                        "CpuBasedAdmissionController limit reached as the current CPU "
                            + "usage [{}] exceeds the allowed limit [{}] for transport action [{}] in admissionControlMode [{}]",
                        cpuUsage,
                        maxCpuLimit,
                        actionName,
                        this.settings.getTransportLayerAdmissionControllerMode()
                    );
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get CPU rejection threshold based on action type
     */
    private long getCpuRejectionThreshold(AdmissionControlActionType admissionControlActionType) {
        switch (admissionControlActionType) {
            case SEARCH:
                return this.settings.getSearchCPULimit();
            case INDEXING:
                return this.settings.getIndexingCPULimit();
            case CLUSTER_ADMIN:
                return this.settings.getClusterAdminCPULimit();
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Admission control not Supported for AdmissionControlActionType: %s",
                        admissionControlActionType.getType()
                    )
                );
        }
    }
}
