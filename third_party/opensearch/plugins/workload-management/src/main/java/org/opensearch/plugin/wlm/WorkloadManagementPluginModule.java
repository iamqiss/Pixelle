/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm;

import org.density.common.inject.AbstractModule;
import org.density.common.inject.Singleton;
import org.density.plugin.wlm.service.WorkloadGroupPersistenceService;

/**
 * Guice Module to manage WorkloadManagement related objects
 */
public class WorkloadManagementPluginModule extends AbstractModule {

    /**
     * Constructor for WorkloadManagementPluginModule
     */
    public WorkloadManagementPluginModule() {}

    @Override
    protected void configure() {
        // Bind WorkloadGroupPersistenceService as a singleton to ensure a single instance is used,
        // preventing multiple throttling key registrations in the constructor.
        bind(WorkloadGroupPersistenceService.class).in(Singleton.class);
    }
}
