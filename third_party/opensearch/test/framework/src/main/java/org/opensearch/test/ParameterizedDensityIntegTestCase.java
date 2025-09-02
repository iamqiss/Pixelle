/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test;

import org.density.common.settings.Settings;

import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.density.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;

/**
 * Base class for running the tests with parameterization of the settings.
 * For any class that wants to use parameterization, use {@link com.carrotsearch.randomizedtesting.annotations.ParametersFactory} to generate
 * different parameters.
 *
 * There are two flavors of applying the parameterized settings to the cluster on the suite level:
 *  - static: the cluster will be pre-created with the settings at startup, please subclass {@link ParameterizedStaticSettingsDensityIntegTestCase}, the method
 *    {@link #hasSameParametersAs(ParameterizedDensityIntegTestCase)} is being used by the test scaffolding to detect when the test suite is instantiated with
 *    the new parameters and the test cluster has to be recreated
 *  - dynamic: the cluster will be created once before the test suite and the settings will be applied dynamically , please subclass {@link ParameterizedDynamicSettingsDensityIntegTestCase},
 *    please notice that not all settings could be changed dynamically
 *
 * If the test suites use per-test level, the cluster will be recreated per each test method (applying static or dynamic settings).
 */
abstract class ParameterizedDensityIntegTestCase extends DensityIntegTestCase {
    protected final Settings settings;

    ParameterizedDensityIntegTestCase(Settings settings) {
        this.settings = settings;
    }

    // This method shouldn't be called in setupSuiteScopeCluster(). Only call this method inside single test.
    public void indexRandomForConcurrentSearch(String... indices) throws InterruptedException {
        if (CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(settings)
            || CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(settings).equals(CONCURRENT_SEGMENT_SEARCH_MODE_AUTO)
            || CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(settings).equals(CONCURRENT_SEGMENT_SEARCH_MODE_ALL)) {
            indexRandomForMultipleSlices(indices);
        }
    }

    /**
     * Compares the parameters of the two {@link ParameterizedDensityIntegTestCase} test suite instances.
     * This method is being use by {@link DensityTestClusterRule} to determine when the parameterized test suite is instantiated with
     * another set of parameters and the test cluster has to be recreated to reflect that.
     * @param obj instance of the {@link ParameterizedDensityIntegTestCase} to compare with
     * @return {@code true} of the parameters of the test suites are the same, {@code false} otherwise
     */
    abstract boolean hasSameParametersAs(ParameterizedDensityIntegTestCase obj);
}
