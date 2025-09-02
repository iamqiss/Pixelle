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

package org.density.gradle.test;

import org.density.gradle.DensityTestBasePlugin;
import org.density.gradle.SystemPropertyCommandLineArgumentProvider;
import org.density.gradle.testclusters.DensityCluster;
import org.density.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class RestTestBasePlugin implements Plugin<Project> {
    private static final String TESTS_REST_CLUSTER = "tests.rest.cluster";
    private static final String TESTS_CLUSTER = "tests.cluster";
    private static final String TESTS_CLUSTER_NAME = "tests.clustername";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(DensityTestBasePlugin.class);
        project.getTasks().withType(RestIntegTestTask.class).configureEach(restIntegTestTask -> {
            @SuppressWarnings("unchecked")
            NamedDomainObjectContainer<DensityCluster> testClusters = (NamedDomainObjectContainer<DensityCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            DensityCluster cluster = testClusters.maybeCreate(restIntegTestTask.getName());
            restIntegTestTask.useCluster(project, cluster);
            restIntegTestTask.include("**/*IT.class");
            restIntegTestTask.systemProperty("tests.rest.load_packaged", Boolean.FALSE.toString());
            if (System.getProperty(TESTS_REST_CLUSTER) == null) {
                if (System.getProperty(TESTS_CLUSTER) != null || System.getProperty(TESTS_CLUSTER_NAME) != null) {
                    throw new IllegalArgumentException(
                        String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                    );
                }
                SystemPropertyCommandLineArgumentProvider runnerNonInputProperties =
                    (SystemPropertyCommandLineArgumentProvider) restIntegTestTask.getExtensions().getByName("nonInputProperties");
                runnerNonInputProperties.systemProperty(TESTS_REST_CLUSTER, () -> String.join(",", cluster.getAllHttpSocketURI()));
                runnerNonInputProperties.systemProperty(TESTS_CLUSTER, () -> String.join(",", cluster.getAllTransportPortURI()));
                runnerNonInputProperties.systemProperty(TESTS_CLUSTER_NAME, cluster::getName);
            } else {
                if (System.getProperty(TESTS_CLUSTER) == null || System.getProperty(TESTS_CLUSTER_NAME) == null) {
                    throw new IllegalArgumentException(
                        String.format("%s, %s, and %s must all be null or non-null", TESTS_REST_CLUSTER, TESTS_CLUSTER, TESTS_CLUSTER_NAME)
                    );
                }
            }
        });
    }
}
