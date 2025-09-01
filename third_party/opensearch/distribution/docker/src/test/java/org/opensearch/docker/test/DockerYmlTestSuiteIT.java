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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.docker.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.density.client.Request;
import org.density.common.settings.Settings;
import org.density.test.rest.yaml.ClientYamlTestCandidate;
import org.density.test.rest.yaml.DensityClientYamlSuiteTestCase;

import java.io.IOException;

public class DockerYmlTestSuiteIT extends DensityClientYamlSuiteTestCase {

    public DockerYmlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return new StringBuilder()
            .append("localhost:")
            .append(getProperty("test.fixtures.density-1.tcp.9200"))
            .append(",")
            .append("localhost:")
            .append(getProperty("test.fixtures.density-2.tcp.9200"))
            .toString();
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the density.test.fixtures Gradle plugin");
        }
        return value;
    }

    @Before
    public void waitForCluster() throws IOException {
        super.initClient();
        Request health = new Request("GET", "/_cluster/health");
        health.addParameter("wait_for_nodes", "2");
        health.addParameter("wait_for_status", "yellow");
        client().performRequest(health);
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    @Override
    protected String getProtocol() {
        return "http";
    }
}
