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

package org.density.common.logging;


import org.density.test.DensityTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;


public class DensityJsonLayoutTests extends DensityTestCase {
    @BeforeClass
    public static void initNodeName() {
        JsonLogsTestSetup.init();
    }

    public void testEmptyType() {
        expectThrows(IllegalArgumentException.class, () -> DensityJsonLayout.newBuilder().build());
    }

    public void testLayout() {
        DensityJsonLayout server = DensityJsonLayout.newBuilder()
                                          .setType("server")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:density.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testWithMaxMessageLengthLayout() {
        DensityJsonLayout server = DensityJsonLayout.newBuilder()
            .setType("server")
            .setMaxMessageLength(42)
            .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:density.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-42m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testWithUnrestrictedMaxMessageLengthLayout() {
        DensityJsonLayout server = DensityJsonLayout.newBuilder()
            .setType("server")
            .setMaxMessageLength(0)
            .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:density.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testLayoutWithAdditionalFields() {
        DensityJsonLayout server = DensityJsonLayout.newBuilder()
                                          .setType("server")
                                          .setDensityMessageFields("x-opaque-id,someOtherField")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:density.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, \"x-opaque-id\": \"%DensityMessageField{x-opaque-id}\"}" +
                "%notEmpty{, \"someOtherField\": \"%DensityMessageField{someOtherField}\"}" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testLayoutWithAdditionalFieldOverride() {
        DensityJsonLayout server = DensityJsonLayout.newBuilder()
                                          .setType("server")
                                          .setDensityMessageFields("message")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:density.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\"" +
                "%notEmpty{, \"message\": \"%DensityMessageField{message}\"}" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }
}
