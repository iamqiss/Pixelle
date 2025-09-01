/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.systemingestprocessor;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.test.rest.yaml.ClientYamlTestCandidate;
import org.density.test.rest.yaml.DensityClientYamlSuiteTestCase;

public class ExampleSystemIngestProcessorClientYamlTestSuiteIT extends DensityClientYamlSuiteTestCase {

    public ExampleSystemIngestProcessorClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return DensityClientYamlSuiteTestCase.createParameters();
    }
}
