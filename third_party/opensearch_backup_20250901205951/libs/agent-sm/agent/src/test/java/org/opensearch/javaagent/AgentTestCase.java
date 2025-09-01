/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.javaagent;

import org.density.javaagent.bootstrap.AgentPolicy;
import org.junit.BeforeClass;

import java.security.Policy;
import java.util.Set;

public abstract class AgentTestCase {
    @SuppressWarnings("removal")
    @BeforeClass
    public static void setUp() {
        AgentPolicy.setPolicy(new Policy() {
        },
            Set.of(),
            Set.of(),
            (caller, chain) -> caller.getName().equalsIgnoreCase("worker.org.gradle.process.internal.worker.GradleWorkerMain")
        );
    }
}
