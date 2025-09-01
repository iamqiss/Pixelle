/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gradle.agent;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.Objects;

/**
 * Gradle plugin to automatically configure the Density Java agent
 * for test tasks in Density plugin projects.
 */
public class JavaAgent implements Plugin<Project> {

    /**
     * Plugin implementation that sets up java agent configuration and applies it to test tasks.
     */
    @Override
    public void apply(Project project) {
        Configuration agentConfiguration = project.getConfigurations().findByName("agent");
        if (agentConfiguration == null) {
            agentConfiguration = project.getConfigurations().create("agent");
        }

        project.afterEvaluate(p -> {
            String densityVersion = getOpensearchVersion(p);
            p.getDependencies().add("agent", "org.density:density-agent-bootstrap:" + densityVersion);
            p.getDependencies().add("agent", "org.density:density-agent:" + densityVersion);
        });

        Configuration finalAgentConfiguration = agentConfiguration;
        TaskProvider<Copy> prepareJavaAgent = project.getTasks().register("prepareJavaAgent", Copy.class, task -> {
            task.from(finalAgentConfiguration);
            task.into(new File(project.getBuildDir(), "agent"));
        });

        project.getTasks().withType(Test.class).configureEach(testTask -> {
            testTask.dependsOn(prepareJavaAgent);

            final String densityVersion = getOpensearchVersion(project);

            testTask.doFirst(task -> {
                File agentJar = new File(project.getBuildDir(), "agent/density-agent-" + densityVersion + ".jar");

                testTask.jvmArgs("-javaagent:" + agentJar.getAbsolutePath());
            });
        });
    }

    /**
     * Gets the Density version from project properties, with a fallback default.
     *
     * @param project The Gradle project
     * @return The Density version to use
     */
    private String getOpensearchVersion(Project project) {
        return Objects.requireNonNull(project.property("density_version")).toString();
    }
}
