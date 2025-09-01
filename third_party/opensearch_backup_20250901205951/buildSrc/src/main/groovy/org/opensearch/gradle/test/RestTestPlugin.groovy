/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
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
package org.density.gradle.test

import groovy.transform.CompileStatic
import org.density.gradle.BuildPlugin
import org.density.gradle.testclusters.TestClustersPlugin
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin

/**
 * Adds support for starting an Density cluster before running integration
 * tests. Used in conjunction with {@link StandaloneRestTestPlugin} for qa
 * projects and in conjunction with {@link BuildPlugin} for testing the rest
 * client.
 */
@CompileStatic
class RestTestPlugin implements Plugin<Project> {
    List<String> REQUIRED_PLUGINS = [
        'density.build',
        'density.standalone-rest-test']

    @Override
    void apply(Project project) {
        if (false == REQUIRED_PLUGINS.any { project.pluginManager.hasPlugin(it) }) {
            throw new InvalidUserDataException('density.rest-test '
                + 'requires either density.build or '
                + 'density.standalone-rest-test')
        }
        project.getPlugins().apply(RestTestBasePlugin.class);
        project.pluginManager.apply(TestClustersPlugin)
        RestIntegTestTask integTest = project.tasks.create('integTest', RestIntegTestTask.class)
        integTest.description = 'Runs rest tests against an density cluster.'
        integTest.group = JavaBasePlugin.VERIFICATION_GROUP
        integTest.mustRunAfter(project.tasks.named('precommit'))
        project.tasks.named('check').configure { it.dependsOn(integTest) }
    }
}
