/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test;

import org.density.common.settings.Settings;
import org.density.indices.replication.common.ReplicationType;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.density.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.density.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;

/**
 * Base class for running the tests with parameterization with static settings: the cluster will be pre-created with the settings at startup, the method
 * {@link #hasSameParametersAs(ParameterizedDensityIntegTestCase)} is being used by the test scaffolding to detect when the test suite is instantiated with
 * the new parameters and the test cluster has to be recreated.
 * <p>
 * Here is the simple illustration on of the execution flow per parameters combination:
 * <ul>
 *   <li><b>suite scope</b>: create cluster -&gt; for each test method { run test method } -&gt; shutdown cluster</li>
 *   <li><b>test scope</b>: for each test method { create cluster -&gt; run test method -&gt; shutdown cluster }</li>
 * </ul>
 */
public abstract class ParameterizedStaticSettingsDensityIntegTestCase extends ParameterizedDensityIntegTestCase {

    protected static final String REMOTE_STORE_REPOSITORY_NAME = "test-remote-store-repo";
    private Path remoteStoreRepositoryPath;
    public static final List<Object[]> replicationSettings = Arrays.asList(
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build() },
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build() }
    );

    public ParameterizedStaticSettingsDensityIntegTestCase(Settings nodeSettings) {
        super(nodeSettings);
    }

    public static final List<Object[]> remoteStoreSettings = Arrays.asList(
        new Object[] { Settings.builder().put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build() },
        new Object[] { Settings.builder().put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false).build() }
    );

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        if (REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings)) {
            if (remoteStoreRepositoryPath == null) {
                remoteStoreRepositoryPath = randomRepoPath().toAbsolutePath();
            }
            builder.put(remoteStoreClusterSettings(REMOTE_STORE_REPOSITORY_NAME, remoteStoreRepositoryPath));
        }
        return builder.put(super.nodeSettings(nodeOrdinal)).put(settings).build();
    }

    @Override
    boolean hasSameParametersAs(ParameterizedDensityIntegTestCase obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final ParameterizedStaticSettingsDensityIntegTestCase other = (ParameterizedStaticSettingsDensityIntegTestCase) obj;
        return Objects.equals(settings, other.settings);
    }
}
