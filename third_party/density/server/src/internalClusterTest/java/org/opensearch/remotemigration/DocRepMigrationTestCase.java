/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotemigration;

import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;
import org.density.transport.client.Client;

import java.util.List;

import static org.density.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class DocRepMigrationTestCase extends MigrationBaseTestCase {

    public void testMixedModeAddDocRep() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);

        Client client = internalCluster().client(cmNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        setAddRemote(false);
        internalCluster().startNode();
        String[] allNodes = internalCluster().getNodeNames();
        assertBusy(() -> { assertEquals(client.admin().cluster().prepareClusterStats().get().getNodes().size(), allNodes.length); });
    }

}
