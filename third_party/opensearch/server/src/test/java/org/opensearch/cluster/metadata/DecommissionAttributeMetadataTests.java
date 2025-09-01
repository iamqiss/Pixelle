/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.cluster.decommission.DecommissionAttribute;
import org.density.cluster.decommission.DecommissionAttributeMetadata;
import org.density.cluster.decommission.DecommissionStatus;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;

public class DecommissionAttributeMetadataTests extends AbstractNamedWriteableTestCase<DecommissionAttributeMetadata> {
    @Override
    protected DecommissionAttributeMetadata createTestInstance() {
        String attributeName = randomAlphaOfLength(6);
        String attributeValue = randomAlphaOfLength(6);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        DecommissionStatus decommissionStatus = randomFrom(DecommissionStatus.values());
        return new DecommissionAttributeMetadata(decommissionAttribute, decommissionStatus, randomAlphaOfLength(10));
    }

    @Override
    protected DecommissionAttributeMetadata mutateInstance(DecommissionAttributeMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(
                    DecommissionAttributeMetadata.class,
                    DecommissionAttributeMetadata.TYPE,
                    DecommissionAttributeMetadata::new
                )
            )
        );
    }

    @Override
    protected Class<DecommissionAttributeMetadata> categoryClass() {
        return DecommissionAttributeMetadata.class;
    }
}
