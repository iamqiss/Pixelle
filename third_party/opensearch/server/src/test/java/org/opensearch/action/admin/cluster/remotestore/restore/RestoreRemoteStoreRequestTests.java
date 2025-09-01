/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.restore;

import org.density.common.xcontent.XContentFactory;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestoreRemoteStoreRequestTests extends AbstractWireSerializingTestCase<RestoreRemoteStoreRequest> {
    private RestoreRemoteStoreRequest randomState(RestoreRemoteStoreRequest instance) {
        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            instance.indices(indices);
        }

        instance.waitForCompletion(randomBoolean());
        instance.restoreAllShards(randomBoolean());

        if (randomBoolean()) {
            instance.clusterManagerNodeTimeout(randomTimeValue());
        }

        return instance;
    }

    @Override
    protected RestoreRemoteStoreRequest createTestInstance() {
        return randomState(new RestoreRemoteStoreRequest());
    }

    @Override
    protected Writeable.Reader<RestoreRemoteStoreRequest> instanceReader() {
        return RestoreRemoteStoreRequest::new;
    }

    @Override
    protected RestoreRemoteStoreRequest mutateInstance(RestoreRemoteStoreRequest instance) throws IOException {
        RestoreRemoteStoreRequest copy = copyInstance(instance);
        // ensure that at least one property is different
        List<String> indices = new ArrayList<>(List.of(instance.indices()));
        indices.add("copied");
        copy.indices(indices);
        return randomState(copy);
    }

    public void testSource() throws IOException {
        RestoreRemoteStoreRequest original = createTestInstance();
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = MediaTypeRegistry.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();

        RestoreRemoteStoreRequest processed = new RestoreRemoteStoreRequest();
        processed.clusterManagerNodeTimeout(original.clusterManagerNodeTimeout());
        processed.waitForCompletion(original.waitForCompletion());
        processed.restoreAllShards(original.restoreAllShards());
        processed.source(map);

        assertEquals(original, processed);
    }
}
