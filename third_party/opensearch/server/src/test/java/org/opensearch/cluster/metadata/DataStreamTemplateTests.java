/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.XContentParser;
import org.density.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DataStreamTemplateTests extends AbstractSerializingTestCase<DataStreamTemplate> {

    @Override
    protected DataStreamTemplate doParseInstance(XContentParser parser) throws IOException {
        return DataStreamTemplate.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStreamTemplate> instanceReader() {
        return DataStreamTemplate::new;
    }

    @Override
    protected DataStreamTemplate createTestInstance() {
        return new DataStreamTemplate(new DataStream.TimestampField("timestamp_" + randomAlphaOfLength(5)));
    }

}
