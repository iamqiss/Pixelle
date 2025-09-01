/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.datastream;

import org.density.action.admin.indices.rollover.RolloverResponse;
import org.density.cluster.metadata.DataStream;
import org.density.core.index.Index;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamRolloverIT extends DataStreamTestCase {

    public void testDataStreamRollover() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");

        DataStream dataStream;
        GetDataStreamAction.Response.DataStreamInfo dataStreamInfo;
        GetDataStreamAction.Response response;

        // Data stream before a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001")
        );

        // Perform a rollover.
        RolloverResponse rolloverResponse = rolloverDataStream("logs-demo");
        assertThat(rolloverResponse.getOldIndex(), equalTo(".ds-logs-demo-000001"));
        assertThat(rolloverResponse.getNewIndex(), equalTo(".ds-logs-demo-000002"));

        // Data stream after a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(2L));
        assertThat(dataStream.getIndices().size(), equalTo(2));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001", ".ds-logs-demo-000002")
        );
    }

}
