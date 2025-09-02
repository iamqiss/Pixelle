/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.http;

import org.density.common.network.NetworkAddress;
import org.density.common.xcontent.XContentFactory;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.common.transport.TransportAddress;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

public class HttpInfoTests extends DensityTestCase {

    public void testCorrectlyDisplayPublishedCname() throws Exception {
        InetAddress localhost = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(
            new HttpInfo(
                new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(localhost, port) },
                    new TransportAddress(localhost, port)
                ),
                0L
            ),
            "localhost/" + NetworkAddress.format(localhost) + ':' + port
        );
    }

    public void testCorrectDisplayPublishedIp() throws Exception {
        InetAddress localhost = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("localhost")));
        int port = 9200;
        assertPublishAddress(
            new HttpInfo(
                new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(localhost, port) },
                    new TransportAddress(localhost, port)
                ),
                0L
            ),
            NetworkAddress.format(localhost) + ':' + port
        );
    }

    public void testCorrectDisplayPublishedIpv6() throws Exception {
        int port = 9200;
        TransportAddress localhost = new TransportAddress(
            InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("0:0:0:0:0:0:0:1"))),
            port
        );
        assertPublishAddress(
            new HttpInfo(new BoundTransportAddress(new TransportAddress[] { localhost }, localhost), 0L),
            localhost.toString()
        );
    }

    @SuppressWarnings("unchecked")
    private void assertPublishAddress(HttpInfo httpInfo, String expected) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        httpInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            expected,
            ((Map<String, Object>) createParser(builder).map().get(HttpInfo.Fields.HTTP)).get(HttpInfo.Fields.PUBLISH_ADDRESS)
        );
    }
}
