/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.messenger.client.blocking.tcp;

import org.testcontainers.containers.GenericContainer;
import static org.apache.messenger.client.blocking.IntegrationTest.TCP_PORT;

class TcpClientFactory {

    static MessengerTcpClient create(GenericContainer<?> messengerServer) {
        String address = messengerServer.getHost();
        Integer port = messengerServer.getMappedPort(TCP_PORT);
        return new MessengerTcpClient(address, port);
    }

}
