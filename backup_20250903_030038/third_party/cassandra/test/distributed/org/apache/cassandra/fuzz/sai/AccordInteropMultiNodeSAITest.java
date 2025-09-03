/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.fuzz.sai;

import org.junit.Ignore;

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.service.consensus.TransactionalMode;

@Ignore("It was believed that these tests were failing due to CASSANDRA-20567, but in fixing that issue it was found that the tests are still failing!  Harry is detecting an incorrect response...")
public class AccordInteropMultiNodeSAITest extends MultiNodeSAITestBase
{
    public AccordInteropMultiNodeSAITest()
    {
        super(TransactionalMode.test_interop_read);
    }

    @Override
    protected Generator<SchemaSpec> schemaGenerator(boolean disableReadRepair)
    {
        return SchemaGenerators.schemaSpecGen(KEYSPACE, "basic_sai", MAX_PARTITION_SIZE,
                                              SchemaSpec.optionsBuilder().disableReadRepair(disableReadRepair)
                                                                         .withTransactionalMode(TransactionalMode.test_interop_read));
    }
}
