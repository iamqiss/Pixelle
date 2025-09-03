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

package org.apache.cassandra.cql3.constraints;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

import static java.lang.String.format;

public class NotNullConstraint extends UnaryConstraintFunction
{
    public static final String FUNCTION_NAME = "NOT_NULL"; // as enum item
    public static final String CQL_FUNCTION_NAME = "NOT NULL";

    private static final List<String> emptyArguments = Collections.emptyList();

    public NotNullConstraint()
    {
        super(FUNCTION_NAME, emptyArguments);
    }

    public NotNullConstraint(List<String> args)
    {
        super(FUNCTION_NAME, args);
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
    {
        // on purpose empty as evaluate method already covered nullity
    }

    @Override
    public void validate(ColumnMetadata columnMetadata, String term) throws InvalidConstraintDefinitionException
    {
        super.validate(columnMetadata, term);
        if (columnMetadata.isPrimaryKeyColumn())
            throw new InvalidConstraintDefinitionException(format("%s constraint can not be specified on a %s key column '%s'",
                                                                  name,
                                                                  columnMetadata.isPartitionKey() ? "partition" : "clustering",
                                                                  columnMetadata.name));
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return CQL_FUNCTION_NAME;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof NotNullConstraint))
            return false;

        NotNullConstraint other = (NotNullConstraint) o;

        return columnName.equals(other.columnName) && name.equals(other.name);
    }
}
