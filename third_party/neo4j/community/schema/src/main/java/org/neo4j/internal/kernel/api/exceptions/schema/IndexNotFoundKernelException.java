/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class IndexNotFoundKernelException extends KernelException {
    private final IndexDescriptor index;

    public IndexNotFoundKernelException(String msg) {
        super(Status.Schema.IndexNotFound, msg);
        this.index = null;
    }

    private IndexNotFoundKernelException(ErrorGqlStatusObject gqlStatusObject, String msg) {
        super(gqlStatusObject, Status.Schema.IndexNotFound, msg);
        this.index = null;
    }

    public IndexNotFoundKernelException(String msg, IndexDescriptor index) {
        super(Status.Schema.IndexNotFound, msg);
        this.index = index;
    }

    private IndexNotFoundKernelException(ErrorGqlStatusObject gqlStatusObject, String msg, IndexDescriptor index) {
        super(gqlStatusObject, Status.Schema.IndexNotFound, msg);
        this.index = index;
    }

    public static IndexNotFoundKernelException indexIsStillPopulating(String indexPopulationJobDescription) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N63)
                .build();
        return new IndexNotFoundKernelException(gql, "Index is still populating: " + indexPopulationJobDescription);
    }

    public static IndexNotFoundKernelException indexIsStillPopulating(IndexDescriptor descriptor) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N63)
                .build();
        return new IndexNotFoundKernelException(gql, descriptor + " is still populating");
    }

    public static IndexNotFoundKernelException indexDroppedWhileSampling() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N64)
                .build();
        return new IndexNotFoundKernelException(gql, "Index dropped while sampling.");
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        if (index == null) {
            return super.getUserMessage(tokenNameLookup);
        } else {
            return super.getUserMessage(tokenNameLookup) + index.userDescription(tokenNameLookup);
        }
    }
}
