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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

public class DropConstraintFailureException extends SchemaKernelException {
    private final SchemaDescriptorSupplier constraint;
    private final String nameOrSchema;

    private DropConstraintFailureException(
            ErrorGqlStatusObject gqlStatusObject, SchemaDescriptorSupplier constraint, Throwable cause) {
        super(
                gqlStatusObject,
                Status.Schema.ConstraintDropFailed,
                cause,
                "Unable to drop constraint: " + cause.getMessage());

        this.constraint = constraint;
        this.nameOrSchema = null;
    }

    private DropConstraintFailureException(ErrorGqlStatusObject gqlStatusObject, String nameOrSchema, Throwable cause) {
        // nameOrSchema is just 'name' or 'on schema'
        super(
                gqlStatusObject,
                Status.Schema.ConstraintDropFailed,
                cause,
                "Unable to drop constraint `" + nameOrSchema + "`: " + cause.getMessage());
        this.nameOrSchema = nameOrSchema;
        this.constraint = null;
    }

    public static DropConstraintFailureException constraintDropFailed(String constraintName, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N12)
                .withParam(GqlParams.StringParam.constrDescrOrName, constraintName)
                .build();

        return new DropConstraintFailureException(gql, constraintName, cause);
    }

    public static DropConstraintFailureException constraintDropFailed(
            ConstraintDescriptor constraint, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N12)
                .withParam(GqlParams.StringParam.constrDescrOrName, constraint.getName())
                .build();

        return new DropConstraintFailureException(gql, constraint, cause);
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        String message;
        if (constraint != null) {
            message = "Unable to drop constraint on " + constraint.userDescription(tokenNameLookup) + ": ";

        } else if (nameOrSchema != null) {
            message = "Unable to drop constraint `" + nameOrSchema + "`: ";
        } else {
            return getMessage();
        }

        Throwable cause = getCause();
        if (cause instanceof KernelException exception) {
            message += exception.getUserMessage(tokenNameLookup);
        } else {
            message += cause.getMessage();
        }

        return message;
    }
}
