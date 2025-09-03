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
package org.neo4j.bolt.security.error;

import java.io.IOException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class AuthenticationException extends IOException implements Status.HasStatus, ErrorGqlStatusObject {
    private final Status status;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    @Deprecated
    public AuthenticationException(Status status) {
        this(status, status.code().description(), null);
    }

    public AuthenticationException(ErrorGqlStatusObject gqlStatusObject, Status status) {
        this(gqlStatusObject, status, status.code().description(), null);
    }

    @Deprecated
    public AuthenticationException(Status status, String message) {
        this(status, message, null);
    }

    public AuthenticationException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        this(gqlStatusObject, status, message, null);
    }

    @Deprecated
    public AuthenticationException(Status status, String message, Throwable e) {
        super(message, e);
        this.status = status;
        gqlStatusObject = null;
        oldMessage = message;
    }

    public AuthenticationException(ErrorGqlStatusObject gqlStatusObject, Status status, String message, Throwable e) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), e);
        this.status = status;
        this.gqlStatusObject = GqlHelper.getInnerGqlStatusObject(gqlStatusObject, e);
        oldMessage = message;
    }

    public static AuthenticationException unauthorized() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthenticationException(gql, Status.Security.Unauthorized);
    }

    @Override
    public String legacyMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
