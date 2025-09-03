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
package org.neo4j.graphdb.security;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Thrown when the database is asked to perform an action that is not authorized based on the AccessMode settings.
 *
 * For instance, if attempting to write with READ_ONLY rights.
 */
public class AuthorizationViolationException extends GqlRuntimeException implements Status.HasStatus {
    public static final String PERMISSION_DENIED = "Permission denied.";

    private final Status statusCode;

    @Deprecated
    public AuthorizationViolationException(String message, Status statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public AuthorizationViolationException(ErrorGqlStatusObject gqlStatusObject, String message, Status statusCode) {
        super(gqlStatusObject, message);
        this.statusCode = statusCode;
    }

    @Deprecated
    public AuthorizationViolationException(String message) {
        super(message);
        statusCode = Status.Security.Forbidden;
    }

    private AuthorizationViolationException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
        statusCode = Status.Security.Forbidden;
    }

    public static AuthorizationViolationException authorizationViolation(String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(gql, message);
    }

    public static AuthorizationViolationException permissionDeniedUnauthorized() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(gql, PERMISSION_DENIED, Status.Security.Unauthorized);
    }

    public static AuthorizationViolationException permissionDeniedForbidden() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(gql, PERMISSION_DENIED, Status.Security.Forbidden);
    }

    public static AuthorizationViolationException updatesWhenImpersonating() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(
                gql, "Not allowed to run updating system commands when impersonating a user.");
    }

    public static AuthorizationViolationException revokingImmutablePrivileges() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(gql, "Immutable privileges cannot be revoked.");
    }

    public static AuthorizationViolationException grantingImmutablePrivileges(String action) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new AuthorizationViolationException(gql, String.format("Permission cannot be granted for %s.", action));
    }

    public static AuthorizationViolationException droppingImmutableRoles() {
        return authorizationViolation(
                "Immutable roles cannot be dropped. Use `SHOW ROLES YIELD *` to see which roles are immutable.");
    }

    public static AuthorizationViolationException creatingImmutableRoles() {
        return authorizationViolation(
                "Immutable roles cannot be created. Try creating the role without the IMMUTABLE keyword.");
    }

    public static AuthorizationViolationException replacingImmutableRoles() {
        return authorizationViolation(
                "Immutable roles cannot be replaced. Use `SHOW ROLES YIELD *` to see which roles are immutable.");
    }

    public static AuthorizationViolationException renamingImmutableRoles() {
        return authorizationViolation(
                "Immutable roles cannot be renamed. Use `SHOW ROLES YIELD *` to see which roles are immutable.");
    }

    public static AuthorizationViolationException assigningMutablePrivilegesToImmutableRole() {
        return authorizationViolation(
                "Only immutable privileges can be assigned to an immutable role. Try `GRANT/DENY IMMUTABLE` instead.");
    }

    public static AuthorizationViolationException copyingRoleWithMutablePrivileges(String role) {
        return authorizationViolation(
                "'$role' cannot be copied to an immutable role because '$role' has one or more non-immutable privileges. Immutable roles can only contain immutable privileges. Use `SHOW ROLE $role PRIVILEGES AS COMMANDS YIELD *` to inspect $role's privileges."
                        .replace("$role", role));
    }

    public static AuthorizationViolationException copyingRoleWithImmutablePrivileges(String role) {
        return authorizationViolation(
                "The role '%s' cannot be copied because it has one or more Immutable Privileges assigned to it. Permission cannot be granted for ASSIGN IMMUTABLE PRIVILEGE."
                        .formatted(role));
    }

    /** The Neo4j status code associated with this exception type. */
    @Override
    public Status status() {
        return statusCode;
    }
}
