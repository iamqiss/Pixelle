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
package org.neo4j.kernel.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.General.InvalidArguments;

import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;

public class QueryExecutionKernelExceptionTest {

    @Test
    void testGqlExceptionWrapping() {
        var gqlException = InvalidArgumentsException.internalAlterServer("server");
        var translatedGqlException = QueryExecutionKernelException.wrapError(gqlException);
        assertEquals("50N00", translatedGqlException.gqlStatus());
        assertEquals(InvalidArguments, translatedGqlException.status());
        assertEquals("Server 'server' can't be altered: must specify options", translatedGqlException.getMessage());
        assertEquals(gqlException, translatedGqlException.getCause());
        // No gql cause added since are replacing the previous exception on the gql cause chain
        assertTrue(translatedGqlException.gqlStatusObject().cause().isEmpty());

        var userException = translatedGqlException.asUserException();
        assertEquals("50N00", userException.gqlStatus());
        assertEquals("Neo.ClientError.General.InvalidArguments", userException.getStatusCode());
        assertEquals("Server 'server' can't be altered: must specify options", userException.getMessage());

        // No gql cause added since are replacing the previous exception on the gql cause chain
        assertTrue(userException.gqlStatusObject().cause().isEmpty());
    }

    @Test
    void testGqlExceptionWrappingWithCause() {
        var gqlException = InvalidArgumentsException.requiresPositiveInteger("the_option", -1);
        var translatedGqlException = QueryExecutionKernelException.wrapError(gqlException);
        assertEquals("22003", translatedGqlException.gqlStatus());
        assertEquals(InvalidArguments, translatedGqlException.status());
        assertEquals(
                "Option `the_option` requires positive integer argument, got `-1`",
                translatedGqlException.getMessage());

        assertTrue(gqlException.cause().isPresent());
        var gqlCause = gqlException.cause().get();
        assertEquals("22N02", gqlCause.gqlStatus());
        assertEquals(
                "22N02: Expected 'the_option' to be a positive number but found -1 instead.", gqlCause.getMessage());

        assertTrue(gqlCause.cause().isEmpty());

        assertEquals(gqlException, translatedGqlException.getCause());
        // Gql cause added since is the same as on the wrapped exception
        assertTrue(translatedGqlException.gqlStatusObject().cause().isPresent());
        assertEquals(
                gqlException.gqlStatusObject().cause().orElse(null),
                translatedGqlException.cause().orElse(null));

        var userException = translatedGqlException.asUserException();
        assertEquals("22003", userException.gqlStatus());
        assertEquals("Neo.ClientError.General.InvalidArguments", userException.getStatusCode());
        assertEquals("Option `the_option` requires positive integer argument, got `-1`", userException.getMessage());

        assertTrue(userException.cause().isPresent());
        var cause = userException.cause().get();
        assertEquals("22N02", cause.gqlStatus());
        assertEquals("22N02: Expected 'the_option' to be a positive number but found -1 instead.", cause.getMessage());

        assertTrue(cause.cause().isEmpty());
    }

    @Test
    void testGqlExceptionWrappingWithJavaCause() {
        var causeException = InvalidArgumentsException.requiresPositiveInteger("the_option", -1);
        var gqlException = TestUnknownGqlException.create("I am an exception", causeException);
        var translatedGqlException = QueryExecutionKernelException.wrapError(gqlException);
        assertEquals("50N42", translatedGqlException.gqlStatus());
        assertEquals(Status.General.UnknownError, translatedGqlException.status());
        assertEquals("I am an exception", translatedGqlException.getMessage());
        // It should set a gql cause
        assertEquals(gqlException, translatedGqlException.getCause());
        assertTrue(translatedGqlException.gqlStatusObject().cause().isPresent());

        var gqlCause = translatedGqlException.gqlStatusObject().cause().get();
        assertEquals("22003", gqlCause.gqlStatus());
        assertEquals("22003: The numeric value $value is outside the required range.", gqlCause.getMessage());

        assertTrue(gqlCause.cause().isPresent());
        var secondGqlCause = gqlCause.cause().get();
        assertEquals("22N02", secondGqlCause.gqlStatus());
        assertEquals(
                "22N02: Expected 'the_option' to be a positive number but found -1 instead.",
                secondGqlCause.getMessage());

        // No further causes should be here
        assertTrue(secondGqlCause.cause().isEmpty());

        var userException = translatedGqlException.asUserException();
        assertEquals("50N42", userException.gqlStatus());
        assertEquals("Neo.DatabaseError.General.UnknownError", userException.getStatusCode());
        assertEquals("I am an exception", userException.getMessage());

        assertTrue(userException.cause().isPresent());

        var userCause = translatedGqlException.gqlStatusObject().cause().get();
        assertEquals("22003", userCause.gqlStatus());
        assertEquals("22003: The numeric value $value is outside the required range.", userCause.getMessage());

        assertTrue(userCause.cause().isPresent());
        var secondUserCause = userCause.cause().get();
        assertEquals("22N02", secondUserCause.gqlStatus());
        assertEquals(
                "22N02: Expected 'the_option' to be a positive number but found -1 instead.",
                secondUserCause.getMessage());

        // No further causes should be here
        assertTrue(secondUserCause.cause().isEmpty());
    }

    @Test
    void testNonGqlExceptionWrapping() {
        var notGqlException = new InvalidArgumentsException("message");
        var translatedGqlException = QueryExecutionKernelException.wrapError(notGqlException);
        assertEquals("50N42", translatedGqlException.gqlStatus());
        assertEquals(InvalidArguments, translatedGqlException.status());
        assertEquals("message", translatedGqlException.getMessage());
        // It should set a cause since we are not wrapping a gql exception
        assertEquals(notGqlException, translatedGqlException.getCause());
        assertNull(translatedGqlException.gqlStatusObject());

        var userException = translatedGqlException.asUserException();
        assertEquals("50N42", userException.gqlStatus());
        assertEquals("Neo.ClientError.General.InvalidArguments", userException.getStatusCode());
        assertEquals("message", userException.getMessage());
    }

    private static class TestUnknownGqlException extends GqlException implements Status.HasStatus {

        private TestUnknownGqlException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
            super(gqlStatusObject, message, cause);
        }

        public static TestUnknownGqlException create(String message, Throwable cause) {
            var gqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                    .build();
            return new TestUnknownGqlException(gqlStatusObject, message, cause);
        }

        @Override
        public Status status() {
            return Status.General.UnknownError;
        }
    }
}
