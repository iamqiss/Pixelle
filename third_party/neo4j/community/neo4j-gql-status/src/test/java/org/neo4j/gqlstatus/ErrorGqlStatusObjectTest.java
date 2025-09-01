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
package org.neo4j.gqlstatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ErrorGqlStatusObjectTest {
    @Test
    void testGetOldCauseMessage() {
        var gql1 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                .withParam(GqlParams.StringParam.option1, "blabla")
                .withParam(GqlParams.StringParam.option2, "blabla")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G10)
                        .build())
                .build();
        var gql2 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_00000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_00001)
                        .build())
                .build();
        var someOtherGql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, "some db")
                .build();
        var oldMessage = "this is an old message";

        // This mocks such that the new gql message is always returned
        try (var errorMessageHolder = mockStatic(ErrorMessageHolder.class)) {
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(someOtherGql, oldMessage))
                    .thenReturn(someOtherGql.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql1, someOtherGql.toString()))
                    .thenReturn(gql1.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql2, gql1.toString()))
                    .thenReturn(gql2.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql2, gql2.toString()))
                    .thenReturn(gql2.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getOldCauseMessage(any()))
                    .thenCallRealMethod();

            var exWithOut = new ExceptionWithoutCause(someOtherGql, oldMessage);
            var exceptionWithCause1 = new ExceptionWithCause(gql1, exWithOut);
            var exceptionWithCause2 = new ExceptionWithCause(gql2, exceptionWithCause1);
            var exceptionWithCause3 = new ExceptionWithCause(gql2, exceptionWithCause2);

            var oldEx1 = exceptionWithCause1.legacyMessage();
            var oldEx2 = exceptionWithCause2.legacyMessage();
            var oldEx3 = exceptionWithCause3.legacyMessage();

            var gqlEx1 = exceptionWithCause1.getMessage();
            var gqlEx2 = exceptionWithCause2.getMessage();
            var gqlEx3 = exceptionWithCause3.getMessage();

            assertEquals(oldEx1, oldMessage);
            assertEquals(oldEx2, oldMessage);
            assertEquals(oldEx3, oldMessage);

            assertNotEquals(oldEx1, gqlEx1);
            assertNotEquals(oldEx2, gqlEx2);
            assertNotEquals(oldEx3, gqlEx3);
            assertNotEquals(gqlEx1, gqlEx2);

            assertEquals(gqlEx3, gqlEx2); // equality since gql2 is in both exceptionWithCause2 and exceptionWithCause3
        }
    }

    @Test
    void testDefaultStatusDescription() {
        var errorWithoutGql = new ExceptionWithoutCause(null, "message");
        assertEquals(
                "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details.",
                errorWithoutGql.statusDescription());
    }

    @Test
    void testAdjustPosition() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .atPosition(1, 2, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .atPosition(1, 2, 3)
                        .withParam(GqlParams.StringParam.valueType, "test")
                        .build())
                .build();
        ((ErrorGqlStatusObjectImplementation) gql).adjustPosition(1, 2, 3, 4, 5, 6);

        assertEquals(gql.gqlStatus(), "51N00");
        assertTrue(gql.diagnosticRecord().containsKey("_position"));
        var pos = (Map<String, Integer>) gql.diagnosticRecord().get("_position");
        assertEquals(pos.get("line"), 4);
        assertEquals(pos.get("column"), 5);
        assertEquals(pos.get("offset"), 6);

        assertTrue(gql.cause().isPresent());
        var cause = gql.cause().get();

        assertEquals(cause.gqlStatus(), "51N15");
        assertTrue(cause.diagnosticRecord().containsKey("_position"));
        var posInCause = (Map<String, Integer>) cause.diagnosticRecord().get("_position");
        assertEquals(posInCause.get("line"), 4);
        assertEquals(posInCause.get("column"), 5);
        assertEquals(posInCause.get("offset"), 6);

        assertFalse(cause.cause().isPresent());
    }

    @Test
    void testAdjustPositionChainOf3() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .atPosition(1, 2, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .atPosition(1, 2, 3)
                        .withParam(GqlParams.StringParam.valueType, "test")
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                                .atPosition(1, 2, 3)
                                .build())
                        .build())
                .build();
        ((ErrorGqlStatusObjectImplementation) gql).adjustPosition(1, 2, 3, 4, 5, 6);

        assertEquals(gql.gqlStatus(), "51N00");
        assertTrue(gql.diagnosticRecord().containsKey("_position"));
        var pos = (Map<String, Integer>) gql.diagnosticRecord().get("_position");
        assertEquals(pos.get("line"), 4);
        assertEquals(pos.get("column"), 5);
        assertEquals(pos.get("offset"), 6);

        assertTrue(gql.cause().isPresent());
        var cause = gql.cause().get();

        assertEquals(cause.gqlStatus(), "51N15");
        assertTrue(cause.diagnosticRecord().containsKey("_position"));
        var posInCause = (Map<String, Integer>) cause.diagnosticRecord().get("_position");
        assertEquals(posInCause.get("line"), 4);
        assertEquals(posInCause.get("column"), 5);
        assertEquals(posInCause.get("offset"), 6);

        assertTrue(cause.cause().isPresent());
        var causeCause = cause.cause().get();

        assertEquals(causeCause.gqlStatus(), "50N42");
        assertTrue(causeCause.diagnosticRecord().containsKey("_position"));
        var posInCauseCause =
                (Map<String, Integer>) causeCause.diagnosticRecord().get("_position");
        assertEquals(posInCauseCause.get("line"), 4);
        assertEquals(posInCauseCause.get("column"), 5);
        assertEquals(posInCauseCause.get("offset"), 6);

        assertFalse(causeCause.cause().isPresent());
    }

    @Test
    void testAdjustPositionNotInCause() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .atPosition(1, 2, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .atPosition(1, 2, 4)
                        .withParam(GqlParams.StringParam.valueType, "test")
                        .build())
                .build();
        ((ErrorGqlStatusObjectImplementation) gql).adjustPosition(1, 2, 3, 4, 5, 6);

        assertEquals(gql.gqlStatus(), "51N00");
        assertTrue(gql.diagnosticRecord().containsKey("_position"));
        var pos = (Map<String, Integer>) gql.diagnosticRecord().get("_position");
        assertEquals(pos.get("line"), 4);
        assertEquals(pos.get("column"), 5);
        assertEquals(pos.get("offset"), 6);

        assertTrue(gql.cause().isPresent());
        var cause = gql.cause().get();

        assertEquals(cause.gqlStatus(), "51N15");
        assertTrue(cause.diagnosticRecord().containsKey("_position"));
        var posInCause = (Map<String, Integer>) cause.diagnosticRecord().get("_position");
        assertEquals(posInCause.get("line"), 1);
        assertEquals(posInCause.get("column"), 2);
        assertEquals(posInCause.get("offset"), 4);

        assertFalse(cause.cause().isPresent());
    }

    @Test
    void testAdjustPositionOnlyInCause() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .atPosition(1, 1, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .atPosition(1, 2, 3)
                        .withParam(GqlParams.StringParam.valueType, "test")
                        .build())
                .build();
        ((ErrorGqlStatusObjectImplementation) gql).adjustPosition(1, 2, 3, 4, 5, 6);

        assertEquals(gql.gqlStatus(), "51N00");
        assertTrue(gql.diagnosticRecord().containsKey("_position"));
        var pos = (Map<String, Integer>) gql.diagnosticRecord().get("_position");
        assertEquals(pos.get("line"), 1);
        assertEquals(pos.get("column"), 1);
        assertEquals(pos.get("offset"), 3);

        assertTrue(gql.cause().isPresent());
        var cause = gql.cause().get();

        assertEquals(cause.gqlStatus(), "51N15");
        assertTrue(cause.diagnosticRecord().containsKey("_position"));
        var posInCause = (Map<String, Integer>) cause.diagnosticRecord().get("_position");
        assertEquals(posInCause.get("line"), 4);
        assertEquals(posInCause.get("column"), 5);
        assertEquals(posInCause.get("offset"), 6);

        assertFalse(cause.cause().isPresent());
    }

    @Test
    void testAdjustPositionUpdateNone() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .atPosition(1, 1, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .atPosition(1, 1, 4)
                        .withParam(GqlParams.StringParam.valueType, "test")
                        .build())
                .build();
        ((ErrorGqlStatusObjectImplementation) gql).adjustPosition(1, 2, 3, 4, 5, 6);

        assertEquals(gql.gqlStatus(), "51N00");
        assertTrue(gql.diagnosticRecord().containsKey("_position"));
        var pos = (Map<String, Integer>) gql.diagnosticRecord().get("_position");
        assertEquals(pos.get("line"), 1);
        assertEquals(pos.get("column"), 1);
        assertEquals(pos.get("offset"), 3);

        assertTrue(gql.cause().isPresent());
        var cause = gql.cause().get();

        assertEquals(cause.gqlStatus(), "51N15");
        assertTrue(cause.diagnosticRecord().containsKey("_position"));
        var posInCause = (Map<String, Integer>) cause.diagnosticRecord().get("_position");
        assertEquals(posInCause.get("line"), 1);
        assertEquals(posInCause.get("column"), 1);
        assertEquals(posInCause.get("offset"), 4);

        assertFalse(cause.cause().isPresent());
    }

    static class ExceptionWithoutCause extends GqlException {
        private final ErrorGqlStatusObject gqlStatusObject;
        private final String oldMessage;

        ExceptionWithoutCause(ErrorGqlStatusObject gqlStatusObject, String oldMessage) {
            super(ErrorMessageHolder.getMessage(gqlStatusObject, oldMessage));
            this.gqlStatusObject = gqlStatusObject;
            this.oldMessage = oldMessage;
        }

        @Override
        public String legacyMessage() {
            return oldMessage;
        }

        @Override
        public ErrorGqlStatusObject gqlStatusObject() {
            return gqlStatusObject;
        }
    }

    static class ExceptionWithCause extends GqlException {
        private final ErrorGqlStatusObject gqlStatusObject;
        private final String oldMessage;

        public ExceptionWithCause(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
            super(ErrorMessageHolder.getMessage(gqlStatusObject, cause.getMessage()));
            this.gqlStatusObject = gqlStatusObject;
            // This logic is what we test
            this.oldMessage = ErrorMessageHolder.getOldCauseMessage(cause);
        }

        @Override
        public String legacyMessage() {
            return oldMessage;
        }

        @Override
        public ErrorGqlStatusObject gqlStatusObject() {
            return gqlStatusObject;
        }
    }
}
