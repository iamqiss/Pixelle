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
package org.neo4j.dbms.systemgraph.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

public class DatabaseAllocationHintsTest {

    @Test
    void hintShouldBeSealed() {
        var hintClass = DatabaseAllocationHints.Hint.class;
        assertTrue(hintClass.isSealed(), "Hint class must be sealed!");
    }

    @Test
    void shouldReturnKeyForAllPermittedHintTypes() {
        var hintClass = DatabaseAllocationHints.Hint.class;
        var hintTypes = hintClass.getPermittedSubclasses();
        for (var hT : hintTypes) {
            var key = DatabaseAllocationHints.key(hT);
            assertThat(key).isNotEmpty();
        }
    }

    @Test
    void keysForAllPermittedHintTypesShouldBeUnique() {
        var hintClass = DatabaseAllocationHints.Hint.class;
        var hintTypes = hintClass.getPermittedSubclasses();
        var listOfKeys =
                Arrays.stream(hintTypes).map(DatabaseAllocationHints::key).toList();
        assertThat(listOfKeys).hasSameSizeAs(DatabaseAllocationHints.VALID_HINT_KEYS);
    }

    private static Stream<Arguments> validValues() {
        return Stream.of(
                Arguments.of(DatabaseWeight.KEY, Values.intValue(0), DatabaseWeight.class, 0),
                Arguments.of(DatabaseWeight.KEY, Values.intValue(100), DatabaseWeight.class, 100),
                Arguments.of(DatabaseWeight.KEY, Values.intValue(1), DatabaseWeight.class, 1));
    }

    @ParameterizedTest
    @MethodSource("validValues")
    void parseValidValuesShouldWork(
            String hintKey,
            AnyValue hintValue,
            Class<? extends DatabaseAllocationHints.Hint<?>> expectedHintClass,
            Object expectedValue) {
        var hint = DatabaseAllocationHints.createFromInput(hintKey, hintValue);
        assertThat(hint).isInstanceOf(expectedHintClass);
        assertThat(hint.getValue()).isEqualTo(expectedValue);
    }

    private static Stream<Arguments> invalidValues() {
        return Stream.of(
                Arguments.of(DatabaseWeight.KEY, Values.intValue(-1), DatabaseWeight.class, -1),
                Arguments.of(DatabaseWeight.KEY, Values.stringValue("hello"), DatabaseWeight.class, "hello"),
                Arguments.of(
                        DatabaseWeight.KEY,
                        Values.intValue(Integer.MIN_VALUE),
                        DatabaseWeight.class,
                        Integer.MIN_VALUE));
    }

    @ParameterizedTest
    @MethodSource("invalidValues")
    void parseInvalidValuesShouldThrow(
            String hintKey,
            AnyValue hintValue,
            Class<? extends DatabaseAllocationHints.Hint<?>> expectedHintClass,
            Object expectedValue) {
        assertThrows(IllegalArgumentException.class, () -> DatabaseAllocationHints.createFromInput(hintKey, hintValue));
    }

    @Test
    void parseUnknownKeysShouldThrow() {
        assertThrows(
                IllegalArgumentException.class,
                () -> DatabaseAllocationHints.createFromInput("_unknown_key_", Values.intValue(100)));
    }
}
