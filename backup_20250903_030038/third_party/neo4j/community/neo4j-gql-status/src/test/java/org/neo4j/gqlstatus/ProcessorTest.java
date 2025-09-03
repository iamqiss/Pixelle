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

import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

public class ProcessorTest {

    private static String expectedIDENT(String s) {
        return "`" + s + "`";
    }

    private static String expectedSTRLIT(String s) {
        return "'" + s + "'";
    }

    private static String expectedUPPER(String s) {
        return s.toUpperCase(Locale.ROOT);
    }

    private static String expectedCALLABLE_IDENT(String s) {
        return s + "()";
    }

    GqlParams.ListProcessor processorNelistStrlit = new GqlParams.NELIST().withInner(new GqlParams.STRLIT());
    GqlParams.ListProcessor processorNelistCallableIdent =
            new GqlParams.NELIST().withInner(new GqlParams.CALLABLE_IDENT());
    GqlParams.ListProcessor processorNelistIdent = new GqlParams.NELIST().withInner(new GqlParams.IDENT());
    GqlParams.ListProcessor processorNelistVerbatim = new GqlParams.NELIST().withInner(new GqlParams.VERBATIM());
    GqlParams.Processor processorUpperCallableIdent = new GqlParams.UPPER().withInner(new GqlParams.CALLABLE_IDENT());
    GqlParams.Processor processorUpperIdent = new GqlParams.UPPER().withInner(new GqlParams.IDENT());
    GqlParams.Processor processorUpperStrlit = new GqlParams.UPPER().withInner(new GqlParams.STRLIT());
    GqlParams.Processor processorUpper = new GqlParams.UPPER();
    GqlParams.Processor processorParam = new GqlParams.PARAM();
    GqlParams.Processor processorIdent = new GqlParams.IDENT();
    GqlParams.Processor processorStrlit = new GqlParams.STRLIT();
    GqlParams.Processor processorCallableIdent = new GqlParams.CALLABLE_IDENT();

    // Fixing changes to format in these tests should be as easy as changing the corresponding
    // expectedPROCESSOR()-method
    @Test
    public void testIDENT() {
        assertEquals(expectedIDENT("abc"), processorIdent.process("abc"));
        assertEquals(expectedIDENT("abc()"), processorIdent.process("abc()"));
        assertEquals(expectedIDENT("{ %s } abc"), processorIdent.process("{ %s } abc"));
        assertEquals(expectedIDENT("`abc`"), processorIdent.process("`abc`"));
    }

    @Test
    public void testCALLABLE_IDENT() {
        assertEquals(expectedCALLABLE_IDENT("abc"), processorCallableIdent.process("abc"));
        assertEquals(expectedCALLABLE_IDENT("abc()"), processorCallableIdent.process("abc()"));
        assertEquals(expectedCALLABLE_IDENT("{ %s } abc"), processorCallableIdent.process("{ %s } abc"));
        assertEquals(expectedCALLABLE_IDENT("'abc'"), processorCallableIdent.process("'abc'"));
    }

    @Test
    public void testSTRLIT() {
        assertEquals(expectedSTRLIT("abc"), processorStrlit.process("abc"));
        assertEquals(expectedSTRLIT("abc()"), processorStrlit.process("abc()"));
        assertEquals(expectedSTRLIT("{ %s } abc"), processorStrlit.process("{ %s } abc"));
        assertEquals(expectedSTRLIT("'abc'"), processorStrlit.process("'abc'"));
    }

    @Test
    public void testPARAM() {
        assertEquals("$" + expectedIDENT("abc"), processorParam.process("abc"));
        assertEquals("$" + expectedIDENT("abc()"), processorParam.process("abc()"));
        assertEquals("$" + expectedIDENT("{ %s } abc"), processorParam.process("{ %s } abc"));
        assertEquals("$" + expectedIDENT("'abc'"), processorParam.process("'abc'"));
    }

    @Test
    public void testUPPER() {
        assertEquals(expectedUPPER("abc"), processorUpper.process("abc"));
        assertEquals(expectedUPPER("abc()"), processorUpper.process("abc()"));
        assertEquals(expectedUPPER("{ %s } abc"), processorUpper.process("{ %s } abc"));
        assertEquals(expectedUPPER("'abc'"), processorUpper.process("'abc'"));
    }

    @Test
    public void testUPPER_with_STRLIT() {
        assertEquals(expectedSTRLIT(expectedUPPER("abc")), processorUpperStrlit.process("abc"));
        assertEquals(expectedSTRLIT(expectedUPPER("abc()")), processorUpperStrlit.process("abc()"));
        assertEquals(expectedSTRLIT(expectedUPPER("{ %s } abc")), processorUpperStrlit.process("{ %s } abc"));
        assertEquals(expectedSTRLIT(expectedUPPER("'abc'")), processorUpperStrlit.process("'abc'"));
    }

    @Test
    public void testUPPER_with_IDENT() {
        assertEquals(expectedUPPER(expectedIDENT("abc")), processorUpperIdent.process("abc"));
        assertEquals(expectedUPPER(expectedIDENT("abc()")), processorUpperIdent.process("abc()"));
        assertEquals(expectedUPPER(expectedIDENT("{ %s } abc")), processorUpperIdent.process("{ %s } abc"));
        assertEquals(expectedUPPER(expectedIDENT("'abc'")), processorUpperIdent.process("'abc'"));
    }

    @Test
    public void testUPPER_with_CALLABLE_IDENT() {
        assertEquals(expectedUPPER(expectedCALLABLE_IDENT("abc")), processorUpperCallableIdent.process("abc"));
        assertEquals(expectedUPPER(expectedCALLABLE_IDENT("abc()")), processorUpperCallableIdent.process("abc()"));
        assertEquals(
                expectedUPPER(expectedCALLABLE_IDENT("{ %s } abc")), processorUpperCallableIdent.process("{ %s } abc"));
        assertEquals(expectedUPPER(expectedCALLABLE_IDENT("'abc'")), processorUpperCallableIdent.process("'abc'"));
    }

    @Test
    public void testNELIST_with_VERBATIM() {
        assertEquals("abc", processorNelistVerbatim.process(List.of("abc"), null));
        assertEquals("abc(), cbd()", processorNelistVerbatim.process(List.of("abc()", "cbd()"), null));
        assertEquals(
                "{ %s } abc, `abc`, a", processorNelistVerbatim.process(List.of("{ %s } abc", "`abc`", "a"), null));
    }

    @Test
    public void testNELIST_with_IDENT() {
        assertEquals(expectedIDENT("abc"), processorNelistIdent.process(List.of("abc"), null));
        assertEquals(
                expectedIDENT("abc()") + ", " + expectedIDENT("cbd()"),
                processorNelistIdent.process(List.of("abc()", "cbd()"), null));
        assertEquals(
                expectedIDENT("{ %s } abc") + ", " + expectedIDENT("`abc`") + ", " + expectedIDENT("a"),
                processorNelistIdent.process(List.of("{ %s } abc", "`abc`", "a"), null));
    }

    @Test
    public void testNELIST_with_CALLABLE_IDENT() {
        assertEquals(expectedCALLABLE_IDENT("abc"), processorNelistCallableIdent.process(List.of("abc"), null));
        assertEquals(
                expectedCALLABLE_IDENT("abc()") + ", " + expectedCALLABLE_IDENT("cbd()"),
                processorNelistCallableIdent.process(List.of("abc()", "cbd()"), null));
        assertEquals(
                expectedCALLABLE_IDENT("{ %s } abc") + ", " + expectedCALLABLE_IDENT("`abc`") + ", "
                        + expectedCALLABLE_IDENT("a"),
                processorNelistCallableIdent.process(List.of("{ %s } abc", "`abc`", "a"), null));
    }

    @Test
    public void testNELIST_with_STRLIT() {
        assertEquals(expectedSTRLIT("abc"), processorNelistStrlit.process(List.of("abc"), null));
        assertEquals(
                expectedSTRLIT("abc()") + ", " + expectedSTRLIT("cbd()"),
                processorNelistStrlit.process(List.of("abc()", "cbd()"), null));
        assertEquals(
                expectedSTRLIT("{ %s } abc") + ", " + expectedSTRLIT("`abc`") + ", " + expectedSTRLIT("a"),
                processorNelistStrlit.process(List.of("{ %s } abc", "`abc`", "a"), null));
    }

    @Test
    public void testANDEDNELIST_with_VERBATIM() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertEquals("abc", processorNelistVerbatim.process(List.of("abc"), joinStyle));
        assertEquals("abc() and cbd()", processorNelistVerbatim.process(List.of("abc()", "cbd()"), joinStyle));
        assertEquals(
                "{ %s } abc, `abc` and a",
                processorNelistVerbatim.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle));
    }

    @Test
    public void testANDEDNELIST_with_IDENT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertEquals(expectedIDENT("abc"), processorNelistIdent.process(List.of("abc"), joinStyle));
        assertEquals(
                expectedIDENT("abc()") + " and " + expectedIDENT("cbd()"),
                processorNelistIdent.process(List.of("abc()", "cbd()"), joinStyle));
        assertEquals(
                expectedIDENT("{ %s } abc") + ", " + expectedIDENT("`abc`") + " and " + expectedIDENT("a"),
                processorNelistIdent.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle));
    }

    @Test
    public void testANDEDNELIST_with_CALLABLE_IDENT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;
        assertEquals(expectedCALLABLE_IDENT("abc"), processorNelistCallableIdent.process(List.of("abc"), joinStyle));
        assertEquals(
                expectedCALLABLE_IDENT("abc()") + " and " + expectedCALLABLE_IDENT("cbd()"),
                processorNelistCallableIdent.process(List.of("abc()", "cbd()"), joinStyle));
        assertEquals(
                expectedCALLABLE_IDENT("{ %s } abc") + ", " + expectedCALLABLE_IDENT("`abc`") + " and "
                        + expectedCALLABLE_IDENT("a"),
                processorNelistCallableIdent.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle));
    }

    @Test
    public void testANDEDNELIST_with_STRLIT() {
        var joinStyle = GqlParams.JoinStyle.ANDED;

        assertEquals(expectedSTRLIT("abc"), processorNelistStrlit.process(List.of("abc"), joinStyle));
        assertEquals(
                expectedSTRLIT("abc()") + " and " + expectedSTRLIT("cbd()"),
                processorNelistStrlit.process(List.of("abc()", "cbd()"), joinStyle));
        assertEquals(
                expectedSTRLIT("{ %s } abc") + ", " + expectedSTRLIT("`abc`") + " and " + expectedSTRLIT("a"),
                processorNelistStrlit.process(List.of("{ %s } abc", "`abc`", "a"), joinStyle));
    }

    // More readable, but tedious fixing of format-changes
    @Test
    public void testLiteralMessages() {
        assertEquals("", processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.ANDED));
        assertEquals("", processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.ORED));
        assertEquals("", processorNelistVerbatim.process(List.of(""), GqlParams.JoinStyle.COMMAD));
        assertEquals("'abc'", processorStrlit.process("abc"));
        assertEquals("`abc`", processorIdent.process("abc"));
        assertEquals("$`abc`", processorParam.process("abc"));
        assertEquals("abc()", processorCallableIdent.process("abc"));
        assertEquals(
                "abc, def, ghi",
                processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.COMMAD));
        assertEquals(
                "abc, def or ghi",
                processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ORED));
        assertEquals(
                "abc, def and ghi",
                processorNelistVerbatim.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED));
        assertEquals(
                "abc(), def() and ghi()",
                processorNelistCallableIdent.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED));
        assertEquals(
                "`abc`, `def` and `ghi`",
                processorNelistIdent.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED));
        assertEquals(
                "'abc', 'def' and 'ghi'",
                processorNelistStrlit.process(List.of("abc", "def", "ghi"), GqlParams.JoinStyle.ANDED));
        assertEquals("ABC()", processorUpperCallableIdent.process("abc"));
        assertEquals("`ABC`", processorUpperIdent.process("abc"));
        assertEquals("'ABC'", processorUpperStrlit.process("abc"));
    }
}
