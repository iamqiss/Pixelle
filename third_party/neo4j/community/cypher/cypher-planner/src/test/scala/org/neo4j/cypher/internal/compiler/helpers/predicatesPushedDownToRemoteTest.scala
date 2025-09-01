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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class predicatesPushedDownToRemoteTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should not allow expressions with multiple dependencies") {
    predicatesPushedDownToRemote(greaterThan(prop("var1", "prop1"), prop("var2", "prop2"))) shouldBe false
  }

  test("should not allow expressions which don't use properties") {
    predicatesPushedDownToRemote(equals(varFor("a"), literal(5))) shouldBe false
    predicatesPushedDownToRemote(hasLabels("a", "A")) shouldBe false
    predicatesPushedDownToRemote(hasTypes("r", "R")) shouldBe false
    predicatesPushedDownToRemote(listComprehension(
      v"n",
      nodes(varLengthPathExpression(v"a", v"anon_3", v"b")),
      None,
      None
    ))
  }

  test("should allow contains with only parameters, properties and literals") {
    predicatesPushedDownToRemote(contains(prop("a", "subStr"), cachedNodeProp("a", "str"))) shouldBe true
    predicatesPushedDownToRemote(contains(cachedRelProp("a", "str"), literalString("42"))) shouldBe true
    predicatesPushedDownToRemote(contains(cachedNodeProp("a", "str"), parameter("str", CTString))) shouldBe true

    predicatesPushedDownToRemote(contains(cachedNodeProp("a", "str"), collect(prop("a", "str2")))) shouldBe false
  }

  test("should allow endsWith with only parameters, properties and literals") {
    predicatesPushedDownToRemote(endsWith(prop("a", "subStr"), cachedNodeProp("a", "str"))) shouldBe true
    predicatesPushedDownToRemote(endsWith(cachedRelProp("a", "str"), literalString("42"))) shouldBe true
    predicatesPushedDownToRemote(endsWith(cachedNodeProp("a", "str"), parameter("str", CTString))) shouldBe true

    predicatesPushedDownToRemote(endsWith(cachedNodeProp("a", "str"), collect(prop("a", "str2")))) shouldBe false

  }

  test("should allow startsWith with only parameters, properties and literals") {
    predicatesPushedDownToRemote(startsWith(prop("a", "subStr"), cachedNodeProp("a", "str"))) shouldBe true
    predicatesPushedDownToRemote(startsWith(cachedRelProp("a", "str"), literalString("42"))) shouldBe true
    predicatesPushedDownToRemote(startsWith(cachedNodeProp("a", "str"), parameter("str", CTString))) shouldBe true

    predicatesPushedDownToRemote(startsWith(cachedNodeProp("a", "str"), collect(prop("a", "str2")))) shouldBe false
  }

  test("should allow less than with only parameters, properties and literals") {
    predicatesPushedDownToRemote(lessThan(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(lessThan(cachedRelProp("a", "num"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(lessThan(cachedNodeProp("a", "num"), parameter("num", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(lessThan(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow lessThanOrEqual with only parameters, properties and literals") {
    predicatesPushedDownToRemote(lessThanOrEqual(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(lessThanOrEqual(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(lessThanOrEqual(cachedNodeProp("a", "str"), parameter("str", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(lessThanOrEqual(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow greater than with only parameters, properties and literals") {
    predicatesPushedDownToRemote(greaterThan(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(greaterThan(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(greaterThan(cachedNodeProp("a", "str"), parameter("str", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(greaterThan(cachedNodeProp("a", "num"), countStar())) shouldBe false

  }

  test("should allow greaterThanOrEqual with only parameters, properties and literals") {
    predicatesPushedDownToRemote(greaterThanOrEqual(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(greaterThanOrEqual(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(
      greaterThanOrEqual(cachedNodeProp("a", "str"), parameter("str", CTInteger))
    ) shouldBe true

    predicatesPushedDownToRemote(greaterThanOrEqual(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow equals with only parameters, properties and literals") {
    predicatesPushedDownToRemote(equals(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(equals(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(equals(cachedNodeProp("a", "str"), parameter("str", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(equals(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow in with only parameters, properties and literals") {
    predicatesPushedDownToRemote(in(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(in(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(in(cachedNodeProp("a", "str"), parameter("str", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(in(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow not equals with only parameters, properties and literals") {
    predicatesPushedDownToRemote(notEquals(prop("a", "num"), cachedNodeProp("a", "num"))) shouldBe true
    predicatesPushedDownToRemote(notEquals(cachedRelProp("a", "str"), literalInt(42))) shouldBe true
    predicatesPushedDownToRemote(notEquals(cachedNodeProp("a", "str"), parameter("str", CTInteger))) shouldBe true

    predicatesPushedDownToRemote(notEquals(cachedNodeProp("a", "num"), countStar())) shouldBe false
  }

  test("should allow valid listliterals") {
    predicatesPushedDownToRemote(in(listOf(prop("n", "prop")), listOfInt(1))) shouldBe true
  }

  test("should allow anded property inequalities") {
    predicatesPushedDownToRemote(andedPropertyInequalities(
      greaterThan(prop("n", "prop"), literalInt(10)),
      lessThan(prop("n", "prop"), literalInt(100))
    )) shouldBe true
  }
}
