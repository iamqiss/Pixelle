/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral

class ListComprehensionParserTest extends AstParsingTestBase {

  test("tests") {
    "[ a in p WHERE a.foo > 123 ]" should parseTo[Expression] {
      ListComprehension(
        ExtractScope(
          varFor("a"),
          Some(GreaterThan(
            Property(varFor("a"), propName("foo"))(pos),
            SignedDecimalIntegerLiteral("123")(pos)
          )(pos)),
          None
        )(pos),
        varFor("p")
      )(pos)
    }

    "[ a in p | a.foo ]" should parseTo[Expression] {
      ListComprehension(
        ExtractScope(
          varFor("a"),
          None,
          Some(Property(varFor("a"), propName("foo"))(pos))
        )(pos),
        varFor("p")
      )(pos)
    }

    "[ a in p WHERE a.foo > 123 | a.foo ]" should parseTo[Expression] {
      ListComprehension(
        ExtractScope(
          varFor("a"),
          Some(GreaterThan(
            Property(varFor("a"), propName("foo"))(pos),
            SignedDecimalIntegerLiteral("123")(pos)
          )(pos)),
          Some(Property(varFor("a"), propName("foo"))(pos))
        )(pos),
        varFor("p")
      )(pos)
    }
  }
}
