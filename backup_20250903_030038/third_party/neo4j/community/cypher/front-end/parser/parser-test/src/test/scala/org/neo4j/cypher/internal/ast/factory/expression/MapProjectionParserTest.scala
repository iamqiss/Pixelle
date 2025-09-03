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

import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VariableSelector

class MapProjectionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("testIdentifierCanContainASCII") {
    "abc{}" should parseTo[MapProjection](
      MapProjection(varFor("abc"), Seq.empty)(pos)
    )

    "abc{.id}" should parseTo[MapProjection](
      MapProjection(
        varFor("abc"),
        Seq(PropertySelector(PropertyKeyName("id")(pos))(pos))
      )(pos)
    )

    "abc{id}" should parseTo[MapProjection](
      MapProjection(
        varFor("abc"),
        Seq(VariableSelector(varFor("id"))(pos))
      )(pos)
    )

    "abc { id : 42 }" should parseTo[MapProjection](
      MapProjection(
        varFor("abc"),
        Seq(LiteralEntry(PropertyKeyName("id")(pos), SignedDecimalIntegerLiteral("42")(pos))(pos))
      )(pos)
    )

    "abc { `a p a` : 42 }" should parseTo[MapProjection](
      MapProjection(
        varFor("abc"),
        Seq(LiteralEntry(PropertyKeyName("a p a")(pos), SignedDecimalIntegerLiteral("42")(pos))(pos))
      )(pos)
    )

    "abc { id : 42, .foo, bar }" should parseTo[MapProjection](
      MapProjection(
        varFor("abc"),
        Seq(
          LiteralEntry(PropertyKeyName("id")(pos), SignedDecimalIntegerLiteral("42")(pos))(pos),
          PropertySelector(PropertyKeyName("foo")(pos))(pos),
          VariableSelector(varFor("bar"))(pos)
        )
      )(pos)
    )
  }

  test("map with non-string key should not parse") {
    "abc {42: 'value'}" should notParse[MapProjection].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Encountered \" <UNSIGNED_DECIMAL_INTEGER> \"42\"\" at line 1, column 6.")
      case _ => _.withMessage(
          """Invalid input '42': expected an identifier, '.' or '}' (line 1, column 6 (offset: 5))
            |"abc {42: 'value'}"
            |      ^""".stripMargin
        )
    }
  }

  test("map without comma separation should not parse") {
    "abc {key1: 'value' key2: 42}" should notParse[MapProjection].in {
      case Cypher5JavaCc => _.withMessageStart("Encountered \" <IDENTIFIER> \"key2\"\" at line 1, column 20.")
      case _ => _.withMessage(
          """Invalid input 'key2': expected an expression, ',' or '}' (line 1, column 20 (offset: 19))
            |"abc {key1: 'value' key2: 42}"
            |                    ^""".stripMargin
        )
    }
  }

  test("map with invalid start comma should not parse") {
    "abc {, key: 'value'}" should notParse[MapProjection].in {
      case Cypher5JavaCc => _.withMessageStart("Encountered \" \",\" \",\"\" at line 1, column 6.")
      case _ => _.withMessage(
          """Invalid input ',': expected an identifier, '.' or '}' (line 1, column 6 (offset: 5))
            |"abc {, key: 'value'}"
            |      ^""".stripMargin
        )
    }
  }
}
