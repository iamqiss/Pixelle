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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.exceptions.SyntaxException

/**
 * The aim of this class is to test parsing for all combinations of
 * IS and WHERE used in node patterns e.g. (WHERE IS WHERE WHERE IS)
 */
class IsWhereNodePatternParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  for {
    (maybeVariable, maybeVariableName) <-
      Seq(("", None), ("IS", Some("IS")), ("WHERE", Some("WHERE")))
  } yield {
    test(s"($maybeVariable)") {
      parsesTo[NodePattern](nodePat(maybeVariableName))
    }

    for {
      isOrWhere <- Seq("IS", "WHERE")
    } yield {
      test(s"($maybeVariable IS $isOrWhere)") {
        parsesTo[NodePattern](
          nodePat(
            maybeVariableName,
            labelExpression = Some(labelLeaf(isOrWhere, containsIs = true))
          )
        )
      }

      test(s"($maybeVariable WHERE $isOrWhere)") {
        parsesTo[NodePattern](
          nodePat(
            maybeVariableName,
            predicates = Some(varFor(isOrWhere))
          )
        )
      }

      for {
        isOrWhere2 <- Seq("IS", "WHERE")
      } yield {
        test(s"($maybeVariable IS $isOrWhere WHERE $isOrWhere2)") {
          parsesTo[NodePattern](
            nodePat(
              maybeVariableName,
              labelExpression = Some(labelLeaf(isOrWhere, containsIs = true)),
              predicates = Some(varFor(isOrWhere2))
            )
          )
        }

        test(s"($maybeVariable WHERE $isOrWhere IS $isOrWhere2)") {
          parsesTo[NodePattern](
            nodePat(
              maybeVariableName,
              predicates = Some(labelExpressionPredicate(
                varFor(isOrWhere),
                labelOrRelTypeLeaf(isOrWhere2, containsIs = true)
              ))
            )
          )
        }

        test(s"MATCH ($maybeVariable WHERE $isOrWhere WHERE $isOrWhere2) RETURN *") {
          failsParsing[Statements].in {
            case Cypher5JavaCc => _.withMessageStart("Invalid input")
            case _             => _.throws[SyntaxException]
          }
        }

        test(s"MATCH ($maybeVariable IS $isOrWhere IS $isOrWhere2) RETURN *") {
          failsParsing[Statements].in {
            case Cypher5JavaCc => _.withMessageStart("Invalid input")
            case _             => _.throws[SyntaxException]
          }
        }
        for {
          isOrWhere3 <- Seq("IS", "WHERE")
        } yield {
          test(s"($maybeVariable IS $isOrWhere WHERE $isOrWhere2 IS $isOrWhere3)") {
            parsesTo[NodePattern](
              nodePat(
                maybeVariableName,
                labelExpression = Some(labelLeaf(isOrWhere, containsIs = true)),
                predicates = Some(labelExpressionPredicate(
                  varFor(isOrWhere2),
                  labelOrRelTypeLeaf(isOrWhere3, containsIs = true)
                ))
              )
            )
          }
        }
      }
    }
  }
}
