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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing transactions */
class ShowTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>
    test(s"SHOW $transactionKeyword") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = false, fromCypher5)(defaultPos)
        )
      )
    }

    test(s"SHOW $transactionKeyword 'db1-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false,
            fromCypher5
          )(defaultPos)
        )
      )
    }

    test(s"""SHOW $transactionKeyword "db1-transaction-123"""") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false,
            fromCypher5
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $transactionKeyword 'my.db-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(literalString("my.db-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword $$param") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(parameter("param", CTAny)),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword $$where") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(parameter("where", CTAny)),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"""SHOW $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Left(List("db1 - transaction - 123", "db2-transaction-45a6")),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword 'yield-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(literalString("yield-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword 'where-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(literalString("where-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"USE db SHOW $transactionKeyword") {
      assertAstVersionBased(
        fromCypher5 =>
          singleQuery(
            use(List("db")),
            ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = false, fromCypher5)(pos)
          ),
        comparePosition = false
      )
    }

  }

  test("SHOW TRANSACTION db-transaction-123") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(subtract(subtract(varFor("db"), varFor("transaction")), literalInt(123))),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION 'neo4j'+'-transaction-'+3") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION ('neo4j'+'-transaction-'+3)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(listOfString("db1-transaction-123", "db2-transaction-456")),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION foo") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("foo")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  test("SHOW TRANSACTION x+2") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(add(varFor("x"), literalInt(2))), None, List.empty, yieldAll = false, fromCypher5)(
          pos
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("YIELD")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(function("YIELD", add(literalInt(123), varFor("xyz")))),
          None,
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS ALL") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("ALL")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  // Filtering tests

  test("SHOW TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(defaultPos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD database") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Left(List.empty),
            None,
            List(commandResultItem("database")),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("database")))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List("db1-transaction-123", "db2-transaction-456")),
          None,
          List.empty,
          yieldAll = true,
          fromCypher5
        )(defaultPos),
        withFromYield(returnAllItems)
      )
    )
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Left(List("db1-transaction-123", "db2-transaction-456", "yield")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = true, fromCypher5)(pos),
          withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("transactionId")))), Some(skip(2)), Some(limit(5)))
        ),
      comparePosition = false
    )
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp WHERE pp < 50 RETURN transactionId") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db")),
          ShowTransactionsClause(
            Left(List.empty),
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
            where = Some(where(lessThan(varFor("pp"), literalInt(50L))))
          ),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db")),
          ShowTransactionsClause(
            Left(List.empty),
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
            Some(orderBy(sortItem(varFor("pp")))),
            Some(skip(2)),
            Some(limit(5)),
            Some(where(lessThan(varFor("pp"), literalInt(50L))))
          ),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp OFFSET 2 LIMIT 5 WHERE pp < 50 RETURN transactionId"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db")),
          ShowTransactionsClause(
            Left(List.empty),
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
            Some(orderBy(sortItem(varFor("pp")))),
            Some(skip(2)),
            Some(limit(5)),
            Some(where(lessThan(varFor("pp"), literalInt(50L))))
          ),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $param YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(parameter("param", CTAny)),
            None,
            List(
              commandResultItem("transactionId", Some("TRANSACTION")),
              commandResultItem("database", Some("OUTPUT"))
            ),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(literalString("where")),
            None,
            List(
              commandResultItem("transactionId", Some("TRANSACTION")),
              commandResultItem("database", Some("OUTPUT"))
            ),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'db1-transaction-123' WHERE transactionId = 'db1-transaction-124'") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(literalString("db1-transaction-123")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-124")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'yield' WHERE transactionId = 'where'") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(literalString("yield")),
          Some(where(equals(varFor("transactionId"), literalString("where")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTION 'db1-transaction-123', 'db1-transaction-124' WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Left(List("db1-transaction-123", "db1-transaction-124")),
          Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTION db1-transaction-123 WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          Right(subtract(subtract(varFor("db1"), varFor("transaction")), literalInt(123))),
          Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(listOfString("db1-transaction-123", "db2-transaction-456")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $x+'123' YIELD transactionId AS TRANSACTION, database AS SHOW") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(add(parameter("x", CTAny), literalString("123"))),
            None,
            List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("SHOW"))),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "SHOW")))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("where")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("yield")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("show")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("terminate")),
            None,
            List.empty,
            yieldAll = true,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD yield") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Left(List.empty),
            None,
            List(commandResultItem("yield")),
            yieldAll = false,
            fromCypher5
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("yield")))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("where")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("yield")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("show")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            Right(varFor("terminate")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS `yield` YIELD *") {
    def expected(yieldIsEscaped: Boolean, returnCypher5Types: Boolean) =
      singleQuery(
        ShowTransactionsClause(
          Right(varFor("yield", yieldIsEscaped)),
          None,
          List.empty,
          yieldAll = true,
          returnCypher5Types
        )(pos),
        withFromYield(returnAllItems)
      )
    parsesIn[Statement] {
      case Cypher5JavaCc => _.toAst(expected(yieldIsEscaped = false, returnCypher5Types = true))
      case Cypher5       => _.toAst(expected(yieldIsEscaped = true, returnCypher5Types = true))
      case _             => _.toAst(expected(yieldIsEscaped = false, returnCypher5Types = false))
    }
  }

  test("SHOW TRANSACTIONS `where` WHERE true") {
    def expected(whereIsEscaped: Boolean, returnCypher5Types: Boolean) =
      singleQuery(
        ShowTransactionsClause(
          Right(varFor("where", whereIsEscaped)),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false,
          returnCypher5Types
        )(pos)
      )
    parsesIn[Statement] {
      case Cypher5JavaCc => _.toAst(expected(whereIsEscaped = false, returnCypher5Types = true))
      case Cypher5       => _.toAst(expected(whereIsEscaped = true, returnCypher5Types = true))
      case _             => _.toAst(expected(whereIsEscaped = false, returnCypher5Types = false))
    }
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY a WHERE a = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(varFor("a")))),
          where = Some(where(equals(varFor("a"), literalInt(1))))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(notEquals(
            simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
            listOf()
          )))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
          where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(add(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))),
          where = Some(where(or(
            varFor("b"),
            AllIterablePredicate(
              varFor("x"),
              listOfInt(1, 2),
              Some(isTyped(varFor("x"), IntegerType(isNullable = true)(pos)))
            )(pos)
          )))
        )
      )
    )
  }

  test(
    "SHOW TRANSACTIONS 'id', 'id' YIELD username as transactionId, transactionId as username WHERE size(transactionId) > 0 RETURN transactionId as username"
  ) {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Left(List("id", "id")),
          None,
          List(
            commandResultItem("username", Some("transactionId")),
            commandResultItem("transactionId", Some("username"))
          ),
          yieldAll = false,
          fromCypher5
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("transactionId", "username")),
          where = Some(where(
            greaterThan(size(varFor("transactionId")), literalInt(0))
          ))
        ),
        return_(aliasedReturnItem("transactionId", "username"))
      )
    )
  }

  // Negative tests

  test("SHOW TRANSACTION db-transaction-123, abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', $param") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS $param, 'db-transaction-123'") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS $param, $param2") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'], abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION foo, 'abc'") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION x+2, abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW CURRENT USER TRANSACTION") {
    failsParsing[Statements]
  }

  test("SHOW USER user TRANSACTION") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'TRANSACTION': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 16 (offset: 15))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TRANSACTION': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 16 (offset: 15))
            |"SHOW USER user TRANSACTION"
            |                ^""".stripMargin
        )
    }
  }

  test("SHOW TRANSACTION EXECUTED BY USER user") {
    failsParsing[Statements]
  }

  test("SHOW ALL TRANSACTIONS") {
    failsParsing[Statements]
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        // Antlr parses YIELD * WITH * MATCH (n) as an expression
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW TRANSACTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>""".stripMargin
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      // parses varFor("WITH")
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'name': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'name': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix WITH 'n' as n SHOW TRANSACTIONS YIELD name RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN name as numIndexes") {
      // parses varFor("RETURN")
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'name': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'name': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      // parses varFor("WITH")
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input '1': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input '1': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c") {
      // parses varFor("WITH")
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input '1': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input '1': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD a WITH a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'UNWIND': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN id2 YIELD id2") {
      // parses varFor("RETURN")
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'id2': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'id2': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }
  }

  // Brief/verbose not allowed

  test("SHOW TRANSACTION BRIEF") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("BRIEF")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS BRIEF YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("BRIEF")), None, List.empty, yieldAll = true, fromCypher5)(pos),
        withFromYield(returnAllItems)
      )
    )
  }

  test("SHOW TRANSACTIONS BRIEF WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(varFor("BRIEF")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION VERBOSE") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("VERBOSE")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS VERBOSE YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("VERBOSE")), None, List.empty, yieldAll = true, fromCypher5)(pos),
        withFromYield(returnAllItems)
      )
    )
  }

  test("SHOW TRANSACTIONS VERBOSE WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          Right(varFor("VERBOSE")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION OUTPUT") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(Right(varFor("OUTPUT")), None, List.empty, yieldAll = false, fromCypher5)(pos)
      )
    )
  }

  test("SHOW TRANSACTION BRIEF OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS BRIEF RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION VERBOSE OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS VERBOSE RETURN *") {
    failsParsing[Statements]
  }

}
