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

import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.util.InputPosition

class RoleAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val roleString = "role"

  //  Showing roles

  Seq("ROLES", "ROLE").foreach(roleKeyword => {
    test(s"SHOW $roleKeyword") {
      parsesTo[Statements](ShowRoles(withUsers = false, showAll = true, None)(pos))
    }

    test(s"SHOW ALL $roleKeyword") {
      parsesTo[Statements](ShowRoles(withUsers = false, showAll = true, None)(pos))
    }

    test(s"SHOW POPULATED $roleKeyword") {
      parsesTo[Statements](ShowRoles(withUsers = false, showAll = false, None)(pos))
    }

    Seq("USERS", "USER").foreach(userKeyword => {
      test(s"SHOW $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ShowRoles(withUsers = true, showAll = true, None)(pos))
      }

      test(s"SHOW ALL $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ShowRoles(withUsers = true, showAll = true, None)(pos))
      }

      test(s"SHOW POPULATED $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ShowRoles(withUsers = true, showAll = false, None)(pos))
      }

    })

  })

  test("USE neo4j SHOW ROLES") {
    parsesTo[Statements] {
      ShowRoles(withUsers = false, showAll = true, None)(pos).withGraph(Some(use(List("neo4j"))))
    }
  }

  test("USE GRAPH SYSTEM SHOW ROLES") {
    parsesTo[Statements] {
      ShowRoles(withUsers = false, showAll = true, None)(pos).withGraph(Some(use(List("SYSTEM"))))
    }
  }

  test("SHOW ALL ROLES YIELD role") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLE YIELD role") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLES WHERE role='PUBLIC'") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLE WHERE role='PUBLIC'") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    parsesTo[Statements](ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem(roleString))),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    )(pos))
  }

  test("SHOW ALL ROLES YIELD return, return RETURN return") {
    parsesTo[Statements](ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem("return"), variableReturnItem("return"))),
        Some(returnClause(returnItems(variableReturnItem("return"))))
      )))
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    parsesTo[Statements](ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(roleString)),
          where = Some(where(equals(varFor(roleString), literalString("PUBLIC"))))
        ),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    parsesTo[Statements](ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    )(pos))
  }

  test("SHOW POPULATED ROLE WITH USER YIELD * RETURN *") {
    parsesTo[Statements](ShowRoles(
      withUsers = true,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    )(pos))
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    parsesTo[Statements](ShowRoles(
      withUsers = true,
      showAll = true,
      Some(Left((
        yieldClause(
          returnAllItems,
          limit = Some(limit(10)),
          where = Some(where(equals(varFor("foo"), literalString("bar"))))
        ),
        Some(returnClause(
          returnItems(variableReturnItem("some"), variableReturnItem("columns")),
          limit = Some(limit(10))
        ))
      )))
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = false,
        Some(Left((
          yieldClause(
            returnItems(variableReturnItem(roleString)),
            Some(orderBy(sortItem(varFor(roleString)))),
            Some(skip(-1))
          ),
          None
        )))
      )(pos)
    )
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role OFFSET -1") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = false,
        Some(Left((
          yieldClause(
            returnItems(variableReturnItem(roleString)),
            Some(orderBy(sortItem(varFor(roleString)))),
            Some(skip(-1))
          ),
          None
        )))
      )(pos)
    )
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role LIMIT -1") {
    parsesTo[Statements](
      ShowRoles(
        withUsers = false,
        showAll = false,
        Some(Left((
          yieldClause(
            returnItems(variableReturnItem(roleString)),
            Some(orderBy(sortItem(varFor(roleString)))),
            limit = Some(limit(-1))
          ),
          None
        )))
      )(pos)
    )
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1*4 + 2") {
    failsParsing[Statements]
  }

  test("SHOW ROLE role") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \",\", \"PRIVILEGE\" or \"PRIVILEGES\" (line 1, column 15 (offset: 14))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 15 (offset: 14))
            |"SHOW ROLE role"
            |               ^""".stripMargin
        )
    }
  }

  test("SHOW ROLES YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW ROLES YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW ALL ROLES YIELD role RETURN") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"*\", \"DISTINCT\" or an expression (line 1, column 33 (offset: 32))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected an expression, '*' or 'DISTINCT' (line 1, column 33 (offset: 32))
            |"SHOW ALL ROLES YIELD role RETURN"
            |                                 ^""".stripMargin
        )
    }
  }

  test("SHOW ROLES WITH USER user") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'user': expected "WHERE", "YIELD" or <EOF> (line 1, column 22 (offset: 21))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'user': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 22 (offset: 21))
            |"SHOW ROLES WITH USER user"
            |                      ^""".stripMargin
        )
    }
  }

  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "OFFSET"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 29 (offset: 28))""".stripMargin
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input ',': expected 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF> (line 1, column 29 (offset: 28))
            |"SHOW POPULATED ROLES YIELD *,blah RETURN role"
            |                             ^""".stripMargin
        )
    }
  }

  //  Creating role

  Seq(true, false).foreach { immutable =>
    val immutableString = maybeImmutable(immutable)
    val immutablePad = maybeImmutablePad(immutable)

    test(s"CREATE$immutableString ROLE foo") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, None, IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE $$foo") {
      parsesTo[Statements](CreateRole(paramFoo, immutable, None, IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE `fo!$$o`") {
      parsesTo[Statements](CreateRole(literal("fo!$o"), immutable, None, IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE ``") {
      parsesTo[Statements](CreateRole(literalEmpty, immutable, None, IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE foo AS COPY OF bar") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(literalBar), IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE foo AS COPY OF $$bar") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(stringParam("bar")), IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE foo AS COPY OF ``") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(literalEmpty), IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE `` AS COPY OF bar") {
      parsesTo[Statements](CreateRole(literalEmpty, immutable, Some(literalBar), IfExistsThrowError)(pos))
    }

    test(s"CREATE$immutableString ROLE foo IF NOT EXISTS") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, None, IfExistsDoNothing)(pos))
    }

    test(s"CREATE$immutableString ROLE foo IF NOT EXISTS AS COPY OF bar") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(literalBar), IfExistsDoNothing)(pos))
    }

    test(s"CREATE OR REPLACE$immutableString ROLE foo") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, None, IfExistsReplace)(pos))
    }

    test(s"CREATE OR REPLACE$immutableString ROLE foo AS COPY OF bar") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(literalBar), IfExistsReplace)(pos))
    }

    test(s"CREATE OR REPLACE$immutableString ROLE foo IF NOT EXISTS") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, None, IfExistsInvalidSyntax)(pos))
    }

    test(s"CREATE OR REPLACE$immutableString ROLE foo IF NOT EXISTS AS COPY OF bar") {
      parsesTo[Statements](CreateRole(literalFoo, immutable, Some(literalBar), IfExistsInvalidSyntax)(pos))
    }

    test(s"CREATE$immutableString ROLE \"foo\"") {
      failsParsing[Statements]
    }

    test(s"CREATE$immutableString ROLE f%o") {
      failsParsing[Statements]
    }

    test(s"CREATE$immutableString ROLE  IF NOT EXISTS") {
      failsParsing[Statements]
    }

    test(s"CREATE$immutableString ROLE foo IF EXISTS") {
      failsParsing[Statements]
    }

    test(s"CREATE OR REPLACE$immutableString ROLE ") {
      val offset = immutableString.length + 22
      failsParsing[Statements].in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))"
          )
        case _ => _.withSyntaxError(
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))
               |"CREATE OR REPLACE$immutableString ROLE"
               |                       $immutablePad^""".stripMargin
          )
      }
    }

    test(s"CREATE$immutableString ROLE foo AS COPY OF") {
      val offset = immutableString.length + 26
      failsParsing[Statements].in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset)"
          )
        case _ => _.withSyntaxError(
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))
               |"CREATE$immutableString ROLE foo AS COPY OF"
               |                           $immutablePad^""".stripMargin
          )
      }
    }

    test(s"CREATE$immutableString ROLE foo IF NOT EXISTS AS COPY OF") {
      val offset = immutableString.length + 40
      failsParsing[Statements].in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))"
          )
        case _ => _.withSyntaxError(
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))
               |"CREATE$immutableString ROLE foo IF NOT EXISTS AS COPY OF"
               |                                         $immutablePad^""".stripMargin
          )
      }
    }

    test(s"CREATE OR REPLACE$immutableString ROLE foo AS COPY OF") {
      val offset = immutableString.length + 37
      failsParsing[Statements].in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))"
          )
        case _ => _.withSyntaxError(
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))
               |"CREATE OR REPLACE$immutableString ROLE foo AS COPY OF"
               |                                      $immutablePad^""".stripMargin
          )
      }
    }

    test(s"CREATE$immutableString ROLE foo UNION CREATE$immutableString ROLE foo2") {
      val offset = immutableString.length + 16
      failsParsing[Statements].in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"Invalid input 'UNION': expected \"AS\", \"IF\" or <EOF> (line 1, column ${offset + 1} (offset: $offset))"
          )
        case _ => _.withSyntaxError(
            s"""Invalid input 'UNION': expected 'IF NOT EXISTS', 'AS COPY OF' or <EOF> (line 1, column ${offset + 1} (offset: $offset))
               |"CREATE$immutableString ROLE foo UNION CREATE$immutableString ROLE foo2"
               |                 $immutablePad^""".stripMargin
          )
      }
    }
  }

  // Renaming role

  test("RENAME ROLE foo TO bar") {
    parsesTo[Statements](RenameRole(literalFoo, literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE foo TO $bar") {
    parsesTo[Statements](RenameRole(literalFoo, stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME ROLE $foo TO bar") {
    parsesTo[Statements](RenameRole(stringParam("foo"), literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE $foo TO $bar") {
    parsesTo[Statements](RenameRole(stringParam("foo"), stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME ROLE foo IF EXISTS TO bar") {
    parsesTo[Statements](RenameRole(literalFoo, literalBar, ifExists = true)(pos))
  }

  test("RENAME ROLE foo IF EXISTS TO $bar") {
    parsesTo[Statements](RenameRole(literalFoo, stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME ROLE $foo IF EXISTS TO bar") {
    parsesTo[Statements](RenameRole(stringParam("foo"), literalBar, ifExists = true)(pos))
  }

  test("RENAME ROLE $foo IF EXISTS TO $bar") {
    parsesTo[Statements](RenameRole(stringParam("foo"), stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME ROLE foo TO ``") {
    parsesTo[Statements](RenameRole(literalFoo, literalEmpty, ifExists = false)(pos))
  }

  test("RENAME ROLE `` TO bar") {
    parsesTo[Statements](RenameRole(literalEmpty, literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE foo TO") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))
            |"RENAME ROLE foo TO"
            |                   ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))")
      case _ => _.withSyntaxError(
          """Invalid input 'bar': expected 'IF EXISTS' or 'TO' (line 1, column 16 (offset: 15))
            |"RENAME ROLE TO bar"
            |                ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE TO") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'IF EXISTS' or 'TO' (line 1, column 15 (offset: 14))
            |"RENAME ROLE TO"
            |               ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE foo SET NAME TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
      case _ => _.withSyntaxError(
          """Invalid input 'SET': expected 'IF EXISTS' or 'TO' (line 1, column 17 (offset: 16))
            |"RENAME ROLE foo SET NAME TO bar"
            |                 ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE foo SET NAME bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
      case _ => _.withSyntaxError(
          """Invalid input 'SET': expected 'IF EXISTS' or 'TO' (line 1, column 17 (offset: 16))
            |"RENAME ROLE foo SET NAME bar"
            |                 ^""".stripMargin
        )
    }
  }

  test("ALTER ROLE foo SET NAME bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'ROLE': expected
            |  "ALIAS"
            |  "CURRENT"
            |  "DATABASE"
            |  "SERVER"
            |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ROLE': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
            |"ALTER ROLE foo SET NAME bar"
            |       ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE foo IF EXIST TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
      case _ => _.withSyntaxError(
          """Invalid input 'EXIST': expected 'EXISTS' (line 1, column 20 (offset: 19))
            |"RENAME ROLE foo IF EXIST TO bar"
            |                    ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE foo IF NOT EXISTS TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected 'EXISTS' (line 1, column 20 (offset: 19))
            |"RENAME ROLE foo IF NOT EXISTS TO bar"
            |                    ^""".stripMargin
        )
    }
  }

  test("RENAME ROLE foo TO bar IF EXISTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
      case _ => _.withSyntaxError(
          """Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))
            |"RENAME ROLE foo TO bar IF EXISTS"
            |                        ^""".stripMargin
        )
    }
  }

  test("RENAME IF EXISTS ROLE foo TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'IF': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'IF': expected 'ROLE', 'SERVER' or 'USER' (line 1, column 8 (offset: 7))
            |"RENAME IF EXISTS ROLE foo TO bar"
            |        ^""".stripMargin
        )
    }
  }

  test("RENAME OR REPLACE ROLE foo TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'OR': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OR': expected 'ROLE', 'SERVER' or 'USER' (line 1, column 8 (offset: 7))
            |"RENAME OR REPLACE ROLE foo TO bar"
            |        ^""".stripMargin
        )
    }
  }

  //  Dropping role

  test("DROP ROLE foo") {
    parsesTo[Statements](DropRole(literalFoo, ifExists = false)(pos))
  }

  test("DROP ROLE $foo") {
    parsesTo[Statements](DropRole(paramFoo, ifExists = false)(pos))
  }

  test("DROP ROLE ``") {
    parsesTo[Statements](DropRole(literalEmpty, ifExists = false)(pos))
  }

  test("DROP ROLE foo IF EXISTS") {
    parsesTo[Statements](DropRole(literalFoo, ifExists = true)(pos))
  }

  test("DROP ROLE `` IF EXISTS") {
    parsesTo[Statements](DropRole(literalEmpty, ifExists = true)(pos))
  }

  test("DROP ROLE ") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))
            |"DROP ROLE"
            |          ^""".stripMargin
        )
    }
  }

  test("DROP ROLE  IF EXISTS") {
    failsParsing[Statements]
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  //  Granting/revoking roles to/from users

  private type grantOrRevokeRoleFunc = (Seq[String], Seq[String]) => InputPosition => AdministrationCommand

  private def grantRole(r: Seq[String], u: Seq[String]): InputPosition => AdministrationCommand =
    GrantRolesToUsers(
      r.map(roleName => literalString(roleName)),
      u.map(userName => literalString(userName))
    )

  private def revokeRole(r: Seq[String], u: Seq[String]): InputPosition => AdministrationCommand =
    RevokeRolesFromUsers(
      r.map(roleName => literalString(roleName)),
      u.map(userName => literalString(userName))
    )

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>
      Seq(
        ("GRANT", "TO", grantRole: grantOrRevokeRoleFunc),
        ("REVOKE", "FROM", revokeRole: grantOrRevokeRoleFunc)
      ).foreach {
        case (verb: String, preposition: String, func: grantOrRevokeRoleFunc) =>
          test(s"$verb $roleKeyword foo $preposition abc") {
            parsesTo[Statements](func(Seq("foo"), Seq("abc"))(defaultPos))
          }

          test(s"$verb $roleKeyword foo, bar $preposition abc") {
            parsesTo[Statements](func(Seq("foo", "bar"), Seq("abc"))(defaultPos))
          }

          test(s"$verb $roleKeyword foo $preposition abc, def") {
            parsesTo[Statements](func(Seq("foo"), Seq("abc", "def"))(defaultPos))
          }

          test(s"$verb $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            parsesTo[Statements](func(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def"))(defaultPos))
          }

          test(s"$verb $roleKeyword `fo:o` $preposition bar") {
            parsesTo[Statements](func(Seq("fo:o"), Seq("bar"))(defaultPos))
          }

          test(s"$verb $roleKeyword foo $preposition `b:ar`") {
            parsesTo[Statements](func(Seq("foo"), Seq("b:ar"))(defaultPos))
          }

          test(s"$verb $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            parsesTo[Statements](func(Seq("$f00", "bar"), Seq("abc", s"$$a&c"))(defaultPos))
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            val javaCcExpected = roleKeyword match {
              case "ROLE" => """Invalid input '': expected "MANAGEMENT", a parameter or an identifier"""
              case _      => """Invalid input '': expected a parameter or an identifier"""
            }

            val antlrExpected = roleKeyword match {
              case "ROLE" =>
                """Invalid input '': expected a parameter, an identifier or 'MANAGEMENT'"""
              case _ =>
                """Invalid input '': expected a parameter or an identifier""".stripMargin
            }

            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(javaCcExpected)
              case _             => _.withSyntaxErrorContaining(antlrExpected)
            }
          }

          test(s"$verb $roleKeyword foo") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(s"""Invalid input '': expected "," or "$preposition"""")
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input '': expected ',' or '$preposition'"""
                )
            }
          }

          test(s"$verb $roleKeyword foo $preposition") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected a parameter or an identifier")
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input '': expected a parameter or an identifier""".stripMargin
                )
            }
          }

          test(s"$verb $roleKeyword $preposition abc") {
            failsParsing[Statements]
          }

          // Should fail to parse when invalid user or role name

          test(s"$verb $roleKeyword fo:o $preposition bar") {
            failsParsing[Statements]
          }

          test(s"$verb $roleKeyword foo $preposition b:ar") {
            failsParsing[Statements]
          }
      }

      // Should fail to parse when mixing TO and FROM

      test(s"GRANT $roleKeyword foo FROM abc") {
        failsParsing[Statements]
      }

      test(s"REVOKE $roleKeyword foo TO abc") {
        failsParsing[Statements]
      }
  }

  test(s"GRANT ROLE $$a TO $$x") {
    parsesTo[Statements](GrantRolesToUsers(Seq(stringParam("a")), Seq(stringParam("x")))(defaultPos))
  }

  test(s"REVOKE ROLE $$a FROM $$x") {
    parsesTo[Statements](RevokeRolesFromUsers(Seq(stringParam("a")), Seq(stringParam("x")))(defaultPos))
  }

  test(s"GRANT ROLES a, $$b, $$c TO $$x, y, z") {
    parsesTo[Statements](GrantRolesToUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    )(defaultPos))
  }

  test(s"REVOKE ROLES a, $$b, $$c FROM $$x, y, z") {
    parsesTo[Statements](RevokeRolesFromUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    )(defaultPos))
  }

  // ROLE[S] TO USER only have GRANT and REVOKE and not DENY

  test(s"DENY ROLE foo TO abc") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'foo': expected "MANAGEMENT"""")
      case _ => _.withSyntaxError(
          """Invalid input 'foo': expected 'MANAGEMENT' (line 1, column 11 (offset: 10))
            |"DENY ROLE foo TO abc"
            |           ^""".stripMargin
        )
    }
  }

  test("DENY ROLES foo TO abc") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'ROLES': expected
            |  "ACCESS"
            |  "ALIAS"
            |  "ALL"
            |  "ALTER"
            |  "ASSIGN"
            |  "COMPOSITE"
            |  "CONSTRAINT"
            |  "CONSTRAINTS"
            |  "CREATE"""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ROLES': expected 'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER' (line 1, column 6 (offset: 5))
            |"DENY ROLES foo TO abc"
            |      ^""".stripMargin
        )
    }
  }
}
