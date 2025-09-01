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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class AllGraphPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          // All versions of ALL [[GRAPH] PRIVILEGES] should be allowed

          test(s"$verb$immutableString ALL ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFoo)(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFoo)(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFoo)(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Home graph should be allowed

          test(s"$verb$immutableString ALL ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, HomeGraphScope()(_))(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, HomeGraphScope()(_))(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, HomeGraphScope()(_))(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple graphs should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, AllGraphsScope()(_))(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS foo,baz $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFooBaz)(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS foo $preposition role1, role2") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFoo)(_),
              List(AllQualifier()(_)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          // Parameter values should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH $$foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeParamFoo)(_),
              List(AllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo $preposition $$role") {
            parsesTo[Statements](func(
              GraphPrivilege(AllGraphAction, graphScopeFoo)(_),
              List(AllQualifier()(_)),
              Seq(paramRole),
              immutable
            )(pos))
          }

          // Qualifier or resource should not be supported

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo NODE A $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo ELEMENTS * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES {prop} ON GRAPH foo $preposition role") {
            failsParsing[Statements]
          }

          // Invalid syntax

          test(s"$verb$immutableString ALL GRAPH ON GRAPH foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString GRAPH ON GRAPH foo $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input 'GRAPH': expected\n  \"ACCESS\"")
              case _             => _.withSyntaxErrorContaining("Invalid input 'GRAPH': expected")
            }
          }

          test(s"$verb$immutableString GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input 'GRAPH': expected\n  \"ACCESS\"")
              case _             => _.withSyntaxErrorContaining("Invalid input 'GRAPH': expected")
            }
          }

          test(s"$verb$immutableString PRIVILEGES ON GRAPH foo $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input 'PRIVILEGES': expected\n  \"ACCESS\"")
              case _             => _.withSyntaxErrorContaining("Invalid input 'PRIVILEGES': expected")
            }
          }

          // Default graph should not be allowed

          test(s"$verb$immutableString ALL ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          // Database/dbms instead of graph keyword

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 25
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(
                  s"""Invalid input 'DATABASES': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 25
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(
                  s"""Invalid input 'DATABASE': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON HOME DATABASE $preposition role") {
            val offset = verb.length + immutableString.length + 25
            val antlrOffset = verb.length + immutableString.length + 30
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(
                  s"""Invalid input 'HOME': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected "GRAPH" (line 1, column ${antlrOffset + 1} (offset: $antlrOffset))"""
                )
            }
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DEFAULT DATABASE $preposition role") {
            val offset = verb.length + immutableString.length + 25
            val antlrOffset = verb.length + immutableString.length + 33
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(
                  s"""Invalid input 'DEFAULT': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected "GRAPH" (line 1, column ${antlrOffset + 1} (offset: $antlrOffset))"""
                )
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DBMS $preposition role") {
            val offset = verb.length + immutableString.length + 25
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(
                  s"""Invalid input 'DBMS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DBMS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          // Alias with too many components
          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH `a`.`b`.`c` $preposition role") {
            // more than two components
            failsParsing[Statements]
              .withMessageContaining(
                "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
              )
          }
      }
  }
}
