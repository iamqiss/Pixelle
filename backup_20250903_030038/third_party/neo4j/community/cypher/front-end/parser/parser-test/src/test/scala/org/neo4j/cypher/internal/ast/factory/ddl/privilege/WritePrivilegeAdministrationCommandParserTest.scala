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

import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class WritePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFoo)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPHS foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFoo)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple graphs should be allowed (with and without plural GRAPHS)

          test(s"$verb$immutableString WRITE ON GRAPH * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, AllGraphsScope()(_))(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPHS * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, AllGraphsScope()(_))(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo, baz $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFooBaz)(pos),
              List(ElementsAllQualifier() _),
              List(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPHS foo, baz $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFooBaz)(pos),
              List(ElementsAllQualifier() _),
              List(literalRole),
              immutable
            )(pos))
          }

          // Home graph should parse

          test(s"$verb$immutableString WRITE ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, HomeGraphScope()(_))(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition role1, role2") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFoo)(_),
              List(ElementsAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          // Parameters and escaped strings should be allowed

          test(s"$verb$immutableString WRITE ON GRAPH $$foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeParamFoo)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPH `f:oo` $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, NamedGraphsScope(Seq(namespacedName("f:oo")))(_))(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition $$role") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFoo)(pos),
              List(ElementsAllQualifier() _),
              Seq(paramRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition `r:ole`") {
            parsesTo[Statements](func(
              GraphPrivilege(WriteAction, graphScopeFoo)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRColonOle),
              immutable
            )(pos))
          }

          // Resource or qualifier should not be supported

          test(s"$verb$immutableString WRITE {*} ON GRAPH foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE {prop} ON GRAPH foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo NODE A $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo NODES * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo RELATIONSHIP R $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo RELATIONSHIPS * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo ELEMENT A $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo ELEMENTS * $preposition role") {
            failsParsing[Statements]
          }

          // Invalid/missing part of the command

          test(s"$verb$immutableString WRITE ON GRAPH f:oo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition ro:le") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH foo $preposition") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE GRAPH foo $preposition role") {
            failsParsing[Statements]
          }

          // DEFAULT and HOME together with plural GRAPHS

          test(s"$verb$immutableString WRITE ON HOME GRAPHS $preposition role") {
            val offset = verb.length + immutableString.length + 15
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'GRAPHS': expected 'GRAPH' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString WRITE ON DEFAULT GRAPHS $preposition role") {
            val offset = verb.length + immutableString.length + 18
            val antlr6Offset = verb.length + immutableString.length + 10
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'GRAPHS': expected 'GRAPH' (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DEFAULT': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${antlr6Offset + 1} (offset: $antlr6Offset))"""
                )
            }
          }

          // Default and home graph with named graph

          test(s"$verb$immutableString WRITE ON HOME GRAPH baz $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON DEFAULT GRAPH baz $preposition role") {
            failsParsing[Statements]
          }

          // Mix of specific graph and *

          test(s"$verb$immutableString WRITE ON GRAPH foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON GRAPH *, foo $preposition role") {
            failsParsing[Statements]
          }

          // Default graph should not be allowed

          test(s"$verb$immutableString WRITE ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString WRITE ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 10
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString WRITE ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 10
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString WRITE ON HOME DATABASE $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString WRITE ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements]
          }

          // Alias with too many components

          test(s"$verb$immutableString WRITE ON GRAPH `a`.`b`.`c` $preposition role") {
            // more than two components
            failsParsing[Statements]
              .withMessageContaining(
                "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
              )
          }
      }
  }
}
