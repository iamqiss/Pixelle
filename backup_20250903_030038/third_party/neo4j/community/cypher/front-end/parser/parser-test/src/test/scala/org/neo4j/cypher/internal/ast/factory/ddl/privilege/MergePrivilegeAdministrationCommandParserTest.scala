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
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class MergePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          test(s"$verb$immutableString MERGE { prop } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple properties should be allowed

          test(s"$verb$immutableString MERGE { * } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              AllPropertyResource()(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop1, prop2 } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(Seq("prop1", "prop2"))(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Home graph should be allowed

          test(s"$verb$immutableString MERGE { * } ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, HomeGraphScope()(_))(_),
              AllPropertyResource()(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop1, prop2 } ON HOME GRAPH RELATIONSHIP * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, HomeGraphScope()(_))(_),
              PropertiesResource(Seq("prop1", "prop2"))(_),
              List(RelationshipAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple graphs should be allowed

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, AllGraphsScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo,baz $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFooBaz)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Qualifiers

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(elemQualifierA, elemQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo ELEMENT A $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo NODES A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo NODES * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(LabelAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(relQualifierA, relQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo RELATIONSHIP * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(RelationshipAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString MERGE { prop } ON GRAPHS foo $preposition role1, role2") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          // Parameter values

          test(s"$verb$immutableString MERGE { prop } ON GRAPH $$foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeParamFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString MERGE { prop } ON GRAPH foo $preposition $$role") {
            parsesTo[Statements](func(
              GraphPrivilege(MergeAdminAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(paramRole),
              immutable
            )(pos))
          }

          // Default graph should not be allowed

          test(s"$verb$immutableString MERGE { * } ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString MERGE { prop1, prop2 } ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
            failsParsing[Statements].in {
              case Cypher5JavaCc | Cypher5 =>
                _.withMessageStart("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString MERGE { prop } ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 19
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

          test(s"$verb$immutableString MERGE { prop } ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 19
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString MERGE { prop } ON HOME DATABASE $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString MERGE { prop } ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements]
          }

          // Alias with too many components

          test(s"$verb$immutableString MERGE { prop } ON GRAPH `a`.`b`.`c` $preposition role") {
            // more than two components
            failsParsing[Statements]
              .withMessageContaining(
                "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
              )
          }
      }
  }
}
