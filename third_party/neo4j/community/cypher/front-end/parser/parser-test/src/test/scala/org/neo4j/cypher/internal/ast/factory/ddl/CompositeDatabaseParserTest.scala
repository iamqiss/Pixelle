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

import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class CompositeDatabaseParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // create

  test("CREATE COMPOSITE DATABASE name") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE $name") {
    parsesTo[Statements](
      CreateCompositeDatabase(stringParamName("name"), IfExistsThrowError, NoOptions, NoWait)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE `db.name`") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("db.name"), IfExistsThrowError, NoOptions, NoWait)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE db.name") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("db", "name"),
      IfExistsThrowError,
      NoOptions,
      NoWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE foo.bar") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("foo", "bar"),
      IfExistsThrowError,
      NoOptions,
      NoWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE `graph.db`.`db.db`") {
    // Fails in semantic checks instead
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("graph.db", "db.db"),
      IfExistsThrowError,
      NoOptions,
      NoWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name IF NOT EXISTS") {
    parsesTo[Statements](CreateCompositeDatabase(namespacedName("name"), IfExistsDoNothing, NoOptions, NoWait)(pos))
  }

  test("CREATE OR REPLACE COMPOSITE DATABASE name") {
    parsesTo[Statements](CreateCompositeDatabase(namespacedName("name"), IfExistsReplace, NoOptions, NoWait)(pos))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {}") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      OptionsMap(Map.empty),
      NoWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {someKey: 'someValue'} NOWAIT") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      OptionsMap(Map(
        "someKey" -> literalString("someValue")
      )),
      NoWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'TOPOLOGY': expected
            |  "."
            |  "IF"
            |  "NOWAIT"
            |  "OPTIONS"
            |  "WAIT"
            |  <EOF> (line 1, column 32 (offset: 31))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TOPOLOGY': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'WAIT' or <EOF> (line 1, column 32 (offset: 31))
            |"CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY"
            |                                ^""".stripMargin
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name WAIT") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name NOWAIT") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE name WAIT 10 SECONDS") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(10)
    )(pos))
  }

  // drop

  test("DROP COMPOSITE DATABASE name") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE `db.name`") {
    parsesTo[Statements](DropDatabase(
      namespacedName("db.name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE db.name") {
    parsesTo[Statements](DropDatabase(
      namespacedName("db", "name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE $name") {
    parsesTo[Statements](DropDatabase(
      stringParamName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = true,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name WAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name WAIT 10 SECONDS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      TimeoutAfter(10)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name NOWAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE foo DUMP DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = true, Restrict, DumpData, NoWait)(pos)
    )
  }

  test("DROP COMPOSITE DATABASE foo DESTROY DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = true, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP COMPOSITE DATABASE name RESTRICT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name CASCADE ALIASES") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      CascadeAliases,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS CASCADE ALIAS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = true,
      composite = true,
      CascadeAliases,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name RESTRICT DUMP DATA") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DumpData,
      NoWait
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name CASCADE ALIASES WAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      CascadeAliases,
      DestroyData,
      IndefiniteWait
    )(pos))
  }
}
