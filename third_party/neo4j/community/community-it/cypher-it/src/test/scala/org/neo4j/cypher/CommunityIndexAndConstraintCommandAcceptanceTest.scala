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
package org.neo4j.cypher

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.kernel.impl.api.index.IndexingService

import scala.jdk.CollectionConverters.IterableHasAsScala

class CommunityIndexAndConstraintCommandAcceptanceTest extends ExecutionEngineFunSuite with GraphDatabaseTestSupport {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val indexName = "myIndex"
  private val constraintName = "myConstraint"
  private val label = "myLabel"
  private val relType = "myRelType"
  private val prop = "myProp"

  private val cypherVersions =
    (CypherVersion.values().map(cv => (s"CYPHER ${cv.versionName} ", cv.equals(CypherVersion.Cypher5)))
      :+ ("", CypherVersion.Default.equals(CypherVersion.Cypher5)))

  private def withoutIdColumn(result: List[Map[String, AnyRef]]) =
    result.map(m => m.filterNot { case (key, _) => key.equals("id") })

  private def removeExistingLookupIndexes(): Unit = graph.withTx(tx => {
    tx.schema().getIndexes().asScala
      .filter(id => id.getIndexType.equals(IndexType.LOOKUP))
      .foreach(i => i.drop())
  })

  private def anyMap(elems: (String, Any)*): Map[String, Any] = Map[String, Any](elems: _*)

  // Index commands

  test("Create node range index") {
    // WHEN
    val statistics = execute(s"CREATE INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.RANGE)
  }

  test("Create relationship range index") {
    // WHEN
    val statistics =
      execute(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.RANGE)
  }

  test("Create node text index") {
    // WHEN
    val statistics = execute(s"CREATE TEXT INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.TEXT)
  }

  test("Create relationship text index") {
    // WHEN
    val statistics =
      execute(s"CREATE TEXT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.TEXT)
  }

  test("Create node point index") {
    // WHEN
    val statistics = execute(s"CREATE POINT INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.POINT)
  }

  test("Create relationship point index") {
    // WHEN
    val statistics =
      execute(s"CREATE POINT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.POINT)
  }

  test("Create node vector index") {
    // WHEN
    val statistics = execute(
      s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_DIMENSIONS.getSettingName -> 50,
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    ).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
  }

  test("Create relationship vector index") {
    // WHEN
    val statistics = execute(
      s"CREATE VECTOR INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_DIMENSIONS.getSettingName -> 50,
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    ).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
  }

  test("Create node fulltext index") {
    // WHEN
    val statistics = execute(s"CREATE FULLTEXT INDEX $indexName FOR (n:$label) ON EACH [n.$prop]").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.FULLTEXT)
  }

  test("Create relationship fulltext index") {
    // WHEN
    val statistics =
      execute(s"CREATE FULLTEXT INDEX $indexName FOR ()-[r:$relType]-() ON EACH [r.$prop]").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.FULLTEXT)
  }

  test("Create node lookup index") {
    // GIVEN
    removeExistingLookupIndexes()

    // WHEN
    val statistics = execute(s"CREATE LOOKUP INDEX $indexName FOR (n) ON EACH labels(n)").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.LOOKUP)
  }

  test("Create relationship lookup index") {
    // GIVEN
    removeExistingLookupIndexes()

    // WHEN
    val statistics =
      execute(s"CREATE LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.LOOKUP)
  }

  test("Drop index") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)

    // WHEN
    val statistics = execute(s"DROP INDEX $indexName").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesRemoved = 1))
    graph.indexExists(constraintName) should be(false)
  }

  test("Show index") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, relType, prop)
    graph.getDependencyResolver.resolveDependency(classOf[IndexingService]).reportUsageStatistics()

    // WHEN
    val result = execute("SHOW RANGE INDEXES")

    // THEN
    withoutIdColumn(result.toList) should be(List(Map[String, AnyRef](
      "name" -> indexName,
      "state" -> "ONLINE",
      "populationPercent" -> 100.0.asInstanceOf[AnyRef],
      "type" -> IndexType.RANGE.name(),
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> List(relType),
      "properties" -> List(prop),
      "indexProvider" -> AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name(),
      "owningConstraint" -> null,
      "lastRead" -> null,
      "readCount" -> 0L.asInstanceOf[AnyRef]
    )))
  }

  test("Index commands with Cypher versions") {
    // GIVEN
    removeExistingLookupIndexes()
    val createCommands = Seq(
      (s"CREATE INDEX $indexName FOR (n:$label) ON n.$prop", "RANGE"),
      (s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "RANGE"),
      (s"CREATE TEXT INDEX $indexName FOR (n:$label) ON n.$prop", "TEXT"),
      (s"CREATE TEXT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "TEXT"),
      (s"CREATE POINT INDEX $indexName FOR (n:$label) ON n.$prop", "POINT"),
      (s"CREATE POINT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "POINT"),
      (s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop", "VECTOR"),
      (s"CREATE VECTOR INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "VECTOR"),
      (s"CREATE FULLTEXT INDEX $indexName FOR (n:$label) ON EACH [n.$prop]", "FULLTEXT"),
      (s"CREATE FULLTEXT INDEX $indexName FOR ()-[r:$relType]-() ON EACH [r.$prop]", "FULLTEXT"),
      (s"CREATE LOOKUP INDEX $indexName FOR (n) ON EACH labels(n)", "LOOKUP"),
      (s"CREATE LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)", "LOOKUP")
    )
    val dropCommand = s"DROP INDEX $indexName"
    val showCommand = "SHOW INDEXES YIELD name, type"

    // WHEN .. THEN
    cypherVersions.foreach { case (cypherVersionString, _) =>
      createCommands.foreach { case (createCommand, indexType) =>
        withClue(cypherVersionString + createCommand) {
          // Create
          val resCreate = execute(cypherVersionString + createCommand)
          resCreate.queryStatistics().indexesAdded should be(1)

          // Show
          val resShow = execute(cypherVersionString + showCommand)
          resShow.toList should be(List(Map("name" -> indexName, "type" -> indexType)))

          // Drop
          val resDrop = execute(cypherVersionString + dropCommand)
          resDrop.queryStatistics().indexesRemoved should be(1)
        }
      }
    }
  }

  // Constraint commands

  test("Create node uniqueness constraint") {
    // WHEN
    val statistics =
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS UNIQUE").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(uniqueConstraintsAdded = 1))
    statistics.constraintsAdded should be(1)

    graph.constraintExists(constraintName) should be(true)
    graph.getConstraintTypeByName(constraintName) should be(ConstraintType.UNIQUENESS)
  }

  test("Create relationship uniqueness constraint") {
    // WHEN
    val statistics =
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS UNIQUE").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(relUniqueConstraintsAdded = 1))
    statistics.constraintsAdded should be(1)

    graph.constraintExists(constraintName) should be(true)
    graph.getConstraintTypeByName(constraintName) should be(ConstraintType.RELATIONSHIP_UNIQUENESS)
  }

  test("Create node key constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NODE KEY")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( type='NODE KEY', schema=(:$label {$prop}) ):
         |Node Key constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship key constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS REL KEY")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( type='RELATIONSHIP KEY', schema=()-[:$relType {$prop}]-() ):
         |Relationship Key constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create node property existence constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NOT NULL")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) ):
         |Property existence constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship property existence constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS NOT NULL")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:$relType {$prop}]-() ):
         |Property existence constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create node property type constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS :: INT")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( name='$constraintName', type='NODE PROPERTY TYPE', schema=(:$label {$prop}), propertyType=INTEGER ):
         |Property type constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship property type constraint") {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS :: INT")
    }

    // THEN
    exception.getMessage should equal(
      s"""Unable to create Constraint( name='$constraintName', type='RELATIONSHIP PROPERTY TYPE', schema=()-[:$relType {$prop}]-(), propertyType=INTEGER ):
         |Property type constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin
    )
    graph.constraintExists(constraintName) should be(false)
  }

  test("Drop constraint") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName(constraintName, label, prop)

    // WHEN
    val statistics = execute(s"DROP CONSTRAINT $constraintName").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(constraintsRemoved = 1))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Show constraints") {
    // GIVEN
    graph.createRelationshipUniquenessConstraintWithName(constraintName, relType, prop)

    // WHEN
    val result = execute("SHOW CONSTRAINTS")

    // THEN
    withoutIdColumn(result.toList) should be(List(Map[String, AnyRef](
      "name" -> constraintName,
      "type" -> ConstraintType.RELATIONSHIP_UNIQUENESS.name(),
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> List(relType),
      "properties" -> List(prop),
      "ownedIndex" -> constraintName,
      "propertyType" -> null
    )))
  }

  test("Constraint commands with Cypher versions") {
    // GIVEN
    val allowedCreateCommands = Seq(
      (
        s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS UNIQUE",
        "NODE_PROPERTY_UNIQUENESS",
        "UNIQUENESS"
      ),
      (
        s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS UNIQUE",
        "RELATIONSHIP_PROPERTY_UNIQUENESS",
        "RELATIONSHIP_UNIQUENESS"
      )
    )
    val failingCreateCommands = Seq(
      s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NODE KEY",
      s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS REL KEY",
      s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NOT NULL",
      s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS NOT NULL",
      s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS :: INT",
      s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS :: INT"
    )
    val dropCommand = s"DROP CONSTRAINT $constraintName"
    val showCommand = "SHOW CONSTRAINTS YIELD name, type"

    // WHEN .. THEN
    cypherVersions.foreach { case (cypherVersionString, usesCypher5) =>
      allowedCreateCommands.foreach { case (createCommand, constraintType, constraintTypeCypher5) =>
        withClue(cypherVersionString + createCommand) {
          // Create
          val resCreate = execute(cypherVersionString + createCommand)
          resCreate.queryStatistics().constraintsAdded should be(1)

          // Show
          val resShow = execute(cypherVersionString + showCommand)
          resShow.toList should be(List(Map(
            "name" -> constraintName,
            "type" -> (if (usesCypher5) constraintTypeCypher5 else constraintType)
          )))

          // Drop
          val resDrop = execute(cypherVersionString + dropCommand)
          resDrop.queryStatistics().constraintsRemoved should be(1)
        }
      }

      failingCreateCommands.foreach(createCommand =>
        withClue(cypherVersionString + createCommand) {
          val exceptionMessage = (the[CypherExecutionException] thrownBy {
            execute(cypherVersionString + createCommand)
          }).getMessage
          exceptionMessage should startWith("Unable to create Constraint(")
          exceptionMessage should endWith(
            "constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown."
          )
          graph.constraintExists(constraintName) should be(false)
        }
      )
    }
  }
}
