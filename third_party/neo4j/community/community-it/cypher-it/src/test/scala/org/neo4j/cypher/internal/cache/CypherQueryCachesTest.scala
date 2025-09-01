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
package org.neo4j.cypher.internal.cache

import org.neo4j.collection.ResourceRawIterator
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.ExecutionEngineTestSupport
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.util.CacheCountsTestSupport
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N02
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.notifications.StandardGqlStatusObject
import org.neo4j.values.AnyValue

class CypherQueryCachesTest extends CypherFunSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport
    with CacheCountsTestSupport {

  test("updateStrategy is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER updateStrategy=eager " + q)
    execute("CYPHER updateStrategy=default " + q)

    stats.preParserCacheEntries().shouldEqual(3)
    stats.astCacheEntries().shouldEqual(2)
    stats.logicalPlanCacheEntries().shouldEqual(2)
    stats.executionPlanCacheEntries().shouldEqual(2)
    stats.executableQueryCacheEntries().shouldEqual(2)
  }

  test("statefulShortestPlanningMode is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER statefulShortestPlanningMode=all_if_possible " + q)
    execute("CYPHER statefulShortestPlanningMode=into_only " + q)
    execute("CYPHER statefulShortestPlanningMode=cardinality_heuristic " + q)

    stats.preParserCacheEntries().shouldEqual(4)
    stats.astCacheEntries().shouldEqual(3)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(3)
    stats.executableQueryCacheEntries().shouldEqual(3)
  }

  test("planVarExpandInto is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER planVarExpandInto=minimum_cost " + q)
    execute("CYPHER planVarExpandInto=single_row " + q)

    stats.preParserCacheEntries().shouldEqual(3)
    stats.astCacheEntries().shouldEqual(2)
    stats.logicalPlanCacheEntries().shouldEqual(2)
    stats.executionPlanCacheEntries().shouldEqual(2)
    stats.executableQueryCacheEntries().shouldEqual(2)
  }

  test("eagerAnalyzer is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER eagerAnalyzer=lp " + q)
    execute("CYPHER eagerAnalyzer=ir " + q)

    stats.preParserCacheEntries().shouldEqual(3)
    stats.astCacheEntries().shouldEqual(2)
    stats.logicalPlanCacheEntries().shouldEqual(2)
    stats.executionPlanCacheEntries().shouldEqual(2)
    stats.executableQueryCacheEntries().shouldEqual(2)
  }

  test("connectComponentsPlanner is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER connectComponentsPlanner=greedy " + q)
    execute("CYPHER connectComponentsPlanner=idp " + q)
    execute("CYPHER connectComponentsPlanner=default " + q)

    stats.preParserCacheEntries().shouldEqual(4)
    stats.astCacheEntries().shouldEqual(3)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(3)
    stats.executableQueryCacheEntries().shouldEqual(3)
  }

  test("inferSchemaParts is honoured by all caches") {
    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (a) RETURN a"
    execute(q)
    execute("CYPHER inferSchemaParts=most_selective_label " + q)
    execute("CYPHER inferSchemaParts=off " + q)

    stats.preParserCacheEntries().shouldEqual(3)
    stats.astCacheEntries().shouldEqual(2)
    stats.logicalPlanCacheEntries().shouldEqual(2)
    stats.executionPlanCacheEntries().shouldEqual(2)
    stats.executableQueryCacheEntries().shouldEqual(2)
  }

  test("can get up to date statistics on cache entries") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER expressionEngine=interpreted RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(6)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(4)
    stats.executableQueryCacheEntries().shouldEqual(6)

    execute("RETURN 1 AS i")
    execute("RETURN 4 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER expressionEngine=interpreted RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(7)
    stats.astCacheEntries().shouldEqual(7)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(4)
    stats.executableQueryCacheEntries().shouldEqual(7)
  }

  test("clearing caches is reflected in statistics") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(5)
    stats.astCacheEntries().shouldEqual(5)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(3)
    stats.executableQueryCacheEntries().shouldEqual(5)

    eengine.queryCaches.preParserCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(5)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(3)
    stats.executableQueryCacheEntries().shouldEqual(5)

    eengine.queryCaches.executionPlanCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(5)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(5)

    eengine.queryCaches.executableQueryCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(5)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)

    eengine.masterCompiler.clearCaches()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(0)
    stats.logicalPlanCacheEntries().shouldEqual(0)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)
  }

  test("clearAll clears all caches") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(5)
    stats.astCacheEntries().shouldEqual(5)
    stats.logicalPlanCacheEntries().shouldEqual(3)
    stats.executionPlanCacheEntries().shouldEqual(3)
    stats.executableQueryCacheEntries().shouldEqual(5)

    eengine.queryCaches.clearAll()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(0)
    stats.logicalPlanCacheEntries().shouldEqual(0)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)
  }

  test("Should only create one executionPlanCacheEntry for plan with parameter") {
    restartWithConfig(databaseConfig() ++ Map(
      GraphDatabaseInternalSettings.cypher_size_hint_parameters -> Boolean.box(true)
    ))

    val stats = eengine.queryCaches.statistics()

    val q = "match (n {prop: $param}) return n"
    execute(q, Map("param" -> 123))
    execute(q, Map("param" -> "a"))
    execute(q, Map("param" -> ""))

    stats.executionPlanCacheEntries().shouldEqual(1)

  }

  test(
    "Should create two executionPlanCacheEntries for plan with parameter giving different plans based on cardinality"
  ) {
    restartWithConfig(databaseConfig() ++ Map(
      GraphDatabaseInternalSettings.cypher_size_hint_parameters -> Boolean.box(true)
    ))

    val stats = eengine.queryCaches.statistics()

    val q = "MATCH (n) WHERE n.prop starts with $param RETURN n"
    execute(q, Map("param" -> ""))
    execute(
      q,
      Map(
        "param" -> "This is a very long string test that needs to be checked if we still hit the executionPlanCacheEntries"
      )
    )

    stats.executionPlanCacheEntries().shouldEqual(2)

  }

  test("should evict executable query after procedures signature change") {
    val originalProcedure = registerProcedure("my.proc") { builder =>
      builder
        .in("a", Neo4jTypes.NTAny)
        .out("b", Neo4jTypes.NTAny)

      new BasicProcedure(builder.build()) {
        override def apply(
          ctx: Context,
          input: Array[AnyValue],
          resourceMonitor: ResourceMonitor
        ): ResourceRawIterator[Array[AnyValue], ProcedureException] = {
          ResourceRawIterator.empty()
        }
      }
    }

    val originalQuery = "CALL my.proc(123)"
    execute(originalQuery)
    cacheCountsFor(CypherQueryCaches.ExecutableQueryCache).evicted shouldBe 0

    globalProcedures.unregister(originalProcedure.signature().name())
    an[Exception] should be thrownBy execute(originalQuery)
    cacheCountsFor(CypherQueryCaches.ExecutableQueryCache).evicted shouldBe 1
  }

  test("Preparser notifications should be cached") {
    val ccpDescription =
      "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement." +
        " The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept." +
        " For more information, see Cypher Manual -> Cypher planner."

    execute("CYPHER connectComponentsPlanner=greedy RETURN  1")

    eengine.queryCaches.executableQueryCache.clear()
    eengine.queryCaches.executionPlanCache.clear()
    eengine.masterCompiler.clearCaches()

    val result = execute("CYPHER connectComponentsPlanner=greedy RETURN  1")
    result.notifications.map(_.getDescription) should contain(ccpDescription)
    result.gqlStatusObjects.map(_.gqlStatus) should contain(STATUS_01N02.getStatusString)
  }

  test("Logical plan cache does only cache its own notifications") {
    val result1 = execute("CREATE (a {f\\u0085oo:1})")
    result1.notifications.size shouldBe 1
    result1.gqlStatusObjects.size shouldBe 2
    // If we wrongly cache the notification from the above query in the logical plan cache, then
    // the 2nd query will also get it. It will miss the String cache but hit the AST cache and
    // thus get the cached logical plan
    val result2 = execute("CREATE (a {`f\\u0085oo`:1})")
    result2.notifications should be(empty)
    result2.gqlStatusObjects should be(List(StandardGqlStatusObject.OMITTED_RESULT))
  }

}
