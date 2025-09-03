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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.hasAnyDynamicLabel
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.hasDynamicLabels
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ForeachPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("should be able to unnest apply for MERGE inside FOREACH") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("PROFILES", 100)
      .setRelationshipCardinality("()-[:RELATION]->()", 100)
      .setRelationshipCardinality("(:PROFILES)-[:RELATION]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->(:PROFILES)", 100)
      .build()

    val query =
      """MATCH (a:PROFILES { _key: $_key })-[r:RELATION]->(b)
        |WITH a, COLLECT(b) AS others
        |SET a.knows = size(others)
        |FOREACH (o IN others | 
        |  MERGE (o)-[:KNOWS]->(a)
        |)      
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .foreachApply("o", "others")
      .|.merge(Seq(), Seq(createRelationship("anon_0", "o", "KNOWS", "a", OUTGOING)), lockNodes = Set("o", "a"))
      .|.expandInto("(o)-[anon_0:KNOWS]->(a)")
      .|.argument("a", "o")
      .setNodeProperty("a", "knows", "size(others)")
      .orderedAggregation(Seq("a AS a"), Seq("COLLECT(b) AS others"), Seq("a"))
      .expandAll("(a)-[r:RELATION]->(b)")
      .filter("a._key = $_key")
      .nodeByLabelScan("a", "PROFILES", IndexOrderAscending)
      .build()
  }

  test("should be able to unnest apply for MERGE and SET inside FOREACH") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("PROFILES", 100)
      .setRelationshipCardinality("()-[:RELATION]->()", 100)
      .setRelationshipCardinality("(:PROFILES)-[:RELATION]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->(:PROFILES)", 100)
      .build()

    val query =
      """MATCH (a:PROFILES { _key: $_key })-[r:RELATION]->(b)
        |WITH a, COLLECT(b) AS others
        |SET a.knows = size(others)
        |FOREACH (o IN others |
        |  MERGE (o)-[:KNOWS]->(a)
        |  SET a.prop = others
        |)
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .foreachApply("o", "others")
      .|.setNodeProperty("a", "prop", "others")
      .|.merge(Seq(), Seq(createRelationship("anon_0", "o", "KNOWS", "a", OUTGOING)), lockNodes = Set("o", "a"))
      .|.expandInto("(o)-[anon_0:KNOWS]->(a)")
      .|.argument("a", "o", "others")
      .setNodeProperty("a", "knows", "size(others)")
      .orderedAggregation(Seq("a AS a"), Seq("COLLECT(b) AS others"), Seq("a"))
      .expandAll("(a)-[r:RELATION]->(b)")
      .filter("a._key = $_key")
      .nodeByLabelScan("a", "PROFILES", IndexOrderAscending)
      .build()
  }

  test("Eager should be inserted between MATCH and FOREACH REMOVE with dynamic label") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("100", 50)
      .build()

    val query =
      """
        |MATCH (), (n:`100`)
        |FOREACH (i IN range(1, 5) | REMOVE n:$(toString(i)))
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .foreach(
        variable = "i",
        expression = "range(1, 5)",
        mutations = Seq(removeLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("toString(i)")))
      )
      .eager(
        ListSet(EagernessReason.UnknownLabelReadRemoveConflict.withConflict(EagernessReason.Conflict(Id(2), Id(5))))
      )
      .cartesianProduct()
      .|.nodeByLabelScan(node = "n", label = "100")
      .allNodeScan("anon_0")
      .build()
  }

  test("Eager should be inserted between FOREACH REMOVE with dynamic label and MATCH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("100", 50)
      .build()

    val query =
      """
        |WITH [i IN range(1, 5) | toString(i)] AS numericLabels
        |MATCH (n:$any(numericLabels))
        |FOREACH (l IN numericLabels | REMOVE n:$(l))
        |WITH *
        |MATCH ()
        |MATCH (m:$all(numericLabels))
        |RETURN count(m) AS shouldBeZero
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner
      .subPlanBuilder()
      .aggregation(groupingExpressions = Seq(), aggregationExpression = Seq("count(m) AS shouldBeZero"))
      .apply()
      .|.cartesianProduct()
      .|.|.filterExpression(hasDynamicLabels(varFor("m"), varFor("numericLabels")))
      .|.|.allNodeScan(node = "m", "n", "numericLabels")
      .|.allNodeScan(node = "anon_0", "n", "numericLabels")
      .eager(
        ListSet(EagernessReason.UnknownLabelReadRemoveConflict.withConflict(EagernessReason.Conflict(Id(8), Id(4))))
      )
      .foreach(
        variable = "l",
        expression = "numericLabels",
        mutations = Seq(removeLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("l")))
      )
      .filterExpression(hasAnyDynamicLabel(varFor("n"), varFor("numericLabels")))
      .apply()
      .|.allNodeScan(node = "n", "numericLabels")
      .projection("[i IN range(1, 5) | toString(i)] AS numericLabels")
      .argument()
      .build()
  }
}
