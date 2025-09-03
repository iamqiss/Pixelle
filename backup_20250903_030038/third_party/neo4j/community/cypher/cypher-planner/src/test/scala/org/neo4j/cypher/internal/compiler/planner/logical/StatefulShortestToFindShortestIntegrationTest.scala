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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.ALL_IF_POSSIBLE
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.INTO_ONLY
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.WindowsSafeAnyRef
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.AllowSameNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatefulShortestToFindShortestIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  // We compare "solvedExpressionString" nested inside LogicalPlans.
  // This saves us from windows line break mismatches in those strings.
  implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

  private val plannerBase = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .addNodeIndex("User", Seq("prop"), 1.0, 0.25)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("()-[:RR]->()", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    // This makes it deterministic which plans ends up on what side of a CartesianProduct.
    .setExecutionModel(Volcano)

  private val planner = plannerBase
    // For the rewrite to trigger, we need an INTO plan
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, INTO_ONLY)
    .build()

  private val plannerForShardedDatabases = plannerBase
    .setDatabaseMode(DatabaseMode.SHARDED)
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, INTO_ONLY)
    .build()

  private val all_if_possible_planner = plannerBase
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
    .build()

  test("Shortest should be rewritten to legacy shortest for simple varLength pattern") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a:User)-[r*]->(b:User)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeByLabelScan("b", "User")
      .nodeByLabelScan("a", "User")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("Shortest Group should be rewritten to legacy all shortest for simple varLength pattern") {
    val query =
      s"""
         |MATCH (a), (b)
         |WITH * SKIP 1
         |MATCH p = SHORTEST GROUP (a:User)-[r*]->(b:User)
         |RETURN *
         |""".stripMargin
    val plan = all_if_possible_planner.plan(query).stripProduceResults
    val expected = all_if_possible_planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        all = true,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .filterExpression(andsReorderableAst(hasLabels("a", "User"), hasLabels("b", "User")))
      .skip(1)
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for varLength pattern with outer relationship predicate") {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r*]->(b) WHERE all(x IN r WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("x", "x.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for varLength pattern with inlined relationship predicate") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a)-[r* {prop: 10}]->(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("anon_0", "anon_0.prop = 10")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for varLength pattern with predicate containing an all nodes reference predicate"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (p = (a)-[r*]->(b) WHERE all(x IN nodes(p) WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(Predicate("x", "x.prop > 5")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for varLength with predicate containing an all relationships reference predicate"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (p = (a)-[r*]->(b) WHERE all(x IN relationships(p) WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("x", "x.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple QPP") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a)-[r]->*(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(a)-[r*0..]->(b)",
          pathName = Some("anon_0"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(),
          sameNodeMode = AllowSameNode
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest GROUP should be rewritten to legacy all shortest for simple QPP") {
    val query =
      s"""
         |MATCH (a), (b)
         |WITH * SKIP 1
         |MATCH SHORTEST GROUP (a)-[r]->*(b)
         |RETURN *
         |""".stripMargin
    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(all_if_possible_planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*0..]->(b)",
        pathName = Some("anon_0"),
        all = true,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .skip(1)
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP with outer relationship predicate and inlined relationship Type"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r:R]->*(b) WHERE all(x IN r WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r:R*0..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("anon_0", "anon_0.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP Kleene star if all quantified nodes share the same predicates and one of the boundary nodes also covers those predicates"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a{prop: 1}) (({prop: 1})-[r]->({prop: 1})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*0..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(Predicate("anon_0", "anon_0.prop = 1")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .filter("a.prop = 1")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP Kleene star if none of the juxtaposed nodes cover the quantified nodes predicates"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) (({prop: 1})-[r]->({prop: 1})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(a)-[r*0..]->(b)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates =
            Seq(Predicate("anon_0", "startNode(anon_0).prop = 1"), Predicate("anon_0", "endNode(anon_0).prop = 1")),
          sameNodeMode = AllowSameNode
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple varLength pattern with set upper bound") {
    val query =
      s"""
         |MATCH (a:User), (b:User)
         |WITH * SKIP 0
         |MATCH ANY SHORTEST (a)-[*0..1]-(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(a)-[anon_0*0..1]-(b)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(),
          sameNodeMode = AllowSameNode
        )
        .skip(0)
        .cartesianProduct()
        .|.nodeByLabelScan("b", "User")
        .nodeByLabelScan("a", "User")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP with different predicates on the quantified nodes."
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) (({prop: 1})-[r]->({prop: 2})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(a)-[r*0..]->(b)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates =
            Seq(Predicate("anon_0", "startNode(anon_0).prop = 1"), Predicate("anon_0", "endNode(anon_0).prop = 2")),
          sameNodeMode = AllowSameNode
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest with multiple QPPs should not be rewritten to legacy shortest") {
    val query =
      s"""
         |MATCH (a:User), (b:User), (x)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (a)-->+(x)-->+(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (anon_0)")
      .addTransition(1, 2, "(anon_0)-[anon_1]->(anon_2)")
      .addTransition(2, 1, "(anon_2) (anon_0)")
      .addTransition(2, 3, "(anon_2) (x WHERE x = x)")
      .addTransition(3, 4, "(x) (anon_3)")
      .addTransition(4, 5, "(anon_3)-[anon_4]->(anon_5)")
      .addTransition(5, 4, "(anon_5) (anon_3)")
      .addTransition(5, 6, "(anon_5) (b)")
      .setFinalState(6)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 (a) ((`anon_0`)-[`anon_1`]->(`anon_2`)){1, } (`x`) ((`anon_3`)-[`anon_4`]->(`anon_5`)){1, } (b)",
          None,
          Set(),
          Set(),
          Set(("x", "x")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto,
          false,
          2,
          None
        )
        .skip(1)
        .cartesianProduct()
        .|.cartesianProduct()
        .|.|.allNodeScan("x")
        .|.nodeByLabelScan("b", "User")
        .nodeByLabelScan("a", "User")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for QPP with post filter referencing relationship") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a:User)-[r]->+(b:User)
         |WHERE none(rel IN r WHERE rel.prop = 1)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .filter("none(rel IN r WHERE rel.prop = 1)")
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeByLabelScan("a", "User")
      .nodeByLabelScan("b", "User")
      .build()
    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should NOT be rewritten to legacy shortest for QPP with inverted pre-filter referencing relationship"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST ((:User)-[r]->+(:User) WHERE none(rel IN r WHERE rel.prop = 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "anon_0")
      .addTransition(0, 1, "(anon_0) (anon_2)")
      .addTransition(1, 2, "(anon_2)-[r]->(anon_3)")
      .addTransition(2, 1, "(anon_3) (anon_2)")
      .addTransition(2, 3, "(anon_3) (anon_1)")
      .setFinalState(3)
      .build()
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "anon_0",
        "anon_1",
        "SHORTEST 1 (`anon_0`) ((`anon_2`)-[`r`]->(`anon_3`)){1, } (`anon_1`)",
        Some("none(rel IN r WHERE rel.prop = 5)"),
        Set(),
        Set(("r", "r")),
        Set(),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandInto,
        false,
        1,
        None
      )
      .cartesianProduct()
      .|.nodeByLabelScan("anon_1", "User")
      .nodeByLabelScan("anon_0", "User")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten since inner nodes are not referenced") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) ((c)-[r]->(d)) +(b)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten even if path is referenced in return") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a) ((c)-[r]->(d)) +(b)
         |RETURN p
         |""".stripMargin
    val plan = planner.plan(query)
    val pathExpression = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val expected = planner.planBuilder()
      .produceResults("p")
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten even if path is referenced in return, incoming rel") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a) ((c)<-[r]-(d)) +(b)
         |RETURN p
         |""".stripMargin
    val plan = planner.plan(query)
    val pathExpression = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), INCOMING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val expected = planner.planBuilder()
      .produceResults("p")
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(a)<-[r*1..]-(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten since inner nodes are only referenced by inner QPP predicates") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) ((c)-[r]->(d) WHERE c.prop = 1 AND d.prop = 1) +(b)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(Predicate("anon_0", "anon_0.prop = 1")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.filter("b.prop = 1")
      .|.allNodeScan("b")
      .filter("a.prop = 1")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten since inner nodes are not referenced with large NFA") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r]->{1,101}(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*1..101]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST with varLengthPattern should be rewritten since inner nodes are not referenced with large NFA") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r*1..101]->(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*1..101]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "SHORTEST with varLengthPattern with total bound set should be rewritten since inner nodes are not referenced with large NFA"
  ) {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r*101]->(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*101]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "SHORTEST with varLengthPattern with lower and upper bound set should be rewritten since inner nodes are not referenced with large NFA"
  ) {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r*101..1001]->(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*101..1001]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "SHORTEST with varLengthPattern with only lower bound set should be rewritten since inner nodes are not referenced with large NFA"
  ) {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r*101..1001]->(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*101..1001]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "SHORTEST with varLengthPattern with only upper bound set should be rewritten since inner nodes are not referenced with large NFA"
  ) {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (start:User {prop: 1})-[r*..101]->(end:User {prop: 1})
         |RETURN p
         |""".stripMargin
    val pathExpression = PathExpression(NodePathStep(
      v"start",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("end")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(start)-[r*..101]->(end)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeIndexOperator("end:User(prop = 1)")
      .nodeIndexOperator("start:User(prop = 1)")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Should not rewrite shortest with more than one relationship"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b)<-[rr:RR]-(c) WHERE a.prop <> b.prop AND c.name ='foo')+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (a)")
      .addTransition(1, 2, "(a)-[r:R WHERE NOT startNode(r).prop = endNode(r).prop]->(b)")
      .addTransition(2, 3, "(b)<-[rr:RR]-(c WHERE c.name = 'foo')")
      .addTransition(3, 1, "(c) (a)")
      .addTransition(3, 4, "(c) (t)")
      .setFinalState(4)
      .build()
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "s",
          "t",
          "SHORTEST 1 (s) ((`a`)-[`r`]->(`b`)<-[`rr`]-(`c`)){1, } (t)",
          None,
          Set(),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto,
          false,
          2,
          None
        )
        .filter("t.name = 'foo'")
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest QPP with predicates on both inner nodes but none of the quantified nodes outside of the Shortest should be rewritten"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b) WHERE a.prop <> b.prop)+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(s)-[r:R*1..]->(t)",
          pathName = Some("anon_1"),
          relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0).prop = endNode(anon_0).prop")),
          sameNodeMode = AllowSameNode
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest QPP with predicates on only one side of the relationship should get rewritten as relationship predicates"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b) WHERE a.name <> "foo")+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(s)-[r:R*1..]->(t)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0).name = 'foo'")),
          sameNodeMode = AllowSameNode
        )
        .filter("NOT cacheN[s.name] = 'foo'")
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .cacheProperties("cacheNFromStore[s.name]")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest QPP with predicates a mixture of predicates comparing both nodes and also comparing for values on both nodes should be rewritten"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b) WHERE a.prop <> b.prop AND a.name <> "foo" AND b.name <> "foo")+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(s)-[r:R*1..]->(t)",
          pathName = Some("anon_2"),
          nodePredicates = Seq(Predicate("anon_0", "NOT anon_0.name = 'foo'")),
          relationshipPredicates = Seq(Predicate("anon_1", "NOT startNode(anon_1).prop = endNode(anon_1).prop")),
          sameNodeMode = AllowSameNode
        )
        .filter("NOT cacheN[s.name] = 'foo'", "NOT t.name = 'foo'")
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .cacheProperties("cacheNFromStore[s.name]")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest QPP with no references to the quantified nodes outside of the Shortest should be rewritten in the correct direction"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)<-[r:R]-(b) WHERE a.prop < b.prop)+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(s)<-[r:R*1..]-(t)",
          pathName = Some("anon_1"),
          relationshipPredicates = Seq(Predicate("anon_0", "endNode(anon_0).prop < startNode(anon_0).prop")),
          sameNodeMode = AllowSameNode
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP Kleene star with predicates on inner nodes only"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b) WHERE a.prop <> b.prop)*(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(s)-[r:R*0..]->(t)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0).prop = endNode(anon_0).prop")),
          sameNodeMode = AllowSameNode
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  /*
   * SHARDED DATABASES MODE
   */
  test(
    "Shortest should not be rewritten to legacy shortest if predicates use properties"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:R]->(b) WHERE a.prop <> b.prop)*(t)
         |RETURN r
         |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults

    plan should equal(
      plannerForShardedDatabases.subPlanBuilder()
        .statefulShortestPath(
          "s",
          "t",
          "SHORTEST 1 (s) ((`a`)-[`r`]->(`b`)){0, } (t)",
          None,
          Set(),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "s")
            .addTransition(0, 1, "(s) (a)")
            .addTransition(0, 3, "(s) (t)")
            .addTransition(1, 2, "(a)-[r:R WHERE NOT startNode(r).prop = endNode(r).prop]->(b)")
            .addTransition(2, 1, "(b) (a)")
            .addTransition(2, 3, "(b) (t)")
            .setFinalState(3)
            .build(),
          ExpandInto,
          false,
          0,
          None
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple QPP for sharded databases") {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r:R]->*(b) WHERE all(x IN r WHERE endNode(x):User))
         |RETURN *
         |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults
    plan should equal(plannerForShardedDatabases.subPlanBuilder()
      .shortestPathExpr(
        "(a)-[r:R*0..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(VariablePredicate(v"anon_0", hasLabels(endNode("anon_0"), "User"))),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should not be rewritten to legacy shortest for varLength patterns with properties for sharded databases"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r*]->(b) WHERE all(x IN r WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults
    plan should equal(plannerForShardedDatabases.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 (a)-[r*]->(b)",
        Some("all(x IN r WHERE x.prop > 5)"),
        Set(),
        Set(("r", "r")),
        Set(),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (anon_0)")
          .addTransition(1, 2, "(anon_0)-[r]->(anon_1)")
          .addTransition(2, 2, "(anon_1)-[r]->(anon_1)")
          .addTransition(2, 3, "(anon_1) (b)")
          .setFinalState(3)
          .build(),
        ExpandInto,
        false,
        1,
        None
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple varLength pattern for sharded databases") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a:User)-[r*]->(b:User)
         |RETURN *
         |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults
    val expected = plannerForShardedDatabases.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeByLabelScan("b", "User")
      .nodeByLabelScan("a", "User")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  def multiOutgoingRelationshipPath(fromNode: String, relationships: String, toNode: String): PathExpression = {
    PathExpression(
      NodePathStep(
        node = varFor(fromNode),
        MultiRelationshipPathStep(
          rel = varFor(relationships),
          direction = OUTGOING,
          toNode = Some(varFor(toNode)),
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
    )(InputPosition.NONE)
  }
}
