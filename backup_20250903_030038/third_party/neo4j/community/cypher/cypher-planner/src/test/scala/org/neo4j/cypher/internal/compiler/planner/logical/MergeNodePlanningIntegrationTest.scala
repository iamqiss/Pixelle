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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownLabelReadSetConflict
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.neo4j.graphdb.schema.IndexType

class MergeNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should plan single merge node") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MERGE (a)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(nodes = Seq(createNode("a")))
      .allNodeScan("a")
      .build()
  }

  test("should plan single merge node from a label scan") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 30)
      .build()

    val plan = cfg.plan("MERGE (a:X)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(nodes = Seq(createNode("a", "X")))
      .nodeByLabelScan("a", "X")
      .build()
  }

  test("should plan single merge node with properties") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MERGE (a {prop: 42})").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(nodes = Seq(createNodeWithProperties("a", Seq.empty, "{prop: 42}")))
      .filter("a.prop = 42")
      .allNodeScan("a")
      .build()
  }

  test("should plan create followed by merge") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("CREATE (a) MERGE (b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(nodes = Seq(createNode("b")))
      .|.allNodeScan("b")
      .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(6), Id(4)))))
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("should plan merge followed by create") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MERGE(a) CREATE (b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNode("b"))
      .merge(nodes = Seq(createNode("a")))
      .allNodeScan("a")
      .build()
  }

  test("should use AssertSameNode when multiple unique indexes match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("X", 5)
      .setLabelCardinality("Y", 5)
      .addNodeIndex("X", Seq("prop"), 1.0, 0.1, isUnique = true)
      .addNodeIndex("Y", Seq("prop"), 1.0, 0.1, isUnique = true)
      .build()

    val plan = planner.plan("MERGE (a:X:Y {prop: 42})").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .merge(Seq(createNodeWithProperties("a", Seq("X", "Y"), "{prop: 42}")), Seq(), Seq(), Seq())
      .assertSameNode("a")
      .|.nodeIndexOperator("a:Y(prop = 42)", unique = true, indexType = IndexType.RANGE)
      .nodeIndexOperator("a:X(prop = 42)", unique = true, indexType = IndexType.RANGE)
      .build())
  }

  test("should use AssertSameNode when multiple unique indexes match, after a MATCH clause") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("X", 5)
      .setLabelCardinality("Y", 5)
      .setLabelCardinality("Z", 5)
      .addNodeIndex("X", Seq("prop"), 1.0, 0.1, isUnique = true)
      .addNodeIndex("Y", Seq("prop"), 1.0, 0.1, isUnique = true)
      .build()

    val plan = planner.plan("MATCH (n:Z) MERGE (a:X:Y {prop: n.prop})").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(
        Seq(createNodeWithProperties("a", Seq("X", "Y"), "{prop: cacheNFromStore[n.prop]}")),
        Seq(),
        Seq(),
        Seq()
      )
      .|.assertSameNode("a")
      .|.|.nodeIndexOperator(
        "a:Y(prop = ???)",
        argumentIds = Set("n"),
        paramExpr = Some(cachedNodePropFromStore("n", "prop")),
        unique = true,
        indexType = IndexType.RANGE
      )
      .|.nodeIndexOperator(
        "a:X(prop = ???)",
        argumentIds = Set("n"),
        paramExpr = Some(cachedNodePropFromStore("n", "prop")),
        unique = true,
        indexType = IndexType.RANGE
      )
      .nodeByLabelScan("n", "Z")
      .build())
  }

  test("should not use AssertSameNode when one unique index matches") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("X", 5)
      .setLabelCardinality("Y", 5)
      .addNodeIndex("X", Seq("prop"), 1.0, 0.1, isUnique = true)
      .build()

    val plan = planner.plan("MERGE (a:X:Y {prop: 42})").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .merge(Seq(createNodeWithProperties("a", Seq("X", "Y"), "{prop: 42}")), Seq(), Seq(), Seq())
      .filter("a:Y")
      .nodeIndexOperator("a:X(prop = 42)", unique = true, indexType = IndexType.RANGE)
      .build())
  }

  test("should use AssertSameNode with PatternComprehension") {
    val query =
      """
        |MERGE (n:X:Y {prop: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)})
        |RETURN n
      """.stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("X", 5)
      .setLabelCardinality("Y", 5)
      .setAllRelationshipsCardinality(10)
      .addNodeIndex("X", Seq("prop"), 1.0, 0.1, isUnique = true)
      .addNodeIndex("Y", Seq("prop"), 1.0, 0.1, isUnique = true)
      .build()

    val plan = planner.plan(query).stripProduceResults

    plan should beLike {
      case Merge(
          AssertSameNode(
            LogicalVariable("n"),
            Apply(
              RollUpApply(Argument(SetExtractor()), _ /* <- This is the subQuery */, collectionName1, _),
              NodeUniqueIndexSeek(LogicalVariable("n"), _, _, _, SetExtractor(argumentName1), _, _, _)
            ),
            Apply(
              RollUpApply(Argument(SetExtractor()), _ /* <- This is the subQuery */, collectionName2, _),
              NodeUniqueIndexSeek(LogicalVariable("n"), _, _, _, SetExtractor(argumentName2), _, _, _)
            )
          ),
          _,
          _,
          _,
          _,
          _
        ) if collectionName1 == argumentName1 && collectionName2 == argumentName2 => ()
    }
  }

  test("should plan merge node with on create and on match ") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MERGE (a) ON CREATE SET a.prop = 1 ON MATCH SET a:L").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(
        nodes = Seq(createNode("a")),
        onMatch = Seq(setLabel("a", "L")),
        onCreate = Seq(setNodeProperty("a", "prop", "1"))
      )
      .allNodeScan("a")
      .build()
  }

  test("single node - should add argument for dependency of ON MATCH") {
    val query =
      """
        |WITH 5 AS five SKIP 0
        |MERGE (a) ON MATCH SET a.p = five
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()

    planner
      .plan(query)
      .stripProduceResults should equal(
      planner.subPlanBuilder()
        .emptyResult()
        .apply()
        .|.merge(Seq(createNode("a")), onMatch = Seq(setNodeProperty("a", "p", "five")))
        .|.allNodeScan("a", "five")
        .projection("5 AS five")
        .skip(0)
        .argument()
        .build()
    )
  }

  test("single node - should add argument for dependency of ON CREATE") {
    val query =
      """
        |WITH 5 AS five SKIP 0
        |MERGE (a) ON CREATE SET a.p = five
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()

    planner
      .plan(query)
      .stripProduceResults should equal(
      planner.subPlanBuilder()
        .emptyResult()
        .apply()
        .|.merge(Seq(createNode("a")), onCreate = Seq(setNodeProperty("a", "p", "five")))
        .|.allNodeScan("a", "five")
        .projection("5 AS five")
        .skip(0)
        .argument()
        .build()
    )
  }

  test("relationship - should add argument for dependency of ON MATCH") {
    val query =
      """
        |WITH 5 AS five SKIP 0
        |MERGE (a)-[r:R]->(b) ON MATCH SET a.p = five
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    planner
      .plan(query)
      .stripProduceResults should equal(
      planner.subPlanBuilder()
        .emptyResult()
        .apply()
        .|.merge(
          Seq(createNode("a"), createNode("b")),
          Seq(createRelationship("r", "a", "R", "b")),
          onMatch = Seq(setNodeProperty("a", "p", "five"))
        )
        .|.relationshipTypeScan("(a)-[r:R]->(b)", "five")
        .projection("5 AS five")
        .skip(0)
        .argument()
        .build()
    )
  }

  test("relationship, one MERGE in between - should add argument for dependency of ON MATCH") {
    val query =
      """
        |WITH 5 AS five SKIP 0
        |MERGE (a)
        |MERGE (a)-[r:R]->(b) ON MATCH SET a.p = five
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    planner
      .plan(query)
      .stripProduceResults should equal(
      planner.subPlanBuilder()
        .emptyResult()
        .apply()
        .|.merge(
          Seq(createNode("b")),
          Seq(createRelationship("r", "a", "R", "b")),
          onMatch = Seq(setNodeProperty("a", "p", "five")),
          lockNodes = Set("a")
        )
        .|.expandAll("(a)-[r:R]->(b)")
        .|.argument("a", "five")
        .eager(
          ListSet(
            ReadCreateConflict.withConflict(Conflict(Id(3), Id(9))),
            ReadCreateConflict.withConflict(Conflict(Id(8), Id(4)))
          )
        )
        .apply()
        .|.merge(Seq(createNode("a")))
        .|.allNodeScan("a")
        .projection("5 AS five")
        .skip(0)
        .argument()
        .build()
    )
  }

  test("merge a single node with a combination of static and dynamic labels") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """MERGE (account IS Account & $($roles))
        |RETURN account""".stripMargin

    val plan = planner.plan(query)

    val expected = planner
      .planBuilder()
      .produceResults("account")
      .merge(nodes = Seq(createNodeFull("account", labels = Seq("Account"), dynamicLabels = Seq("$roles"))))
      .filterExpression(hasDynamicLabels(varFor("account"), parameter("roles", CTAny)))
      .nodeByLabelScan("account", "Account")
      .build()

    plan shouldEqual expected
  }

  test("insert an eager between merging a node with a dynamic label and reading nodes") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """MERGE (:$($label))
        |WITH *
        |MATCH (n:Account)
        |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("n")
      .apply()
      .|.nodeByLabelScan("n", "Account", IndexOrderNone, "anon_0")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))))
      .merge(nodes = Seq(createNodeFull("anon_0", dynamicLabels = Seq("$label"))))
      .filterExpression(hasDynamicLabels(varFor("anon_0"), parameter("label", CTAny)))
      .allNodeScan("anon_0")
      .build()
  }

  test("insert an eager between reading nodes and merging a node with a dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (n:Account)
        |MERGE (:$($label))""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(Seq(createNodeFull("anon_0", dynamicLabels = Seq("$label"))))
      .|.filterExpression(hasDynamicLabels(varFor("anon_0"), parameter("label", CTAny)))
      .|.allNodeScan("anon_0")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(3), Id(8)))))
      .apply()
      .|.nodeByLabelScan("n", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("Eager should be inserted between MATCH and MERGE ON CREATE with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (account:Account)
        |MERGE (n)
        |ON CREATE
        |  SET n:$($label)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(nodes = Seq(createNodeFull("n")), onCreate = Seq(setLabel("n", Seq(), Seq("$label"))))
      .|.allNodeScan("n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(7)))))
      .apply()
      .|.nodeByLabelScan("account", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("Eager should be inserted between MATCH and MERGE ON MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (account:Account)
        |MERGE (n)
        |ON MATCH
        |  SET n:$($label)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(nodes = Seq(createNodeFull("n")), onMatch = Seq(setLabel("n", Seq(), Seq("$label"))))
      .|.allNodeScan("n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(7)))))
      .apply()
      .|.nodeByLabelScan("account", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("Eager should be inserted between MERGE ON CREATE and MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("Account", 50)
        .setLabelCardinality("Person", 50)
        .build()

    val query =
      """MERGE (n:Account)
        |ON CREATE
        |  SET n:$($label)
        |WITH *
        |MATCH (account:Person)
        |RETURN account""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("account")
      .apply()
      .|.nodeByLabelScan(node = "account", label = "Person", "n")
      .eager(
        ListSet(
          UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(0))),
          EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))
        )
      )
      .merge(
        nodes = Seq(createNodeFull(node = "n", labels = Seq("Account"))),
        onCreate = Seq(setLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("$label")))
      )
      .nodeByLabelScan(node = "n", label = "Account")
      .build()
  }

  test("Eager should be inserted between MERGE ON MATCH and MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("Account", 50)
        .setLabelCardinality("Person", 50)
        .build()

    val query =
      """MERGE (n:Account)
        |ON MATCH
        |  SET n:$($label)
        |WITH *
        |MATCH (account:Person)
        |RETURN account""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("account")
      .apply()
      .|.nodeByLabelScan(node = "account", label = "Person", "n")
      .eager(
        ListSet(
          UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(0))),
          EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))
        )
      )
      .merge(
        nodes = Seq(createNodeFull(node = "n", labels = Seq("Account"))),
        onMatch = Seq(setLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("$label")))
      )
      .nodeByLabelScan(node = "n", label = "Account")
      .build()
  }

  test("Eager should be inserted between MATCH and subquery containing MERGE ON CREATE with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("A", 20)
        .setLabelCardinality("B", 20)
        .build()

    val query =
      """MATCH (), (n:A)
        |CALL (n) {
        |  MERGE (m:B)
        |  ON CREATE
        |    SET m:$(labels(n))
        |  RETURN m.p AS mp
        |}
        |RETURN n.p AS np, mp""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("np", "mp")
      .projection("n.p AS np")
      .apply()
      .|.projection("m.p AS mp")
      .|.merge(
        nodes = Seq(createNodeFull(node = "m", labels = Seq("B"))),
        onCreate = Seq(setLabel(node = "m", staticLabels = Seq(), dynamicLabelExpressions = Seq("labels(n)")))
      )
      .|.nodeByLabelScan(node = "m", label = "B", "n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(8)))))
      .cartesianProduct()
      .|.nodeByLabelScan(node = "n", label = "A")
      .allNodeScan("anon_0")
      .build()
  }
}
