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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.InferSchemaPartsStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.RepetitionCardinalityModel
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import scala.math.sqrt

class CardinalityIntegrationTest extends CypherFunSuite with CardinalityIntegrationTestSupport {

  private val allNodes = 733.0
  private val personCount = 324.0
  private val relCount = 50.0
  private val rel2Count = 78.0

  test("should agree between QPP with 2 relationships and its expansion") {
    val config = plannerBuilder()
      .setAllNodesCardinality(200)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 20)
      .setRelationshipCardinality("()-[]->()", 70)
      .setRelationshipCardinality("()-[:R]->()", 40)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 35)
      .setRelationshipCardinality("()-[:R]->(:B)", 25)
      .setRelationshipCardinality("()-[:S]->()", 48)
      .setRelationshipCardinality("(:B)-[:S]->()", 12)
      .setRelationshipCardinality("(:B)-[:R]->()", 15)
      .build()

    val expectedCardinality = 35 * 40 * 40 * 15 / math.pow(200, 3) * math.pow(.99, 6)

    queryShouldHaveCardinality(
      config,
      "MATCH (a:A)(()-[r:R]->()<-[s:R]-()){2}(b:B)",
      expectedCardinality
    )

    queryShouldHaveCardinality(
      config,
      "MATCH (a:A)-[r1:R]->()<-[s1:R]-()-[r2:R]->()<-[s2:R]-(b:B)",
      expectedCardinality
    )
  }

  test("query containing a WITH and LIMIT on low/fractional cardinality") {
    val i = .1
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.min(i, 10.0) * relCount / i
    )
  }

  test("query containing a WITH and LIMIT on high cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.min(i, 10.0) * relCount / i
    )
  }

  test("query containing a WITH and LIMIT on parameterized cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT $limit MATCH (a)-[:REL]->()",
      Math.min(i, DEFAULT_LIMIT_CARDINALITY) * relCount / i
    )
  }

  test("query containing a WITH and aggregation vol. 2") {
    val patternNodeCrossProduct = allNodes * allNodes
    val labelSelectivity = personCount / allNodes
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = rel2Count / maxRelCount
    val firstQG = patternNodeCrossProduct * labelSelectivity * relSelectivity
    val aggregation = Math.sqrt(firstQG)

    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personCount)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL2]->()", rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL2]->()", rel2Count)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person)-[:REL2]->(b) WITH a, count(*) as c MATCH (a)-[:REL]->()",
      aggregation * relCount / personCount
    )
  }

  test("query containing both SKIP and LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n SKIP 5 LIMIT 10", Math.min(i, 10.0))
  }

  test("query containing LIMIT by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n LIMIT toInteger(1+1)", Math.min(i, 2.0))
  }

  test("query containing both SKIP and LIMIT with large skip, so skip + limit exceeds total row count boundary") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      s"MATCH (n:Person) WITH n SKIP ${(personCount - 5).toInt} LIMIT 10",
      Math.min(i, 5.0)
    )
  }

  test("query containing SKIP by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, s"MATCH (n:Person) WITH n SKIP toInteger($personCount - 2)", Math.min(i, 2.0))
  }

  test("should reduce cardinality for a WHERE after a WITH") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 WHERE a.age = 20",
      Math.min(i, 10.0) * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality using index stats for a WHERE after a WITH") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .addNodeIndex("Person", Seq("age"), 0.3, 0.2)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a, 1 AS x WHERE a.age = 20", i * 0.3 * 0.2)
  }

  test("should reduce cardinality for a WHERE after a WITH, unknown LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT $limit WHERE a.age = 20",
      Math.min(i, DEFAULT_LIMIT_CARDINALITY) * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with ORDER BY") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a ORDER BY a.name WHERE a.age = 20",
      i * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with DISTINCT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH DISTINCT a WHERE a.age = 20",
      i * DEFAULT_DISTINCT_SELECTIVITY * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION without grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH count(a) AS count WHERE count > 20",
      DEFAULT_RANGE_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION with grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH count(a) AS count, a.name AS name WHERE count > 20",
      Math.sqrt(i) * DEFAULT_RANGE_SELECTIVITY
    )
  }

  private val signature = ProcedureSignature(
    QualifiedName(Seq("my", "proc"), "foo"),
    IndexedSeq(FieldSignature("int", CTInteger)),
    Some(IndexedSeq(FieldSignature("x", CTNode))),
    None,
    ProcedureReadOnlyAccess,
    id = 0
  )

  test("standalone procedure call should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "CALL my.proc.foo(42) YIELD x", DEFAULT_MULTIPLIER)
  }

  test("procedure call with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x", 1)
  }

  test("procedure call with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x", DEFAULT_MULTIPLIER * inputSize)
  }

  test("standalone LOAD CSV should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "LOAD CSV FROM 'foo' AS csv", DEFAULT_MULTIPLIER)
  }

  test("LOAD CSV with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv", 1)
  }

  test("LOAD CSV with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv", DEFAULT_MULTIPLIER * inputSize)
  }

  test("UNWIND with no information should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND $foo AS i", DEFAULT_MULTIPLIER)
  }

  test("UNWIND with empty list literal should have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [] AS i", 0.0)
  }

  test("UNWIND with non-empty list literal should have list size cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [1, 2, 3, 4, 5] AS i", 5)
  }

  test("UNWIND with single element range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 0) AS i", 1)
  }

  test("UNWIND with empty range 1") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, -1) AS i", 0.0)
  }

  test("UNWIND with empty range 2") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 0, 1) AS i", 0.0)
  }

  test("UNWIND with empty range 3") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 10, -1) AS i", 0.0)
  }

  test("UNWIND with non-empty range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 10) AS i", 10)
  }

  test("UNWIND with non-empty DESC range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 1, -1) AS i", 10)
  }

  test("UNWIND with non-empty range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 2) AS i", 5)
  }

  test("UNWIND with non-empty DESC range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -2) AS i", 5)
  }

  test("UNWIND with non-empty range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 3) AS i", 3)
  }

  test("UNWIND with non-empty DESC range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -3) AS i", 3)
  }

  test("empty graph") {
    val config = plannerBuilder()
      .setAllNodesCardinality(0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a) WHERE a.prop = 10", 0)
  }

  test("honours bound arguments") {
    val relCount = 1000.0
    val fooCount = 100.0
    val barCount = 400.0
    val inboundCardinality = 13
    val nodeCount = fooCount + barCount
    val config = plannerBuilder()
      .setAllNodesCardinality(nodeCount)
      .setLabelCardinality("FOO", fooCount)
      .setLabelCardinality("BAR", barCount)
      .setAllRelationshipsCardinality(relCount)
      .setRelationshipCardinality("()-[:TYPE]->()", relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->()", relCount)
      .setRelationshipCardinality("()-[:TYPE]->(:BAR)", relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->(:BAR)", relCount)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:FOO) WITH a LIMIT 1 UNWIND range(1, $inboundCardinality) AS i MATCH (a)-[:TYPE]->(b:BAR)",
      relCount / fooCount * inboundCardinality
    )
    queryShouldHaveCardinality(
      config,
      s"MATCH (a:FOO) WITH a LIMIT 1 UNWIND range(1, $inboundCardinality) AS i MATCH (a:FOO)-[:TYPE]->(b:BAR)",
      relCount / fooCount * inboundCardinality
    )
    queryShouldHaveCardinality(
      config,
      s"MATCH (a) WITH a LIMIT 1 UNWIND range(1, $inboundCardinality) AS i MATCH (a:FOO)-[:TYPE]->(b:BAR)",
      relCount / nodeCount * inboundCardinality
    )
  }

  test("input cardinality <1.0 => input cardinality * scan cardinality") {
    val inboundCardinality = 1
    val whereSelectivity = 0.5
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("Foo", inboundCardinality)
      .addNodeIndex("Foo", Seq("bar"), 1.0, whereSelectivity)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      inboundCardinality * whereSelectivity * nodes
    )
  }

  test("input cardinality >1.0 => input cardinality * scan cardinality") {
    val inboundCardinality = 10
    val whereSelectivity = 1.0
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("Foo", inboundCardinality)
      .addNodeIndex("Foo", Seq("bar"), 1.0, whereSelectivity)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      inboundCardinality * whereSelectivity * nodes
    )
  }

  test("should use relationship index for cardinality estimation with inlined type predicate") {
    val inboundCardinality = 10
    val whereSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("A", inboundCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", inboundCardinality)
      .setRelationshipCardinality("()-[:R]->()", inboundCardinality)
      .setAllRelationshipsCardinality(10)
      .addRelationshipIndex("R", Seq("prop"), whereSelectivity, whereSelectivity)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:A)-[r:R]->() WHERE r.prop IS NOT NULL",
      inboundCardinality * whereSelectivity
    )
  }

  test("should use relationship index for cardinality estimation with non-inlined type predicate") {
    val inboundCardinality = 10
    val whereSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("A", inboundCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", inboundCardinality)
      .setRelationshipCardinality("()-[:R]->()", inboundCardinality)
      .setAllRelationshipsCardinality(10)
      .addRelationshipIndex("R", Seq("prop"), whereSelectivity, whereSelectivity)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:A)-[r]->() WHERE r:R AND r.prop IS NOT NULL",
      inboundCardinality * whereSelectivity
    )
  }

  test("should only use predicates marked as solved for cardinality estimation of node index seek") {
    val labelCardinality = 50
    val existsSelectivity = 0.5
    val uniqueSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", labelCardinality)
      .addNodeIndex("Person", Seq("prop1", "prop2"), existsSelectivity, uniqueSelectivity)
      .build()

    val query = "MATCH (n:Person) WHERE n.prop1 > 0 AND n.prop2 = 0"

    val planState = config.planState(query + " RETURN n")
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.effectiveCardinalities
    val nodeIndexSeekCardinality = plan.flatten(CancellationChecker.neverCancelled())
      .collectFirst { case lp: NodeIndexSeek => cardinalities.get(lp.id) }.get

    // The range selectivity defaults to equality selectivity if there are few unique values.
    nodeIndexSeekCardinality.amount shouldEqual (labelCardinality * existsSelectivity * sqrt(uniqueSelectivity))

    queryShouldHaveCardinality(config, query, labelCardinality * existsSelectivity * uniqueSelectivity)
  }

  test("node by id seek should have cardinality 1") {
    val allNodesCardinality = 1000
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .build()

    val query = "MATCH (n) WHERE id(n) = 5"
    queryShouldHaveCardinality(config, query, 1)
  }

  test("directed relationship by id seek should have cardinality 1") {
    val allNodesCardinality = 1000
    val allRelationshipsCardinality = 20
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .setAllRelationshipsCardinality(allRelationshipsCardinality)
      .build()

    val query = "MATCH ()-[r]->() WHERE id(r) = 5"
    queryShouldHaveCardinality(config, query, 1)
  }

  test("undirected relationship by id seek should have cardinality 2") {
    val allNodesCardinality = 1000
    val allRelationshipsCardinality = 20
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .setAllRelationshipsCardinality(allRelationshipsCardinality)
      .build()

    val query = "MATCH ()-[r]-() WHERE id(r) = 5"
    queryShouldHaveCardinality(config, query, 2)
  }

  test("relationship type scan on bound start node should correctly calculate cardinality") {
    val aCardinality = 500
    val rCardinality = 10
    val arCardinality = 7

    val config = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query =
      """
        |MATCH (a:A)
        |WITH a, 1 AS foo
        |MATCH (a)-[r:R]->() USING SCAN r:R
        |""".stripMargin

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipTypeScan(LogicalVariable("r"), _, _, _, _, _) => true
      },
      aCardinality * rCardinality
    )
    // The whole query checks that r's start node is a
    queryShouldHaveCardinality(config, query, arCardinality)
  }

  test("relationship type scan with equal start and end node should correctly calculate cardinality") {
    val aCardinality = 500
    val rCardinality = 100
    val araCardinality = 7

    val config = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", araCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", araCardinality * 5)
      .build()

    val query = "MATCH (a:A)-[r:R]->(a)"

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipTypeScan(LogicalVariable("r"), _, _, _, _, _) => true
      },
      rCardinality
    )
  }

  test("relationship by id seek on bound start node should correctly calculate cardinality") {
    val aCardinality = 5
    val rCardinality = 1000
    val arCardinality = 700

    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query =
      """
        |MATCH (a:A)
        |WITH a, 1 AS foo
        |MATCH (a)-[r:R]->() WHERE id(r) = 5
        |""".stripMargin

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipByIdSeek(LogicalVariable("r"), _, _, _, _) => true
      },
      aCardinality
    )
    // The whole query checks that r's start node is a
    queryShouldHaveCardinality(config, query, arCardinality * 1.0 / rCardinality)
  }

  test("relationship by id seek with equal start and end node should correctly calculate cardinality") {
    val aCardinality = 5
    val rCardinality = 1000
    val arCardinality = 700

    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query = "MATCH (a)-[r:R]->(a) WHERE id(r) = 5"

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipByIdSeek(LogicalVariable("r"), _, _, _, _) => true
      },
      1
    )
  }

  test("text index predicate with an empty string argument") {
    val aNodeCount = 500
    val textIndexSelectivity = 0.1

    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodeCount)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = textIndexSelectivity,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .build()

    for (op <- Seq("STARTS WITH", "ENDS WITH", "CONTAINS")) withClue(op) {
      val q = s"MATCH (a:A) WHERE a.prop $op '' "
      queryShouldHaveCardinality(cfg, q, aNodeCount * textIndexSelectivity)
    }
  }

  test("should use distance seekable predicate for cardinality estimation") {
    val labelCardinality = 50
    val propSelectivity = 0.5
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", labelCardinality)
      .addNodeIndex("Person", Seq("prop"), propSelectivity, 1, indexType = IndexType.POINT)
      .build()

    val query = "MATCH (n:Person) WHERE point.distance(n.prop, point({x: 1.1, y: 5.4})) < 0.5"

    val planState = config.planState(query + " RETURN n")
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.effectiveCardinalities
    val nodeIndexSeekCardinality = plan.flatten(CancellationChecker.neverCancelled())
      .collectFirst { case lp: NodeIndexSeek => cardinalities.get(lp.id) }.get

    nodeIndexSeekCardinality.amount shouldEqual (labelCardinality * propSelectivity * DEFAULT_RANGE_SEEK_FACTOR)

    queryShouldHaveCardinality(config, query, labelCardinality * propSelectivity * DEFAULT_RANGE_SEEK_FACTOR)
  }

  test("Infer label of intermediate node with outgoing relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)<-[friendship:KNOWS]-(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer label of intermediate node with one incoming relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)-[friendship:KNOWS]->(friend)<-[membership:HAS_MEMBER]-(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer label of intermediate node with one incoming and one outgoing relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)-[friendship:KNOWS]->(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer most selective label of intermediate node") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val forumNodes: Double = 15
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Forum", forumNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Forum)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Forum)", 0)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Forum)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Forum)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)<-[friendship:KNOWS]-(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / forumNodes // since we can infer that friend:Forum
    )
  }

  test("Should not infer label of intermediate node that already has a label") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val forumNodes: Double = 15
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Forum", forumNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Forum)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Forum)", 0)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Forum)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Forum)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)<-[friendship:KNOWS]-(friend:Person)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / personNodes
    )
  }

  test("should infer label of intermediate varlength expand nodes, 3..3 length") {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val entityNodes: Double = 1000
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Entity", entityNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("(:Entity)-[:KNOWS]->()", 0)
      .setRelationshipCardinality("()-[:KNOWS]->(:Entity)", 0)
      .build()

    val queries = Seq(
      "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d)",
      "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)",
      "MATCH (a)-[:KNOWS*3..3]->(d)",
      "MATCH (a:Person)-[:KNOWS*3..3]->(d:Person)",
      "MATCH (a)-[:KNOWS]->{3,3}(b)",
      "MATCH (a:Person) ((:Person)-[:KNOWS]->(:Person)){3,3} (b:Person)"
    )

    val expectedCardinality = {
      val personKnowsPersonSel = knowsRelationships / (personNodes * personNodes)

      math.pow(personNodes, 4) *
        math.pow(personKnowsPersonSel, 3) *
        uniquenessSelectivityForNRels(3)
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test(
    "should infer label of intermediate varlength expand nodes when it can only infer the label on the target node, 3..3 length"
  ) {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships / 2)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .build()

    val queries = Seq(
      "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d)",
      "MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)",
      "MATCH (a)-[:KNOWS*3..3]->(d)",
      "MATCH (a)-[:KNOWS*3..3]->(d:Person)",
      "MATCH (a)-[:KNOWS]->{3,3}(b)",
      "MATCH (a) (()-[:KNOWS]->(:Person)){3,3} (b:Person)"
    )

    val expectedCardinality = {
      val personKnowsPersonSel = knowsRelationships / 2 / (personNodes * personNodes)

      val anyKnowsPersonSel = knowsRelationships / (allNodes * personNodes)

      allNodes * math.pow(personNodes, 3) *
        anyKnowsPersonSel * math.pow(personKnowsPersonSel, 2) *
        uniquenessSelectivityForNRels(3)
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test("should infer label of left boundary node of varlength expand nodes, 1..1 length") {
    val allNodes: Double = 3000
    val aNodes: Double = 500

    val r1Relationships: Double = 5000
    val r2Relationships: Double = 2000

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("A", aNodes)
      .setRelationshipCardinality("()-[:R1]->()", r1Relationships)
      .setRelationshipCardinality("(:A)-[:R1]->()", r1Relationships / 2)
      .setRelationshipCardinality("()-[:R1]->(:A)", r1Relationships / 3)
      .setRelationshipCardinality("()-[:R2]->()", r2Relationships)
      .setRelationshipCardinality("(:A)-[:R2]->()", r2Relationships)
      .setRelationshipCardinality("()-[:R2]->(:A)", r2Relationships / 2)
      .build()

    val queries = Seq(
      "MATCH ()-[:R1]->()-[:R2*1..1]->()"
    )

    val expectedCardinality = {
      val any_R1_A_Sel = (r1Relationships / 3) / (allNodes * aNodes)

      val a_R2_any_sel = r2Relationships / (aNodes * allNodes)

      allNodes * aNodes * allNodes *
        any_R1_A_Sel * a_R2_any_sel
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test("should infer label of intermediate varlength expand nodes, 2..3 length") {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val entityNodes: Double = 1000
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Entity", entityNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("(:Entity)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Entity)", knowsRelationships)
      .build()

    val queries = Seq(
      """CALL {
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN 1 AS one
        |}
        |""".stripMargin,
      """CALL {
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) RETURN 1 AS one
        |}
        |""".stripMargin,
      "MATCH (a)-[:KNOWS*2..3]->(d)",
      "MATCH (a:Person)-[:KNOWS*2..3]->(d:Person)",
      "MATCH (a)-[:KNOWS]->{2,3}(b)",
      "MATCH (a:Person) ((:Person)-[:KNOWS]->(:Person)){2,3} (b:Person)"
    )

    val expected = {
      val personKnowsPersonSel = knowsRelationships / (personNodes * personNodes)

      val cardinality2_2 =
        math.pow(personNodes, 3) *
          math.pow(personKnowsPersonSel, 2) *
          uniquenessSelectivityForNRels(2)

      val cardinality3_3 =
        math.pow(personNodes, 4) *
          math.pow(personKnowsPersonSel, 3) *
          uniquenessSelectivityForNRels(3)

      cardinality2_2 + cardinality3_3
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expected)
    }
  }

  test(
    "should infer label of intermediate varlength expand nodes when it can only infer the label on the target node, 2..3 length"
  ) {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships / 2)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .build()

    val queries = Seq(
      """CALL {
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN 1 AS one
        |}
        |""".stripMargin,
      """CALL {
        |  MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) RETURN 1 AS one
        |}
        |""".stripMargin,
      "MATCH (a)-[:KNOWS*2..3]->(d)",
      "MATCH (a)-[:KNOWS*2..3]->(d:Person)",
      "MATCH (a)-[:KNOWS]->{2,3}(b)",
      "MATCH (a) (()-[:KNOWS]->(:Person)){2,3} (b:Person)"
    )

    val expectedCardinality = {
      val personKnowsPersonSel = knowsRelationships / 2 / (personNodes * personNodes)

      val anyKnowsPersonSel = knowsRelationships / (allNodes * personNodes)

      val cardLength2 = allNodes * math.pow(personNodes, 2) *
        anyKnowsPersonSel * personKnowsPersonSel *
        uniquenessSelectivityForNRels(2)

      val cardLength3 = allNodes * math.pow(personNodes, 3) *
        anyKnowsPersonSel * math.pow(personKnowsPersonSel, 2) *
        uniquenessSelectivityForNRels(3)

      cardLength2 + cardLength3
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test(
    "should infer label of intermediate varlength expand nodes when it can only infer the label on the source node, 2..3 length"
  ) {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships / 2)
      .build()

    val queries = Seq(
      """CALL {
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN 1 AS one
        |}
        |""".stripMargin,
      """CALL {
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN 1 AS one
        |}
        |""".stripMargin,
      "MATCH (a)-[:KNOWS*2..3]->(d)",
      "MATCH (a:Person)-[:KNOWS*2..3]->(d)",
      "MATCH (a)-[:KNOWS]->{2,3}(b)",
      "MATCH (a:Person) ((:Person)-[:KNOWS]->()){2,3} (b)"
    )

    val expectedCardinality = {
      val personKnowsPersonSel = knowsRelationships / 2 / (personNodes * personNodes)

      val personKnowsAnySel = knowsRelationships / (allNodes * personNodes)

      val cardLength2 = math.pow(personNodes, 2) * allNodes *
        personKnowsPersonSel * personKnowsAnySel *
        uniquenessSelectivityForNRels(2)

      val cardLength3 = math.pow(personNodes, 3) * allNodes *
        math.pow(personKnowsPersonSel, 2) * personKnowsAnySel *
        uniquenessSelectivityForNRels(3)

      cardLength2 + cardLength3
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test("should infer label of intermediate varlength expand nodes with a narrower end label, 2..3 length") {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val lastNodes: Double = 200
    val lastRelationships: Double = 200
    val knowsRelationships: Double = 300

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Last", lastNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("(:Last)-[:KNOWS]->()", 0)
      .setRelationshipCardinality("()-[:KNOWS]->(:Last)", lastRelationships)
      .build()

    val queries = Seq(
      """CALL {
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c:Last) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d:Last) RETURN 1 AS one
        |}
        |""".stripMargin,
      """CALL {
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Last) RETURN 1 AS one
        |  UNION ALL
        |  MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Last) RETURN 1 AS one
        |}
        |""".stripMargin,
      "MATCH (a)-[:KNOWS*2..3]->(d:Last)",
      "MATCH (a:Person)-[:KNOWS*2..3]->(d:Last)",
      "MATCH (a)-[:KNOWS]->{2,3}(d:Last)",
      "MATCH (a:Person) ((:Person)-[:KNOWS]->()){2,3} (d:Last)"
    )

    val expected = {
      val personKnowsPersonSel = knowsRelationships / (personNodes * personNodes)
      val personKnowsLastSel = lastRelationships / (personNodes * lastNodes)

      val cardinality2_2 =
        personNodes * personNodes * lastNodes *
          personKnowsPersonSel * personKnowsLastSel *
          uniquenessSelectivityForNRels(2)

      val cardinality3_3 =
        personNodes * personNodes * personNodes * lastNodes *
          personKnowsPersonSel * personKnowsPersonSel * personKnowsLastSel *
          uniquenessSelectivityForNRels(3)

      cardinality2_2 + cardinality3_3
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expected)
    }
  }

  test("should infer label of intermediate nodes in a QPP containing multiple relationships, 2..2") {
    val allNodes: Double = 3000
    val personNodes: Double = 500
    val entityNodes: Double = 1000
    val allRels = 50000
    val knowsRelationships: Double = 300
    val allPersonToPersonRelationships: Double = knowsRelationships * 2

    val config = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .defaultRelationshipCardinalityTo0(enable = false)
      .setAllNodesCardinality(allNodes)
      .setAllRelationshipsCardinality(allRels)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Entity", entityNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Entity)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Entity)", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[]->(:Person)", allPersonToPersonRelationships)
      .setRelationshipCardinality("(:Person)-[]->()", allPersonToPersonRelationships)
      .setRelationshipCardinality("()-[]->(:Person)", allPersonToPersonRelationships)
      .build()

    val queries = Seq(
      """MATCH
        |  (a:Person)-[:KNOWS]->(:Person)-[]->(b:Person),
        |  (b:Person)-[:KNOWS]->(:Person)-[]->(c:Person)
        |""".stripMargin,
      """MATCH
        |  (a)-[:KNOWS]->()-[]->(b),
        |  (b)-[:KNOWS]->()-[]->(c)
        |""".stripMargin,
      "MATCH (a) (()-[:KNOWS]->()-[]->()){2,2} (c)",
      "MATCH (a) ((:Person)-[:KNOWS]->(:Person)-[]->(:Person)){2,2} (c)"
    )

    val expectedCardinality = {
      val personKnowsPersonSel = knowsRelationships / (personNodes * personNodes)
      val personToPersonSel = allPersonToPersonRelationships / (personNodes * personNodes)

      math.pow(personNodes, 5) *
        math.pow(personKnowsPersonSel, 2) *
        math.pow(personToPersonSel, 2) *
        uniquenessSelectivityForNRels(4)
    }

    for (query <- queries) withClue(query) {
      queryShouldHaveCardinality(config, query, expectedCardinality)
    }
  }

  test(
    "the pattern within the QPP should infer labels from the relationship statistics"
  ) {
    val n = 100
    val r = 50
    val a = 10
    val r1 = 40

    val builder = plannerBuilder()
      .setAllNodesCardinality(n)
      .setAllRelationshipsCardinality(r)
      .setLabelCardinality("A", a)
      .setRelationshipCardinality("()-[:R1]->()", r1)
      .setRelationshipCardinality("()-[:R1]->(:A)", r1)
      .setRelationshipCardinality("(:A)-[:R1]->()", r1)
      .setRelationshipCardinality("(:A)-[:R1]->(:A)", r1)
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )

    val query = "MATCH (nOuterLeft:A)(()-[:R1]->()-[:R1]->()){1}(nOuterRight:A)"
    planShouldHaveCardinality(
      builder.build(),
      query,
      {
        case Expand(_: Argument, _, _, _, _, _, _) => true
      },
      a * r1 / a.toDouble
    )
  }

  test(
    "the outer nodes of the QPP should infer labels from the relationship statistics"
  ) {
    val n = 100
    val r = 50
    val a = 60
    val r1 = 40

    val builder = plannerBuilder()
      .setAllNodesCardinality(n)
      .setAllRelationshipsCardinality(r)
      .setLabelCardinality("A", a)
      .setRelationshipCardinality("()-[:R1]->()", r1)
      .setRelationshipCardinality("()-[:R1]->(:A)", r1)
      .setRelationshipCardinality("(:A)-[:R1]->()", r1)
      .setRelationshipCardinality("(:A)-[:R1]->(:A)", r1)
      .setRelationshipCardinality("(:A)-[]->()", r1)
      .setRelationshipCardinality("()-[]->(:A)", r1)
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )

    val query = "MATCH ()-[:R1]->(nOuterLeft)((testNode)-[]->()-[]->(:A)){1}(nOuterRight)"

    planShouldHaveCardinality(
      builder.build(),
      query,
      {
        case Expand(_: Argument, _, _, _, _, _, _) => true
      },
      r1 * r1 / a.toDouble
    )
  }

  test("Label inference should infer label A on both start and end nodes of undirected relationships with type S") {
    val N = 100
    val R = 50
    val S = 44
    val A = 30
    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:S]->()", S)
      .setRelationshipCardinality("(:A)-[:S]->()", S)
      .setRelationshipCardinality("()-[:S]->(:A)", S)
      .setRelationshipCardinality("(:A)-[:S]->(:A)", S)
      .build()

    val query = "MATCH (n1)-[r1:S]-(n2)-[r2:S]-(n3)"

    // Each relationship with the type S comes from and goes to a node with the label A
    // This can be observed from the fact that the cardinality of ()-[:S]->() and (:A)-[:S]->(:A) is the same
    // When the node label A is not inferred, the cardinality will be estimated as: S*2 * (S*2 / N) = 44*2 * (44*2/100) = 77.44
    // When the node label A is inferred,     the cardinality will be estimates as: S*2 * (S*2 / A) = 44*2 * (44*2/30)  = 258.13
    planShouldHaveCardinality(
      builder,
      query,
      {
        case _: Expand => true
      },
      S * 2 * (S * 2 / A.toDouble)
      // Wrong would be not using (the inferred) node label: S*2 * (S*2 / N)
    )
  }

  test(
    "Label inference should not infer label A on any of the nodes of undirected relationships with type S, if not all sources of this type"
  ) {
    val N = 100
    val R = 50
    val S = 44
    val A = 30
    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:S]->()", S)
      .setRelationshipCardinality("(:A)-[:S]->()", S - 1)
      .setRelationshipCardinality("()-[:S]->(:A)", S)
      .setRelationshipCardinality("(:A)-[:S]->(:A)", S - 1)
      .build()

    val query = "MATCH (n1)-[r1:S]-(n2)-[r2:S]-(n3)"

    planShouldHaveCardinality(
      builder,
      query,
      {
        case _: Expand => true
      },
      S * 2 * (S * 2 / N.toDouble)
    )
  }

  test(
    "Label inference should infer label A on the destination node, since all types imply label A on its destination node"
  ) {
    val N = 100
    val R = 50

    val A = 5

    val R1 = 20
    val R2 = 7
    val R3 = 10
    val R4 = 8

    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:R1]->()", R1)
      // Every type R1 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R1]->(:A)", R1)
      // Not every type R1 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R1]->()", R1 - 1)
      .setRelationshipCardinality("()-[:R2]->()", R2)
      // Every type R2 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R2]->(:A)", R2)
      // Not every type R2 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R2]->()", R2 - 1)
      .setRelationshipCardinality("()-[:R3]->()", R3)
      // Every type R3 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R3]->(:A)", R3)
      // Not every type R3 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R3]->()", R3 - 1)
      .setRelationshipCardinality("()-[:R4]->()", R4)
      // Not every type R4 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R4]->(:A)", R4 - 1)
      // Not every type R4 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R4]->()", R4 - 2)
      .build()

    val query = "MATCH (n1)-[r1:R1|R2|R3]->(n2)-[r2:R4]->(n3)"

    // When label A is not inferred for n2, then cardinality estimate = |()-[:R1|R2|R3]->()|   * |(()-[:R4]->()   / ())|   = (R1+R2+R3) * (R4    /N.toDouble) = 37 * 8/100 * .. =  2.96
    // Otherwise,                                cardinality estimate = |()-[:R1|R2|R3]->(:A)| * |((:A)-[:R4]->() / (:A))| = (R1+R2+R3) * ((R4-2)/A.toDouble) = 37 * 6/5   * .. = 44.4
    queryShouldHaveCardinality(builder, query, (R1 + R2 + R3) * ((R4 - 2) / A.toDouble))
  }

  test(
    "Label inference should infer label A on the source node, since all types imply label A on its source node"
  ) {
    val N = 100
    val R = 50

    val A = 5

    val R1 = 20
    val R2 = 7
    val R3 = 10
    val R4 = 8

    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:R1]->()", R1)
      // Not every type R1 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R1]->(:A)", R1 - 1)
      // Every type R1 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R1]->()", R1)
      .setRelationshipCardinality("()-[:R2]->()", R2)
      // Not every type R2 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R2]->(:A)", R2 - 1)
      // Every type R2 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R2]->()", R2)
      .setRelationshipCardinality("()-[:R3]->()", R3)
      // Not Every type R3 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R3]->(:A)", R3 - 1)
      // Every type R3 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R3]->()", R3)
      .setRelationshipCardinality("()-[:R4]->()", R4)
      // Not every type R4 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R4]->(:A)", R4 - 1)
      // Not every type R4 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R4]->()", R4 - 2)
      .build()

    val query = "MATCH (n1)-[r1:R4]->(n2)-[r2:R1|R2|R3]->(n3)"

    queryShouldHaveCardinality(builder, query, (R1 + R2 + R3) * ((R4 - 1) / A.toDouble))
  }

  test(
    "Label inference should not infer label A on the destination node, " +
      "since not all disjunctive types imply label A on its destination node " +
      "and R3 does not imply label A on its source node"
  ) {
    val N = 100
    val R = 50

    val A = 5

    val R1 = 20
    val R2 = 7
    val R3 = 10

    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:R1]->()", R1)
      // Every type R1 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R1]->(:A)", R1)
      // Every type R1 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R1]->()", R1)
      .setRelationshipCardinality("(:A)-[:R1]->(:A)", R1)
      .setRelationshipCardinality("()-[:R2]->()", R2)
      // Not every type R2 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R2]->(:A)", R2 - 1)
      // Not every type R2 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R2]->()", R2 - 1)
      .setRelationshipCardinality("()-[:R3]->()", R3)
      // Every type R3 is associated to a destination node with label A
      .setRelationshipCardinality("()-[:R3]->(:A)", R3)
      // Not every type R3 is associated to a source node with label A
      .setRelationshipCardinality("(:A)-[:R3]->()", R3 - 1)
      .build()

    val query = "MATCH (n1)-[r1:R1|R2]->(n2)-[r2:R3]->(n3)"

    queryShouldHaveCardinality(builder, query, (R1 + R2) * (R3 / N.toDouble))
  }

  test(
    "Should infer label on the middle nodes of a varlength relationship but not on the end nodes that already specifies a label"
  ) {
    val N: Double = 100
    val R: Double = 50

    val A: Double = 5
    val B: Double = 2

    val R1: Double = 45

    // Can infer label A on the source of R
    val A_R1_any: Double = R1
    val any_R1_A: Double = R1 - 15
    val A_R1_A: Double = any_R1_A

    val B_R1_any: Double = R1 - 10
    val any_R1_B: Double = R1 - 12
    val B_R1_B: Double = R1 - 15

    val A_R1_B: Double = any_R1_B
    val B_R1_A: Double = R1 - 20

    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(N)
      .setLabelCardinality("A", A)
      .setLabelCardinality("B", B)
      .setAllRelationshipsCardinality(R)
      .setRelationshipCardinality("()-[:R1]->()", R1)
      .setRelationshipCardinality("()-[:R1]->(:A)", any_R1_A)
      .setRelationshipCardinality("(:A)-[:R1]->()", A_R1_any)
      .setRelationshipCardinality("(:A)-[:R1]->(:A)", A_R1_A)
      .setRelationshipCardinality("(:B)-[:R1]->()", B_R1_any)
      .setRelationshipCardinality("()-[:R1]->(:B)", any_R1_B)
      .setRelationshipCardinality("(:B)-[:R1]->(:B)", B_R1_B)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", A_R1_B)
      .setRelationshipCardinality("(:B)-[:R1]->(:A)", B_R1_A)
      .build()

    val query = "MATCH (n1)<-[r1:R1*2..2]-(n2:B)-[r2:R1]->(n3)"

    // Label A can be inferred on the sources of relationships with the type R1
    // After unrolling and label inference
    // ()<-[:R1]-(:A)<-[:R1]-(:B)-[:R1]->()
    val nodesCardinality = N * A * B * N
    val A_R1_any_sel = A_R1_any / (A * N)
    val B_R1_A_sel = B_R1_A / (B * A)
    val B_R1_any_sel = B_R1_any / (B * N)
    val expectedCardinality =
      nodesCardinality * A_R1_any_sel * B_R1_A_sel * B_R1_any_sel * uniquenessSelectivityForNRels(
        2
      ) * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY.negate.factor

    // Note that the "PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY.negate.factor" comes from the fact that none of the matched relationships for r1 should be the relationship matched to r2 (NoneOfRelationships(r1, r2))
    // In the future the calculation for NoneOfRelationships(r1, r2) needs to be changed, since it is way too selective.

    queryShouldHaveCardinality(builder, query, expectedCardinality)
  }

  test("should infer labels on the intermediate nodes of a varlength relationships - support case") {
    val nodes: Double = 818547
    val relationships: Double = 76279528

    val A: Double = 518911
    val B: Double = 299297

    val R1: Double = 75582442
    val A_R1: Double = 75582442
    val B_R1: Double = 0
    val R1_A: Double = 0
    val R1_B: Double = 75582442

    val R2: Double = 232111
    val A_R2: Double = 232111
    val B_R2: Double = 0
    val R2_A: Double = 232111
    val R2_B: Double = 0

    // ()-[:R1]->() implies (:A)-[:R1]->(:B)
    // ()-[:R2]->() implies (:A)-[:R2]->(:A)

    val builder = plannerBuilder()
      .withSetting(
        GraphDatabaseSettings.cypher_infer_schema_parts_strategy,
        GraphDatabaseSettings.InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL
      )
      .setAllNodesCardinality(nodes)
      .setAllRelationshipsCardinality(relationships)
      .setLabelCardinality("A", A)
      .setLabelCardinality("B", B)
      // Relationship R1
      .setRelationshipCardinality("()-[:R1]->()", R1)
      .setRelationshipCardinality("(:A)-[:R1]->()", A_R1)
      .setRelationshipCardinality("()-[:R1]->(:A)", R1_A)
      .setRelationshipCardinality("(:B)-[:R1]->()", B_R1)
      .setRelationshipCardinality("()-[:R1]->(:B)", R1_B)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", R1)
      .setRelationshipCardinality("(:A)-[:R1]->(:A)", 0)
      .setRelationshipCardinality("(:B)-[:R1]->(:A)", 0)
      .setRelationshipCardinality("(:B)-[:R1]->(:B)", 0)
      // Relationship R2
      .setRelationshipCardinality("()-[:R2]->()", R2)
      .setRelationshipCardinality("(:A)-[:R2]->()", A_R2)
      .setRelationshipCardinality("()-[:R2]->(:A)", R2_A)
      .setRelationshipCardinality("(:B)-[:R2]->()", B_R2)
      .setRelationshipCardinality("()-[:R2]->(:B)", R2_B)
      .setRelationshipCardinality("(:A)-[:R2]->(:A)", R2)
      .setRelationshipCardinality("(:A)-[:R2]->(:B)", 0)
      .setRelationshipCardinality("(:B)-[:R2]->(:A)", 0)
      .setRelationshipCardinality("(:B)-[:R2]->(:B)", 0)
      .build()

    val query = "match p=(a:A)-[:R1|R2*0..4]->(b:B)"

    // 0 gives (:A&:B)
    // 1 gives (:A)-[R1|R2]->(:B)
    // 2 gives (:A)-[R1|R2]->()-[R1|R2]->(:B)
    // 3 gives (:A)-[R1|R2]->()-[R1|R2]->()-[R1|R2]->(:B)
    // 4 gives (:A)-[R1|R2]->()-[R1|R2]->()-[R1|R2]->()-[R1|R2]->(:B)

    // After label inference

    // 0 gives (:A&:B)
    // 1 gives (:A)-[R1|R2]->(:B)
    // 2 gives (:A)-[R1|R2]->(:A)-[R1|R2]->(:B)
    // 3 gives (:A)-[R1|R2]->(:A)-[R1|R2]->(:A)-[R1|R2]->(:B)
    // 4 gives (:A)-[R1|R2]->(:A)-[R1|R2]->(:A)-[R1|R2]->(:A)-[R1|R2]->(:B)

    // Stats indicate that ()-[R1]->(:A) is 0
    //                     ()-[:R2]->(:B) is 0
    // Therefore, it is equivalent to:

    // 0 gives (:A&B)
    // 1 gives (:A)-[R1]->(:B)
    // 2 gives (:A)-[:R2]->(:A)-[R1]->(:B)
    // 3 gives (:A)-[:R2]->(:A)-[:R2]->(:A)-[R1]->(:B)
    // 4 gives (:A)-[:R2]->(:A)-[:R2]->(:A)-[:R2]->(:A)-[R1]->(:B)

    val A_R1_B_sel = R1 / (A * B)
    val A_R2_A_sel = R2 / (A * A)

    val card_0 = A * B / nodes
    val card_1 = A * B * A_R1_B_sel
    val card_2 = Math.pow(A, 2) * B * A_R2_A_sel * A_R1_B_sel * uniquenessSelectivityForNRels(2)
    val card_3 = Math.pow(A, 3) * B * Math.pow(A_R2_A_sel, 2) * A_R1_B_sel * uniquenessSelectivityForNRels(3)
    val card_4 = Math.pow(A, 4) * B * Math.pow(A_R2_A_sel, 3) * A_R1_B_sel * uniquenessSelectivityForNRels(4)

    val card = card_0 + card_1 + card_2 + card_3 + card_4
    queryShouldHaveCardinality(builder, query, card)
  }

  test("subtractionNodeByLabelScan should obtain cardinality estimates using independence assumptions") {
    val nodes = 200.0
    val a = 50.0
    val b = 20.0
    val builder = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("A", a)
      .setLabelCardinality("B", b)
      .build()

    val expectedCardinality = nodes * (a / nodes) * ((nodes - b) / nodes)

    val query = "MATCH (n:A&!B)"

    planShouldHaveCardinality(
      builder,
      query,
      {
        case _: SubtractionNodeByLabelsScan => true
      },
      expectedCardinality
    )
  }

  test("Pass label info through horizons") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 70)
      .setRelationshipCardinality("()-[]->()", 1000)
      .setRelationshipCardinality("()-[:R]->()", 800)
      .setRelationshipCardinality("(:A)-[:R]->()", 500)
      .build()

    val query = "MATCH (a:A) WITH a as b WITH b as c MATCH (c)-[:R]->(d)"
    queryShouldHaveCardinality(builder, query, 500)
  }

  private def uniquenessSelectivityForNRels(n: Int): Double = {
    RepetitionCardinalityModel.relationshipUniquenessSelectivity(
      differentRelationships = 0,
      uniqueRelationships = 1,
      repetitions = n
    ).factor
  }
}
