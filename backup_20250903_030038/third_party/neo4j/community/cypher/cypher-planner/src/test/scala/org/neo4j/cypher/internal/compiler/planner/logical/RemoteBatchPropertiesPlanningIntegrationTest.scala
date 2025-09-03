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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.lang.Boolean.TRUE

class RemoteBatchPropertiesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  // Graph counts based on a subset of LDBC SF 1
  final private val planner =
    plannerBuilder()
      .setDatabaseMode(DatabaseMode.SHARDED)
      .withSetting(GraphDatabaseInternalSettings.push_predicates_into_remote_batch_properties, TRUE)
      .setAllNodesCardinality(3181725)
      .setLabelCardinality("Person", 9892)
      .setLabelCardinality("Message", 3055774)
      .setLabelCardinality("Post", 1003605)
      .setLabelCardinality("Comment", 2052169)
      .setRelationshipCardinality("()-[:KNOWS]->()", 180623)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 180623)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 180623)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 180623)
      .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->()", 1003605)
      .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->()", 1003605)
      .setRelationshipCardinality("(:Post)-[:POST_HAS_CREATOR]->()", 1003605)
      .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Person)", 1003605)
      .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
      .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->()", 2052169)
      .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->()", 2052169)
      .setRelationshipCardinality("(:Comment)-[:COMMENT_HAS_CREATOR]->()", 2052169)
      .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
      .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
      .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 9892.0, isUnique = true)
      .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 1323.0)
      .addNodeIndex("Message", List("creationDate"), existsSelectivity = 1.0, uniqueSelectivity = 3033542.0 / 3055774.0)
      .build()

  test("should batch node properties") {
    val query =
      """MATCH (person:Person)
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should not batch node properties for write queries") {
    val query =
      """MATCH (person:Person)
        |CREATE ()
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .cacheProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .create(createNode("anon_0"))
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch relationship properties") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("knowsSince")
      .projection("cacheR[knows.creationDate] AS knowsSince")
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]")
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("should also batch properties used in filters, even if just once") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |  USING INDEX person:Person(firstName)
        |  WHERE person.firstName = friend.firstName AND knows.creationDate < $max_creation_date
        |RETURN person.lastName AS personLastName,
        |       friend.lastName AS friendLastName,
        |       knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    val expected = planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName", "knowsSince")
      .projection(
        "cacheN[person.lastName] AS personLastName",
        "cacheN[friend.lastName] AS friendLastName",
        "cacheR[knows.creationDate] AS knowsSince"
      )
      .remoteBatchProperties("cacheNFromStore[person.lastName]", "cacheNFromStore[friend.lastName]")
      .filter("cacheN[person.firstName] = cacheN[friend.firstName]", "friend:Person")
      .remoteBatchPropertiesWithFilter("cacheRFromStore[knows.creationDate]", "cacheNFromStore[friend.firstName]")(
        "cacheRFromStore[knows.creationDate] < $max_creation_date"
      )
      .expandAll("(person)-[knows:KNOWS]->(friend)")
      .nodeIndexOperator("person:Person(firstName)", getValue = Map("firstName" -> GetValue))
      .build()

    plan shouldEqual expected
  }

  test("should retrieve properties from indexes where applicable") {
    val query =
      """MATCH (person:Person {id:$Person})
        |RETURN person.id AS personId,
        |       person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName")
      .projection("cacheN[person.id] AS personId", "cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(pos)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties of renamed entities") {
    val query =
      """MATCH (person:Person)
        |  WHERE person.creationDate < $max_creation_date
        |WITH person AS earlyAdopter, person.creationDate AS earlyAdopterSince ORDER BY earlyAdopterSince LIMIT 10
        |MATCH (earlyAdopter)-[knows:KNOWS]->(friend:Person)
        |RETURN earlyAdopter.lastName AS personLastName,
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName")
      .projection(Map(
        "personLastName" -> CachedProperty(
          // notice how `originalEntity` and `entityVariable` differ here
          originalEntity = varFor("person"),
          entityVariable = varFor("earlyAdopter"),
          PropertyKeyName("lastName")(pos),
          NODE_TYPE
        )(pos),
        "friendLastName" -> CachedProperty(
          originalEntity = varFor("friend"),
          entityVariable = varFor("friend"),
          PropertyKeyName("lastName")(pos),
          NODE_TYPE
        )(pos)
      ))
      .remoteBatchProperties("cacheNFromStore[friend.lastName]")
      .filter("friend:Person")
      .expandAll("(earlyAdopter)-[knows:KNOWS]->(friend)")
      .projection("person AS earlyAdopter")
      .remoteBatchProperties("cacheNFromStore[person.lastName]")
      .top(10, "earlyAdopterSince ASC")
      .projection("cacheN[person.creationDate] AS earlyAdopterSince")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.creationDate]")(
        "cacheNFromStore[person.creationDate] < $max_creation_date"
      )
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("probably should but currently does not batch properties when returning entire entities") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN person, friend""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("person", "friend")
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  // Arguably we will want to relax this rule, and limit the number of round-trips to the shards when filters aren't very selective
  test("should batch properties wherever cardinality is the smallest") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person.id = friend.id
        |RETURN friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName", "personLastName")
      .projection(
        "cacheN[friend.id] AS personId",
        "cacheN[friend.firstName] AS personFirstName",
        "cacheN[friend.lastName] AS personLastName"
      )
      .remoteBatchProperties("cacheNFromStore[friend.firstName]", "cacheNFromStore[friend.lastName]")
      .filter("NOT cacheN[person.id] = cacheN[friend.id]") // This filter has a very small impact on cardinality (40.6238 -> 40.6197), we could probably batch all friend properties together
      .remoteBatchProperties("cacheNFromStore[friend.id]")
      .expand("(person)-[anon_0:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(pos)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties in complex enough queries (Query 9 in LDBC SF 1)") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person=friend
        |WITH DISTINCT friend
        |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
        |WHERE message.creationDate < $Date0
        |WITH friend, message
        |ORDER BY message.creationDate DESC, message.id ASC
        |LIMIT 20
        |RETURN message.id AS messageId,
        |       coalesce(message.content,message.imageFile) AS messageContent,
        |       message.creationDate AS messageCreationDate,
        |       friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults(
        "messageId",
        "messageContent",
        "messageCreationDate",
        "personId",
        "personFirstName",
        "personLastName"
      )
      .projection(
        "cacheN[message.id] AS messageId",
        "cacheN[friend.lastName] AS personLastName",
        "cacheN[friend.id] AS personId",
        "cacheN[message.creationDate] AS messageCreationDate",
        "coalesce(cacheN[message.content], cacheN[message.imageFile]) AS messageContent",
        "cacheN[friend.firstName] AS personFirstName"
      )
      .remoteBatchProperties(
        "cacheNFromStore[message.imageFile]",
        "cacheNFromStore[friend.lastName]",
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]",
        "cacheNFromStore[message.content]"
      )
      .top(20, "`message.creationDate` DESC", "`message.id` ASC")
      .projection("cacheN[message.creationDate] AS `message.creationDate`", "cacheN[message.id] AS `message.id`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[message.creationDate]", "cacheNFromStore[message.id]")(
        "cacheNFromStore[message.creationDate] < $Date0"
      )
      .expandAll("(friend)<-[anon_0:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
      .projection("friend AS friend")
      .filter("NOT person = friend")
      .bfsPruningVarExpand("(person)-[:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(pos)),
        unique = true
      )
      .build()
  }

  test("should batch node properties with predicates and merge multiple batchings") {
    val query =
      """MATCH (person:Person) WHERE person.creationDate > 0
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.creationDate]",
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[person.lastName]"
      )("cacheNFromStore[person.creationDate] > 0")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch node properties without adding predicates when all predicates has multiple dependencies") {
    val query =
      """MATCH (person:Person)-[:KNOWS]->(friend) WHERE person.creationDate > friend.creationDate
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .filter("cacheN[person.creationDate] > cacheN[friend.creationDate]")
      .remoteBatchProperties("cacheNFromStore[friend.creationDate]")
      .expandAll("(person)-[anon_0:KNOWS]->(friend)")
      .remoteBatchProperties(
        "cacheNFromStore[person.creationDate]",
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[person.lastName]"
      )
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch node properties only adding valid predicates") {
    val query =
      """MATCH (person:Person)-[:KNOWS]->(friend) WHERE person.creationDate > friend.creationDate AND person.creationDate CONTAINS "test"
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .filter("cacheN[person.creationDate] > cacheN[friend.creationDate]")
      .remoteBatchProperties("cacheNFromStore[friend.creationDate]")
      .expandAll("(person)-[anon_0:KNOWS]->(friend)")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.creationDate]",
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[person.lastName]"
      )("cacheNFromStore[person.creationDate] CONTAINS 'test'")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch node properties not adding invalid predicates") {
    val query =
      """MATCH (person:Person) WHERE person.creationDate = EXISTS{Match (person)-[:KNOWS]->(friend)}
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    val filterExpr = Equals(
      cachedNodeProp("person", "creationDate"),
      HasDegreeGreaterThan(
        varFor("person"),
        Some(relTypeName("KNOWS")),
        OUTGOING,
        SignedDecimalIntegerLiteral("0")(pos)
      )(pos)
    )(pos)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .filterExpression(filterExpr)
      .remoteBatchProperties("cacheNFromStore[person.creationDate]")
      .nodeByLabelScan("person", "Person")
      .build()

  }

  test("Should not add expressions referencing multiple variables") {
    val query =
      """MATCH (person:Person), (friend)
        |WITH friend.creationDate AS friendDate
        |MATCH (person)-[:KNOWS]->(friend) WHERE person.creationDate > friendDate
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .filter("cacheN[person.creationDate] > friendDate")
      .remoteBatchProperties("cacheNFromStore[person.creationDate]")
      .apply()
      .|.relationshipTypeScan("(person)-[anon_0:KNOWS]->(friend)", "friendDate")
      .projection("cacheN[friend.creationDate] AS friendDate")
      .cartesianProduct()
      .|.nodeByLabelScan("person", "Person")
      .remoteBatchProperties("cacheNFromStore[friend.creationDate]")
      .allNodeScan("friend")
      .build()
  }

  test("Should not push down predicate that is not being cached in RemoteBatchProperties") {
    val query =
      """MATCH (person:Person), (friend)
        |WITH friend.creationDate AS friendDate
        |MATCH (person)-[:KNOWS]->(friend) WHERE person.creationDate > 0 AND friendDate > 1
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .filter("friendDate > 1")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.creationDate]"
      )(
        "cacheNFromStore[person.creationDate] > 0"
      )
      .apply()
      .|.relationshipTypeScan("(person)-[anon_0:KNOWS]->(friend)", "friendDate")
      .projection("cacheN[friend.creationDate] AS friendDate")
      .cartesianProduct()
      .|.nodeByLabelScan("person", "Person")
      .remoteBatchProperties("cacheNFromStore[friend.creationDate]")
      .allNodeScan("friend")
      .build()
  }

  test("should plan an index seek with property lookup on the right-hand side of an apply") {
    /*
     * The call sub-query is only here to enforce that person gets solved first preventing the test from being flaky.
     * The query is strictly equivalent to:
     *
     *   MATCH (person:Person)
     *   MATCH (friend:Person { id: person.id })
     *   RETURN person.id, friend.id
     */
    val query =
      """MATCH (person:Person)
        |CALL {
        |  WITH person
        |  MATCH (friend:Person { id: person.id })
        |  RETURN friend
        |}
        |RETURN person.id, friend.id""".stripMargin

    val plan = planner.plan(query)

    /*
     * Ideally we would retrieve person.id in batches on the left-hand side of the apply, as follows:
     *
     *   .produceResults("`person.id`", "`friend.id`")
     *   .projection("cacheN[person.id] AS `person.id`", "cacheN[friend.id] AS `friend.id`")
     *   .apply()
     *   .|.nodeIndexOperator("friend:Person(id = cacheN[person.id])", argumentIds = Set("person"), getValue = Map("id" -> GetValue), unique = true)
     *   .remoteBatchProperties("cacheNFromStore[person.id]")
     *   .nodeByLabelScan("person", "Person")
     *   .build()
     *
     * Unfortunately, PushdownPropertyReads doesn't support it as it stands, so we retrieve person.id on the right-hand
     * side for each person:
     */
    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.id`", "`friend.id`")
      .projection("cacheN[person.id] AS `person.id`", "cacheN[friend.id] AS `friend.id`")
      .apply()
      // UNIQUE friend:Person(id) WHERE id = cache[person.id], cache[friend.id]
      .|.nodeIndexOperator(
        "friend:Person(id)",
        getValue = Map("id" -> GetValue),
        argumentIds = Set("person"),
        unique = true,
        customQueryExpression = Some(SingleQueryExpression(cachedNodePropFromStore("person", "id")))
      )
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch properties for aggregating function") {
    val query =
      """MATCH (person)
        |WITH MAX(person.age) as maxAge
        |MATCH (person) WHERE person.age = maxAge
        |RETURN person.name""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.name`")
      .projection("cacheN[person.name] AS `person.name`")
      .remoteBatchProperties("cacheNFromStore[person.name]")
      .filter("cacheN[person.age] = maxAge")
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .apply()
      .|.allNodeScan("person", "maxAge")
      .aggregation(Seq(), Seq("MAX(cacheN[person.age]) AS maxAge"))
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }

  test("should batch properties for ordered aggregations") {
    val query =
      """MATCH (person)
        |WITH person ORDER BY person.age
        |RETURN person.name, person.age, count(person.age)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.name`", "`person.age`", "`count(person.age)`")
      .orderedAggregation(
        Seq("cacheN[person.name] AS `person.name`", "cacheN[person.age] AS `person.age`"),
        Seq("count(cacheN[person.age]) AS `count(person.age)`"),
        Seq("cacheN[person.age]")
      )
      .remoteBatchProperties("cacheNFromStore[person.name]")
      .sort("`person.age` ASC")
      .projection("cacheN[person.age] AS `person.age`")
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }

  test("should propagate cached properties for an unwind projection") {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName = 'foo'
        |UNWIND [n.firstName] AS foo
        |RETURN foo""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .unwind("[cacheN[n.firstName]] AS foo")
      .nodeIndexOperator("n:Person(firstName = 'foo')", _ => GetValue)
      .build())
  }

  test("should batch properties for rollup apply") {
    val query =
      """
        |MATCH (p:Person)
        |RETURN p.name, [(p)<-[r:COMMENT_HAS_CREATOR]-(message) | message.name] AS title
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`p.name`", "title")
      .projection("cacheN[p.name] AS `p.name`")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .rollUpApply("title", "anon_0")
      .|.projection("cacheN[message.name] AS anon_0")
      .|.remoteBatchProperties("cacheNFromStore[message.name]")
      .|.expandAll("(p)<-[r:COMMENT_HAS_CREATOR]-(message)")
      .|.argument("p")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should batch properties when finding triadic selections") {
    val query =
      """
        |MATCH (n:Person {firstName: "Dave"})-[:KNOWS]-(friend:Person)-[:KNOWS]-(friendOfFriend:Person)
        |WHERE NOT (n)-[:KNOWS]-(friendOfFriend)
        |RETURN n. firstName, n.lastName, friendOfFriend.firstName, friendOfFriend.lastName""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection(
        "cacheN[n.firstName] AS `n. firstName`",
        "cacheN[n.lastName] AS `n.lastName`",
        "cacheN[friendOfFriend.firstName] AS `friendOfFriend.firstName`",
        "cacheN[friendOfFriend.lastName] AS `friendOfFriend.lastName`"
      )
      .filter("NOT anon_1 = anon_0", "friendOfFriend:Person")
      .triadicSelection(positivePredicate = false, "n", "friend", "friendOfFriend")
      .|.remoteBatchProperties("cacheNFromStore[friendOfFriend.firstName]", "cacheNFromStore[friendOfFriend.lastName]")
      .|.expandAll("(friend)-[anon_1:KNOWS]-(friendOfFriend)")
      .|.argument("friend", "anon_0")
      .filter("friend:Person")
      .expandAll("(n)-[anon_0:KNOWS]-(friend)")
      .remoteBatchProperties("cacheNFromStore[n.lastName]")
      .nodeIndexOperator("n:Person(firstName = 'Dave')", getValue = Map("firstName" -> GetValue))
      .build())
  }

  test("should batch properties for anti semi apply and anti applies") {
    val query =
      """
        |MATCH (a:Person)
        |WHERE  NOT EXISTS { (:Post)-[:POST_HAS_CREATOR]->(n) } OR (a)-[:KNOWS]-(:Person{lastName:"Smith"}) OR a.age > 30
        |RETURN a.firstName, a.age""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[a.firstName] AS `a.firstName`", "cacheN[a.age] AS `a.age`")
      .remoteBatchProperties("cacheNFromStore[a.firstName]")
      .selectOrSemiApply("anon_4")
      .|.filter("anon_3:Person")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_3.lastName]")(
        "cacheNFromStore[anon_3.lastName] = 'Smith'"
      )
      .|.expandAll("(a)-[anon_2:KNOWS]-(anon_3)")
      .|.argument("a")
      .letSelectOrAntiSemiApply("anon_4", "cacheN[a.age] > 30")
      .|.expandAll("(anon_0)-[anon_1:POST_HAS_CREATOR]->(n)")
      .|.nodeByLabelScan("anon_0", "Post")
      .remoteBatchProperties("cacheNFromStore[a.age]")
      .nodeByLabelScan("a", "Person")
      .build())
  }

  test("should batch properties for anti-conditional apply and shortestpath") {
    val query =
      """
        |MATCH p=shortestPath((a:Person{lastName:"Smith"})-[:KNOWS*]-(b:Person{lastName:"Smith"}))
        |WHERE all(r IN relationships(p) WHERE r.creationDate < $max_creation_date)
        |AND all(n IN nodes(p) WHERE n.firstName = "John")
        |AND length(p) > 5
        |RETURN a.lastName, [n in nodes(p) | n.lastName] as lastNames, b.lastName
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(
        "cacheN[a.lastName] AS `a.lastName`",
        "[n IN nodes(p) | n.lastName] AS lastNames",
        "cacheN[b.lastName] AS `b.lastName`"
      )
      .antiConditionalApply("p")
      .|.top(1, "anon_1 ASC")
      .|.projection("length(p) AS anon_1")
      .|.filter("length(p) > 5")
      .|.projection(Map("p" -> varLengthPathExpression(v"a", v"anon_0", v"b")))
      .|.expand(
        "(a)-[anon_0:KNOWS*1..]-(b)",
        expandMode = ExpandInto,
        projectedDir = OUTGOING,
        nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.firstName] = 'John'")),
        relationshipPredicates =
          Seq(Predicate(
            "r",
            "cacheRFromStore[r.creationDate] < $max_creation_date"
          )) // This is a bug with the rewriter. We should not use cachedPropertiesFromStore from inline predicates, since it could slow down the operator significantly
      )
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath(
        "(a)-[anon_0:KNOWS*1..]-(b)",
        pathName = Some("p"),
        nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.firstName] = 'John'")),
        relationshipPredicates =
          Seq(Predicate(
            "r",
            "cacheRFromStore[r.creationDate] < $max_creation_date"
          )), // This is a bug with the rewriter. We should not use cachedPropertiesFromStore from inline predicates, since it could slow down the operator significantly
        pathPredicates = Seq("length(p) > 5"),
        withFallback = true
      )
      .|.argument("a", "b")
      .cartesianProduct()
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[b.lastName]")("cacheNFromStore[b.lastName] = 'Smith'")
      .|.nodeByLabelScan("b", "Person")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.lastName]")("cacheNFromStore[a.lastName] = 'Smith'")
      .nodeByLabelScan("a", "Person")
      .build())
  }
}
