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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.MergeNodePattern
import org.neo4j.cypher.internal.ir.MergeRelationshipPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QgWithLeafInfo
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.UpdateGraph.LeafPlansPredicatesResolver
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.Parser
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class UpdateGraphTest extends CypherFunSuite with AstConstructionTestSupport with TableDrivenPropertyChecks {
  implicit private val semanticTable: SemanticTable = SemanticTable()

  private val noLeafPlanProvider: LeafPlansPredicatesResolver = _ => LeafPlansPredicatesResolver.NoLeafPlansFound

  test("should not be empty after adding label to set") {
    val original = QueryGraph()
    val setLabel = SetLabelPattern(v"name", Seq.empty, Seq.empty)

    original.addMutatingPatterns(setLabel).containsUpdates should be(true)
  }

  test("overlap when reading all labels and creating a specific label") {
    // MATCH (a) CREATE (:L)
    val qg = QueryGraph(patternNodes = Set(v"a"))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading all labels and not setting any label") {
    // ... WITH a, labels(a) as myLabels SET a.prop=[]
    val qg = QueryGraph(
      argumentIds = Set(v"a"),
      selections =
        Selections(Set(Predicate(Set(v"a"), varFor("a")), Predicate(Set(v"a"), Labels(varFor("a"))(pos))))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(setPropertyIr("a", "prop", "[]")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("overlap when reading all labels and removing a label") {
    // ... WITH a, labels(a) as myLabels REMOVE a:Label
    val qg = QueryGraph(
      argumentIds = Set(v"a"),
      selections =
        Selections(Set(Predicate(Set(v"a"), varFor("a")), Predicate(Set(v"a"), Labels(varFor("a"))(pos))))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(removeLabelIr("a", "Label")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(
      LabelReadRemoveConflict(labelName("Label"))
    )
  }

  test("overlap when reading and creating the same label") {
    // MATCH (a:L) CREATE (b:L)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and creating different labels") {
    // MATCH (a:L1:L2) CREATE (b:L3)
    val selections =
      Selections.from(HasLabels(varFor("a"), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L3")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("no overlap when properties don't overlap and no label on read GQ, but a label on write QG") {
    // MATCH (a {foo: 42}) CREATE (b:L)
    val selections = Selections.from(In(
      Property(varFor("a"), PropertyKeyName("foo")(pos))(pos),
      ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos)
    )(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("Don't overlap when properties don't overlap but labels explicitly do for simple predicates") {
    // MATCH (a:L {foo: 42}) CREATE (b:L) assuming `a` is unstable
    val selections = Selections.from(Seq(
      In(
        Property(varFor("a"), PropertyKeyName("foo")(pos))(pos),
        ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos)
      )(pos),
      HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos)
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("Overlap when properties don't overlap but labels explicitly do for difficult predicates") {
    // MATCH (a:L) WHERE a.foo < 42 CREATE (b:L) assuming `a` is unstable
    val variable = varFor("a")
    val property = Property(variable, PropertyKeyName("foo")(pos))(pos)
    val lessThan = LessThan(property, SignedDecimalIntegerLiteral("42")(pos))(pos)
    val selections = Selections.from(Seq(
      AndedPropertyInequalities(variable, property, NonEmptyList(lessThan)),
      HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos)
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading all rel types and creating a specific type") {
    // MATCH (a)-[r]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and writing different rel types") {
    // MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship(
        v"r",
        (v"a", v"b"),
        SemanticDirection.OUTGOING,
        Seq(RelTypeName("T1")(pos)),
        SimplePatternLength
      ))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T2", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("overlap when reading and writing same rel types") {
    // MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T1]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship(
        v"r",
        (v"a", v"b"),
        SemanticDirection.OUTGOING,
        Seq(RelTypeName("T1")(pos)),
        SimplePatternLength
      ))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and writing same rel types but matching on rel property") {
    // MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections =
      Selections.from(In(
        Property(varFor("r"), PropertyKeyName("foo")(pos))(pos),
        ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos)
      )(pos))
    val qg = QueryGraph(
      patternRelationships =
        Set(PatternRelationship(
          v"r",
          (v"a", v"b"),
          SemanticDirection.OUTGOING,
          Seq(RelTypeName("T1")(pos)),
          SimplePatternLength
        )),
      selections = selections
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("overlap when reading and writing same property and rel type") {
    // MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections =
      Selections.from(In(
        Property(varFor("r"), PropertyKeyName("foo")(pos))(pos),
        ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos)
      )(pos))
    val qg = QueryGraph(
      patternRelationships =
        Set(PatternRelationship(
          v"r",
          (v"a", v"b"),
          SemanticDirection.OUTGOING,
          Seq(RelTypeName("T1")(pos)),
          SimplePatternLength
        )),
      selections = selections
    )
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        CreatePattern(
          List(
            CreateRelationship(
              v"r2",
              v"a",
              RelTypeName("T1")(pos),
              v"b",
              SemanticDirection.OUTGOING,
              Some(
                MapExpression(Seq(
                  (PropertyKeyName("foo")(pos), SignedDecimalIntegerLiteral("42")(pos))
                ))(pos)
              )
            )
          )
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading, deleting and merging") {
    // MATCH (a:L1:L2) DELETE a CREATE (b:L3)
    val selections =
      Selections.from(HasLabels(varFor("a"), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        DeleteExpression(varFor("a"), detachDelete = false),
        MergeNodePattern(
          CreateNode(v"b", Set(LabelName("L3")(pos), LabelName("L3")(pos)), Set.empty, None),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(
      EagernessReason.ReadDeleteConflict("a")
    )
  }

  test("overlap when reading and deleting with collections") {
    // WITH collect(a) as col DELETE col[0]
    val qg = QueryGraph(argumentIds = Set(v"col"))
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        DeleteExpression(varFor("col"), detachDelete = false)
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(
      EagernessReason.ReadDeleteConflict("col")
    )
  }

  test("overlap when reading and setting node properties") {
    val propName = "prop"
    val pKey = PropertyKeyName(propName)(pos)

    // noinspection NameBooleanParameters
    // MATCH (a {prop: 42}) ...
    val tests = Table[SetMutatingPattern, Any](
      "Set pattern" -> "Expected",

      // SET b_no_type.prop = 123
      SetPropertyPattern(v"b_no_type", pKey, literalInt(123)) -> ListSet(EagernessReason.Unknown),

      // SET b_no_type.prop = 123, ...
      SetPropertiesPattern(v"b_no_type", Seq(pKey -> literalInt(123))) -> ListSet(EagernessReason.Unknown),

      // SET r.prop = 123
      SetRelationshipPropertyPattern(v"r", pKey, literalInt(123)) -> ListSet(),

      // SET r.prop = 123, ...
      SetRelationshipPropertiesPattern(v"r", Seq(pKey -> literalInt(123))) -> ListSet(),

      // SET b = {prop: 123}
      SetNodePropertiesFromMapPattern(v"b", mapOfInt(propName -> 123), true) -> ListSet(EagernessReason.Unknown),

      // SET r = {prop: 123}
      SetRelationshipPropertiesFromMapPattern(v"r", mapOfInt(propName -> 123), true) -> ListSet(),

      // SET b_no_type = {prop: 123}
      SetPropertiesFromMapPattern(v"b_no_type", mapOfInt(propName -> 123), true) -> ListSet(
        EagernessReason.Unknown
      ),

      // SET b.prop = 123
      SetNodePropertyPattern(v"b", pKey, literalInt(123)) -> ListSet(EagernessReason.Unknown),

      // SET b.prop = 123, ...
      SetNodePropertiesPattern(v"b", Seq(pKey -> literalInt(123))) -> ListSet(EagernessReason.Unknown)
    )

    val selections = Selections.from(Seq(
      in(prop("a", propName), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)

    forAll(tests) {
      case (pattern, expected) =>
        QueryGraph(mutatingPatterns = IndexedSeq(pattern))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }

    // Test with SET in MERGE ON CREATE
    forAll(tests) {
      case (pattern, expected) =>
        val merge = MergeNodePattern(
          CreateNode(v"b", Set.empty, Set.empty, None),
          QueryGraph.empty,
          Seq(pattern),
          Seq.empty
        )
        QueryGraph(mutatingPatterns = IndexedSeq(merge))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }

    // Test with SET in MERGE ON MATCH
    forAll(tests) {
      case (pattern, expected) =>
        val merge = MergeNodePattern(
          CreateNode(v"b", Set.empty, Set.empty, None),
          QueryGraph.empty,
          Seq.empty,
          Seq(pattern)
        )
        QueryGraph(mutatingPatterns = IndexedSeq(merge))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }
  }

  test("overlap when reading and setting relationship properties") {
    val propName = "prop"
    val pKey = PropertyKeyName(propName)(pos)

    // noinspection NameBooleanParameters
    // MATCH ()-[q {prop: 42}]-() ...
    val tests = Table[SetMutatingPattern, Any](
      "Set pattern" -> "Expected",

      // SET r_no_type.prop = 123
      SetPropertyPattern(v"r_no_type", pKey, literalInt(123)) -> ListSet(EagernessReason.Unknown),

      // SET r_no_type.prop = 123, ...
      SetPropertiesPattern(v"r_no_type", Seq(pKey -> literalInt(123))) -> ListSet(EagernessReason.Unknown),

      // SET r.prop = 123
      SetRelationshipPropertyPattern(v"r", pKey, literalInt(123)) -> ListSet(EagernessReason.Unknown),

      // SET r.prop = 123, ...
      SetRelationshipPropertiesPattern(v"r", Seq(pKey -> literalInt(123))) -> ListSet(EagernessReason.Unknown),

      // SET b = {prop: 123}
      SetNodePropertiesFromMapPattern(v"b", mapOfInt(propName -> 123), true) -> ListSet(),

      // SET r = {prop: 123}
      SetRelationshipPropertiesFromMapPattern(v"r", mapOfInt(propName -> 123), true) -> ListSet(
        EagernessReason.Unknown
      ),

      // SET r_no_type = {prop: 123}
      SetPropertiesFromMapPattern(v"r_no_type", mapOfInt(propName -> 123), true) -> ListSet(
        EagernessReason.Unknown
      ),

      // SET b.prop = 123
      SetNodePropertyPattern(v"b", pKey, literalInt(123)) -> ListSet(),

      // SET b.prop = 123, ...
      SetNodePropertiesPattern(v"b", Seq(pKey -> literalInt(123))) -> ListSet()
    )

    val selections = Selections.from(Seq(
      in(prop("q", propName), listOfInt(42))
    ))
    val pr = PatternRelationship(v"q", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternNodes = Set(v"a", v"b"), patternRelationships = Set(pr), selections = selections)

    // Test with bare SET
    forAll(tests) {
      case (clause, expected) =>
        QueryGraph(mutatingPatterns = IndexedSeq(clause))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }

    // Test with SET in MERGE ON CREATE
    forAll(tests) {
      case (pattern, expected) =>
        val merge = MergeRelationshipPattern(
          Seq.empty,
          Seq(CreateRelationship(
            v"r",
            v"a",
            RelTypeName("R")(pos),
            v"b",
            SemanticDirection.OUTGOING,
            None
          )),
          QueryGraph.empty,
          Seq(pattern),
          Seq.empty
        )
        QueryGraph(mutatingPatterns = IndexedSeq(merge))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }

    // Test with SET in MERGE ON MATCH
    forAll(tests) {
      case (pattern, expected) =>
        val merge = MergeRelationshipPattern(
          Seq.empty,
          Seq(CreateRelationship(
            v"r",
            v"a",
            RelTypeName("R")(pos),
            v"b",
            SemanticDirection.OUTGOING,
            None
          )),
          QueryGraph.empty,
          Seq.empty,
          Seq(pattern)
        )
        QueryGraph(mutatingPatterns = IndexedSeq(merge))
          .overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe expected
    }
  }

  test("overlap when reading and merging on the same label and property") {
    // MATCH (a:Label {prop: 42}) MERGE (b:Label {prop: 123})
    val selections = Selections.from(Seq(
      hasLabels("a", "Label"),
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode(v"b", Set(labelName("Label")), Set.empty, Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading and merging on the same property, no label on MATCH") {
    // MATCH (a {prop: 42}) MERGE (b:Label {prop: 123})
    val selections = Selections.from(Seq(
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode(v"b", Set(labelName("Label")), Set.empty, Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and merging on the same property but different labels") {
    // MATCH (a:Label {prop: 42}) MERGE (b:OtherLabel {prop: 123})
    val selections = Selections.from(Seq(
      hasLabels("a", "Label"),
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode(v"b", Set(labelName("OtherLabel")), Set.empty, Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider) shouldBe empty
  }

  test("allQueryGraphs should include IRExpressions recursively") {
    val innerQg1 = QueryGraph(patternNodes = Set(v"a"))
    val innerQg2 = QueryGraph(patternNodes = Set(v"b"))
    val qg = QueryGraph(
      selections = Selections.from(Seq(
        ExistsIRExpression(RegularSinglePlannerQuery(innerQg1), v"", "")(pos, Some(Set.empty), Some(Set.empty)),
        ListIRExpression(RegularSinglePlannerQuery(innerQg2), v"", v"", "")(
          pos,
          Some(Set.empty),
          Some(Set.empty)
        )
      ))
    )

    qg.allQGsWithLeafInfo.map(_.queryGraph) should (contain(innerQg1) and contain(innerQg2))
  }

  test("allQueryGraphs in horizon should include IRExpressions recursively") {
    val innerQg1 = QueryGraph(patternNodes = Set(v"a"))
    val innerQg2 = QueryGraph(patternNodes = Set(v"b"))
    val horizon = RegularQueryProjection(
      Map(
        v"a" -> ExistsIRExpression(RegularSinglePlannerQuery(innerQg1), v"", "")(
          pos,
          Some(Set.empty),
          Some(Set.empty)
        ),
        v"b" -> ListIRExpression(RegularSinglePlannerQuery(innerQg2), v"", v"", "")(
          pos,
          Some(Set.empty),
          Some(Set.empty)
        )
      )
    )

    horizon.allQueryGraphs.map(_.queryGraph) should (contain(innerQg1) and contain(innerQg2))
  }

  private val `((a)-[r:R]->(b))+` =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(v"a", v"start"),
      rightBinding = NodeBinding(v"b", v"end"),
      patternRelationships = NonEmptyList(PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = SimplePatternLength
      )),
      selections = Selections.empty,
      repetition = Repetition(1, UpperBound.Unlimited),
      nodeVariableGroupings = Set(v"a", v"b").map(name => variableGrouping(name, name)),
      relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
    )

  // SPPs does not contain any unstable leaf nodes so there should never be any overlaps on creating a node without a relationship.
  test("Should only find node overlaps based on unstable leaf nodes") {
    // MATCH ANY SHORTEST ((start:!Label)((a)-[r:R]->(b))+(end:!Label)) CREATE q:Label

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(`((a)-[r:R]->(b))+`)),
        selections = Selections.from(List(
          unique(v"r")
        )),
        selector = SelectivePathPattern.Selector.Shortest(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addPredicates(
          not(hasLabels("end", "Label")),
          not(hasLabels("start", "Label"))
        )
        .addSelectivePathPattern(shortestPathPattern)

    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode(v"q", Set(labelName("Label")), Set.empty, None),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    val qgWithLeafInfo = QgWithLeafInfo(queryGraph, Set.empty, Set.empty, Some(StableIdentifier(v"start")), false)

    ug.overlaps(qgWithLeafInfo, noLeafPlanProvider) shouldBe ListSet()
  }

  test("throw exception doing IR eagerness analysis on SET with dynamic labels") {
    // MATCH (a:L) SET b:$($label)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(SetLabelPattern(
        variable = varFor("b"),
        labels = Seq(),
        dynamicLabels = Seq(Parser.parseExpression("$label"))
      ))
    )

    val exception = the[IllegalArgumentException] thrownBy
      ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider)

    exception.getMessage should equal("Dynamic node labels are not supported in IR eagerness analysis")
  }

  test("throw exception doing IR eagerness analysis on SET with static and dynamic labels") {
    // MATCH (a:L) SET b:L&$($label)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(SetLabelPattern(
        variable = varFor("b"),
        labels = Seq(labelName("L")),
        dynamicLabels = Seq(Parser.parseExpression("$label"))
      ))
    )

    val exception = the[IllegalArgumentException] thrownBy
      ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider)

    exception.getMessage should equal("Dynamic node labels are not supported in IR eagerness analysis")
  }

  test("throw exception doing IR eagerness analysis on REMOVE with dynamic labels") {
    // MATCH (a:L) REMOVE b:$($label)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(RemoveLabelPattern(
        variable = varFor("b"),
        labels = Seq(),
        dynamicLabels = Seq(Parser.parseExpression("$label"))
      ))
    )

    val exception = the[IllegalArgumentException] thrownBy
      ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider)

    exception.getMessage should equal("Dynamic node labels are not supported in IR eagerness analysis")
  }

  test("throw exception doing IR eagerness analysis on MERGE ON CREATE with dynamic labels") {
    // MATCH (a:L) MERGE b:M ON CREATE SET b:$($label)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          createNode = CreateNode(
            variable = v"b",
            labels = Set(labelName("M")),
            labelExpressions = Set(),
            properties = None
          ),
          matchGraph = QueryGraph.empty,
          onCreate = Seq(
            setLabel(
              node = "b",
              staticLabels = Seq(),
              dynamicLabelExpressions = Seq("$label")
            )
          ),
          onMatch = Seq()
        )
      )
    )

    val exception = the[IllegalArgumentException] thrownBy
      ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider)

    exception.getMessage should equal("Dynamic node labels are not supported in IR eagerness analysis")
  }

  test("throw exception doing IR eagerness analysis on MERGE ON MATCH with dynamic labels") {
    // MATCH (a:L) MERGE b:M ON MATCH SET b:$($label)
    val selections = Selections.from(HasLabels(varFor("a"), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set(v"a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          createNode = CreateNode(
            variable = v"b",
            labels = Set(labelName("M")),
            labelExpressions = Set(),
            properties = None
          ),
          matchGraph = QueryGraph.empty,
          onCreate = Seq(),
          onMatch = Seq(
            setLabel(
              node = "b",
              staticLabels = Seq(),
              dynamicLabelExpressions = Seq("$labels")
            )
          )
        )
      )
    )

    val exception = the[IllegalArgumentException] thrownBy
      ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg), noLeafPlanProvider)

    exception.getMessage should equal("Dynamic node labels are not supported in IR eagerness analysis")
  }

  private def createNode(name: String, labels: String*) =
    CreatePattern(List(CreateNode(varFor(name), labels.map(l => LabelName(l)(pos)).toSet, Set.empty, None)))

  private def createRelationship(name: String, start: String, relType: String, end: String) =
    CreatePattern(
      List(CreateRelationship(
        varFor(name),
        varFor(start),
        RelTypeName(relType)(pos),
        varFor(end),
        SemanticDirection.OUTGOING,
        None
      ))
    )

  private def setPropertyIr(
    entity: String,
    key: String,
    value: String
  ): org.neo4j.cypher.internal.ir.SetMutatingPattern =
    org.neo4j.cypher.internal.ir.SetPropertyPattern(
      Parser.parseExpression(entity),
      PropertyKeyName(key)(InputPosition.NONE),
      Parser.parseExpression(value)
    )

  private def removeLabelIr(node: String, labels: String*): org.neo4j.cypher.internal.ir.RemoveLabelPattern =
    org.neo4j.cypher.internal.ir.RemoveLabelPattern(
      varFor(node),
      labels.map(l => LabelName(l)(InputPosition.NONE)),
      Seq.empty
    )
}
