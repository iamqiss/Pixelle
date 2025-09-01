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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlanEventHorizonTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should do projection if necessary") {
    // Given
    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val literal = literalInt(42)
      val pq = RegularSinglePlannerQuery(horizon = RegularQueryProjection(Map(v"a" -> literal)))
      val inputPlan = Argument()

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(Projection(inputPlan, Map(v"a" -> literal)))
    }
  }

  test("should plan procedure calls") {
    // Given
    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val ns = Namespace(List("my", "proc"))(pos)
      val name = ProcedureName("foo")(pos)
      val qualifiedName = QualifiedName(ns.parts, name.name)
      val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
      val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
      val signature =
        ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
      val callResults = IndexedSeq(ProcedureResultItem(v"x")(pos), ProcedureResultItem(v"y")(pos))

      val call = ResolvedCall(signature, Seq.empty, callResults)(pos)
      val pq = RegularSinglePlannerQuery(horizon = ProcedureCallProjection(call))
      val inputPlan = Argument()

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, call))
    }
  }

  test("should plan subqueries calls") {
    // Given
    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val sq = RegularSinglePlannerQuery(
        QueryGraph(patternNodes = Set(v"a")),
        horizon = RegularQueryProjection(Map(v"a" -> v"a"))
      )

      val pq = RegularSinglePlannerQuery(horizon =
        CallSubqueryHorizon(
          sq,
          correlated = false,
          yielding = true,
          inTransactionsParameters = None,
          optional = false,
          importedVariables = Set.empty
        )
      )
      val inputPlan = Argument()

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(CartesianProduct(
        inputPlan,
        AllNodesScan(v"a", Set.empty)
      ))
    }
  }

  test("should plan correlated subqueries calls") {
    // Given
    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val sq = RegularSinglePlannerQuery(
        QueryGraph(patternNodes = Set(v"a")),
        horizon = RegularQueryProjection(Map(v"a" -> v"a"))
      )

      val pq = RegularSinglePlannerQuery(horizon =
        CallSubqueryHorizon(
          sq,
          correlated = true,
          yielding = true,
          inTransactionsParameters = None,
          optional = false,
          importedVariables = Set.empty
        )
      )
      val inputPlan = Argument()

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(Apply(
        inputPlan,
        AllNodesScan(v"a", Set.empty)
      ))
    }
  }

  test("should plan entire projection if there is no pre-projection") {
    // Given
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val literal = literalInt(42)
      val interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(v"a", Map(v"a" -> v"a")))
      val horizon =
        RegularQueryProjection(Map(v"a" -> v"a", v"b" -> literal, v"c" -> literal), QueryPagination())
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "a")
      context.staticComponents.planningAttributes.solveds.set(inputPlan.id, SinglePlannerQuery.empty)

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(Projection(
        Sort(inputPlan, Seq(Ascending(v"a"))),
        Map(v"b" -> literal, v"c" -> literal)
      ))
    }
  }

  test("should plan partial projection if there is a pre-projection for sorting") {
    // Given
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val literal = literalInt(42)
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"a", Map(v"a" -> literal)))
      val horizon = RegularQueryProjection(Map(v"a" -> literal, v"b" -> literal, v"c" -> literal), QueryPagination())
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes)
      context.staticComponents.planningAttributes.solveds.set(inputPlan.id, SinglePlannerQuery.empty)

      // When
      val producedPlan =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      producedPlan should equal(Projection(
        Sort(Projection(inputPlan, Map(v"a" -> literal)), Seq(Ascending(v"a"))),
        Map(v"b" -> literal, v"c" -> literal)
      ))
    }
  }

  test("should add the correct plans when query uses both ORDER BY, SKIP and LIMIT") {
    // given
    val x = literalUnsignedInt(110)
    val y = literalUnsignedInt(10)
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))
      val horizon = RegularQueryProjection(
        Map(v"x" -> v"x"),
        queryPagination = QueryPagination(skip = Some(y), limit = Some(x))
      )
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      val sorted = Sort(inputPlan, Seq(Ascending(v"x")))
      val limited = Limit(sorted, add(x, y))
      val skipped = Skip(limited, y)
      result should equal(skipped)
    }
  }

  test("should add sort without pre-projection for DistinctQueryProjection") {
    // [WITH DISTINCT n, m] WITH n AS n, m AS m, 5 AS notSortColumn ORDER BY m
    val mSortVar = v"m"
    val projectionsMap = Map[LogicalVariable, Expression](
      v"n" -> v"n",
      mSortVar -> mSortVar,
      // a projection that sort will not take care of
      v"notSortColumn" -> literalUnsignedInt(5)
    )
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"m"))
      val horizon = DistinctQueryProjection(groupingExpressions = projectionsMap)
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "m", "n")

      // When
      val result =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      val distinct =
        Distinct(inputPlan, groupingExpressions = projectionsMap)
      val sorted = Sort(distinct, Seq(Ascending(v"m")))
      result should equal(sorted)
    }
  }

  test("should add sort without pre-projection for AggregatingQueryProjection") {
    // [WITH n, m, o] // o is an aggregating expression
    // WITH o, n AS n, m AS m, 5 AS notSortColumn ORDER BY m, o
    val grouping = Map[LogicalVariable, Expression](
      v"n" -> v"n",
      v"m" -> v"m",
      // a projection that sort will not take care of
      v"notSortColumn" -> literalUnsignedInt(5)
    )
    val aggregating = Map[LogicalVariable, Expression](v"o" -> v"o")

    val projection = AggregatingQueryProjection(
      groupingExpressions = grouping,
      aggregationExpressions = aggregating,
      queryPagination = QueryPagination(skip = None, limit = None)
    )

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"m").asc(v"o"))
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = projection)
      val inputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "m", "n", "o")

      // When
      val result =
        PlanEventHorizon.planHorizonForPlan(pq, inputPlan, None, context, InterestingOrderConfig(pq.interestingOrder))

      // Then
      val aggregation = Aggregation(inputPlan, grouping, aggregating)
      val sorted = Sort(aggregation, Seq(Ascending(v"m"), Ascending(v"o")))
      result should equal(sorted)
    }
  }

  /**
   * Plan that claims to solve a projection `variable` AS `variable` and is sorted ASC by `variable`.
   */
  private def fakeSortedLogicalPlanFor(planningAttributes: PlanningAttributes, variable: String) = {
    // __sorted to disambiguate from unsorted plan.
    val result = fakeLogicalPlanFor(planningAttributes, variable, "__sorted")
    // Fake sort the plan
    planningAttributes.solveds.set(
      result.id,
      RegularSinglePlannerQuery(interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor(variable)))
      )
    )
    planningAttributes.providedOrders.set(result.id, DefaultProvidedOrderFactory.asc(varFor(variable)))
    result
  }

  test("planHorizon, only self required order, cheapest to maintain sorted plan") {
    // Given
    new givenConfig {
      cost = {
        // Cheaper to maintain sort.
        case (lp, _, _, _) if lp.availableSymbols.contains(v"__sorted") => 1.0
        case _                                                          => 10.0
      }
    }.withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))
      val horizon = RegularQueryProjection(Map(v"x" -> v"x"))
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)

      val bestInputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      val bestSortedInputPlan = fakeSortedLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result =
        PlanEventHorizon.planHorizon(pq, BestResults(bestInputPlan, Some(bestSortedInputPlan)), None, context)

      // Then
      result shouldBe BestResults(bestSortedInputPlan, None)
    }
  }

  test("planHorizon, only self required order, cheapest to sort") {
    // Given
    new givenConfig {
      cost = {
        // Cheaper to sort.
        case (lp, _, _, _) if lp.availableSymbols.contains(v"__sorted") => 10.0
        case _                                                          => 1.0
      }
    }.withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))
      val horizon = RegularQueryProjection(Map(v"x" -> v"x"))
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)

      val bestInputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      val bestSortedInputPlan = fakeSortedLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result =
        PlanEventHorizon.planHorizon(pq, BestResults(bestInputPlan, Some(bestSortedInputPlan)), None, context)

      // Then
      result shouldBe BestResults(Sort(bestInputPlan, Seq(Ascending(v"x"))), None)
    }
  }

  test("planHorizon, tail required order, cheapest to maintain sorted plan") {
    // Given
    new givenConfig {
      cost = {
        // Cheaper to maintain sort.
        case (lp, _, _, _) if lp.availableSymbols.contains(v"__sorted") => 1.0
        case _                                                          => 10.0
      }
    }.withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))
      val horizon = RegularQueryProjection(Map(v"x" -> v"x"))
      val pq = RegularSinglePlannerQuery(
        interestingOrder = InterestingOrder.empty,
        horizon = horizon,
        tail = Some(RegularSinglePlannerQuery(interestingOrder = interestingOrder))
      )

      val bestInputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      val bestSortedInputPlan = fakeSortedLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result =
        PlanEventHorizon.planHorizon(pq, BestResults(bestInputPlan, Some(bestSortedInputPlan)), None, context)

      // Then
      result shouldBe BestResults(bestInputPlan, Some(bestSortedInputPlan))
    }
  }

  test("planHorizon, tail required order, cheapest to sort") {
    // Given
    new givenConfig {
      cost = {
        // Cheaper to sort.
        case (lp, _, _, _) if lp.availableSymbols.contains(v"__sorted") => 10.0
        case _                                                          => 1.0
      }
    }.withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))
      val horizon = RegularQueryProjection(Map(v"x" -> v"x"))
      val pq = RegularSinglePlannerQuery(
        interestingOrder = InterestingOrder.empty,
        horizon = horizon,
        tail = Some(RegularSinglePlannerQuery(interestingOrder = interestingOrder))
      )

      val bestInputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      val bestSortedInputPlan = fakeSortedLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result =
        PlanEventHorizon.planHorizon(pq, BestResults(bestInputPlan, Some(bestSortedInputPlan)), None, context)

      // Then
      result shouldBe BestResults(bestInputPlan, Some(Sort(bestInputPlan, Seq(Ascending(v"x")))))
    }
  }

  test("planHorizon, no required order") {
    // Given
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val horizon = RegularQueryProjection(Map(v"x" -> v"x"))
      val pq = RegularSinglePlannerQuery(
        interestingOrder = InterestingOrder.empty,
        horizon = horizon,
        tail = Some(RegularSinglePlannerQuery(interestingOrder = InterestingOrder.empty))
      )

      val bestInputPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      // When
      val result = PlanEventHorizon.planHorizon(pq, BestResults(bestInputPlan, None), None, context)

      // Then
      result shouldBe BestResults(bestInputPlan, None)
    }
  }
}
