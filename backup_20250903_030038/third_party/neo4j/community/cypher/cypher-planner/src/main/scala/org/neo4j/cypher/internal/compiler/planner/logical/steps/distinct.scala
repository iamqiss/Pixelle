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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.RemoteBatchingResult
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leverageOrder.OrderToLeverageWithAliases
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object distinct {

  /**
   * Given a so-far planned `plan` and a `distinctQueryProjection` to plan,
   * return a new plan on top of `plan` that also solves `distinctQueryProjection`.
   */
  def apply(
    plan: LogicalPlan,
    distinctQueryProjection: DistinctQueryProjection,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val solver = SubqueryExpressionSolver.solverFor(plan, context)
    val groupingExpressionsMap = distinctQueryProjection.groupingExpressions.map { case (k, v) =>
      (k, solver.solve(v, Some(k)))
    }
    val rewrittenPlan = solver.rewrittenPlan()

    val inputProvidedOrder = context.staticComponents.planningAttributes.providedOrders(plan.id)
    val OrderToLeverageWithAliases(orderToLeverageForGrouping, newGroupingExpressionsMap, _) =
      leverageOrder(inputProvidedOrder, groupingExpressionsMap, Map.empty, plan.availableSymbols)
    val RemoteBatchingResult(
      rewrittenExpressionsWithCachedProperties,
      planWithAllProperties
    ) = context.settings.remoteBatchPropertiesStrategy.planBatchPropertiesForGroupingExpressions(
      rewrittenPlan,
      context,
      groupingExpressionsMap = newGroupingExpressionsMap,
      orderToLeverage = orderToLeverageForGrouping
    )

    val previousDistinctness = rewrittenPlan.distinctness

    // If the already distinct columns cover the columns to distinctify,
    // then we do not need to plan a Distinct.
    if (previousDistinctness.covers(newGroupingExpressionsMap.values)) {
      val projections =
        projection.filterOutEmptyProjections(
          rewrittenExpressionsWithCachedProperties.groupExpressions,
          rewrittenPlan.availableSymbols
        )

      if (projections.isEmpty) {
        context.staticComponents.logicalPlanProducer.planEmptyDistinct(
          planWithAllProperties,
          distinctQueryProjection.groupingExpressions,
          context
        )
      } else {
        context.staticComponents.logicalPlanProducer.planProjectionForDistinct(
          planWithAllProperties,
          projections,
          distinctQueryProjection.groupingExpressions,
          context
        )
      }
    } else if (
      rewrittenExpressionsWithCachedProperties.orderToLeverage.isEmpty || !context.settings.executionModel.providedOrderPreserving
    ) {
      // Parallel runtime does currently not support OrderedDistinct
      context.staticComponents.logicalPlanProducer.planDistinct(
        planWithAllProperties,
        rewrittenExpressionsWithCachedProperties.groupExpressions,
        distinctQueryProjection.groupingExpressions,
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planOrderedDistinct(
        planWithAllProperties,
        rewrittenExpressionsWithCachedProperties.groupExpressions,
        rewrittenExpressionsWithCachedProperties.orderToLeverage,
        distinctQueryProjection.groupingExpressions,
        context
      )
    }
  }
}
