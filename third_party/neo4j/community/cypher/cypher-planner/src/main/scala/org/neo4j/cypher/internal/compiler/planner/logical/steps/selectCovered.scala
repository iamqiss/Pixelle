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
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case object selectCovered extends SelectionCandidateGenerator {

  override def apply(
    input: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate] = {
    val unsolvedScalarPredicates =
      unsolvedPredicates.filterNot(_.folder(context.staticComponents.cancellationChecker).treeExists {
        case _: IRExpression => true
      })
    if (unsolvedScalarPredicates.isEmpty) {
      Iterator.empty
    } else {
      val RemoteBatchingResult(
        rewrittenExpressionsWithCachedProperties,
        planWithProperties
      ) =
        context.settings.remoteBatchPropertiesStrategy.planBatchPropertiesForSelections(
          queryGraph,
          input,
          context,
          unsolvedScalarPredicates
        )

      val plan = rewrittenExpressionsWithCachedProperties.selections match {
        case rewrittenSelections: Set[Expression] if rewrittenSelections.nonEmpty =>
          context.staticComponents.logicalPlanProducer.planSelectionWithSolvedPredicates(
            planWithProperties,
            rewrittenExpressionsWithCachedProperties.selections.toVector,
            unsolvedScalarPredicates.toVector,
            context
          )
        case _ => planWithProperties
      }

      Iterator(SelectionCandidate(plan, unsolvedScalarPredicates))
    }
  }
}
