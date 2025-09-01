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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

case object VarLengthQuantifierMerger extends PlannerQueryRewriter with StepSequencer.Step with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = topDown(
    Rewriter.lift {
      case RegularSinglePlannerQuery(queryGraph, interestingOrder, horizon, tail, queryInput) =>
        val (simpleRels, varLengthRels) = queryGraph.patternRelationships.partition(_.length.isSimple)

        val mergedRels = varLengthRels.groupBy(_.variable).values.flatMap { rels =>
          val newLength = rels.map(_.length).fold(VarPatternLength(0, None))(_ intersect _)
          rels.map(_.copy(length = newLength))
        }

        RegularSinglePlannerQuery(
          queryGraph.withPatternRelationships(simpleRels ++ mergedRels),
          interestingOrder,
          horizon,
          tail,
          queryInput
        )
    },
    cancellation = context.cancellationChecker
  )

  override def preConditions: Set[StepSequencer.Condition] =
    Set(CompilationContains[PlannerQuery]()) // = CreatePlannerQuery.postConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this
}
