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

import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.DeriveEagerAnalyzerOption.unsupportedClauseForIREagerness
import org.neo4j.cypher.internal.compiler.planner.logical.UnsupportedMergeClause.UnsupportedForEachClause
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption.ir
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption.lp
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.exceptions.InvalidCypherOption

case object DeriveEagerAnalyzerOption extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState]
    with StepSequencer.Step
    with PlanPipelineTransformerFactory {
  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  /**
   * Since IR eagerness analysis is being slowly removed, it is not being supported for features that are being developed after 5.22.
   * This phase adds the eagerAnalyzer option to the logical plan state with the following conditions:
   * a. translate ir_from_config to ir if the query is not using dynamic labels and properties are not being set or removed.
   * b. translate ir_from_config to lp if the query is using dynamic labels/properties.
   * c. throw an invalid cypher option if the query passes eagerAnalyzer=ir (plannerContext.eagerAnalyzer should be ir in this case) and dynamic labels/properties are being used.
   * d. copy the eagerAnalyzer from the plannerContext to the logicalPlanState
   */
  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    from.copy(maybeEagerAnalyzerOption =
      Some(derivedEagerAnalyzerOption(context.eagerAnalyzer, from.maybeSemanticTable))
    )
  }

  private def maybeUnsupportedFeatureForIREagerness(maybeSemanticTable: Option[SemanticTable])
    : Option[InvalidEagerReason] = {
    maybeSemanticTable.flatMap(_.recordedScopes.keys.collectFirst {
      case PositionedNode(UnsupportedSetClause(setClause))         => setClause
      case PositionedNode(UnsupportedRemoveClause(remove))         => remove
      case PositionedNode(UnsupportedMergeClause(mergeClause))     => mergeClause
      case PositionedNode(UnsupportedCreateClause(createClause))   => createClause
      case PositionedNode(UnsupportedMatchClause(expression))      => expression
      case PositionedNode(UnsupportedForEachClause(forEachClause)) => forEachClause
    })
  }

  private def derivedEagerAnalyzerOption(
    eagerAnalyzerOption: CypherEagerAnalyzerOption,
    maybeSemanticTable: Option[SemanticTable]
  ): CypherEagerAnalyzerOption = eagerAnalyzerOption match {
    case CypherEagerAnalyzerOption.`irFromConfig` =>
      maybeUnsupportedFeatureForIREagerness(maybeSemanticTable).map(_ => lp).getOrElse(ir)

    case CypherEagerAnalyzerOption.ir =>
      val unsupportedFeatures = maybeUnsupportedFeatureForIREagerness(maybeSemanticTable)
      if (unsupportedFeatures.nonEmpty)
        throw InvalidCypherOption.irEagerAnalyzerUnsupported(
          unsupportedFeatures.get.operation
        )
      else ir

    case CypherEagerAnalyzerOption.lp => lp
  }

  def unsupportedClauseForIREagerness(clause: ASTNode): Boolean =
    clause match {
      case UnsupportedSetClause(_)     => true
      case UnsupportedRemoveClause(_)  => true
      case UnsupportedMergeClause(_)   => true
      case UnsupportedCreateClause(_)  => true
      case UnsupportedMatchClause(_)   => true
      case UnsupportedForEachClause(_) => true
      case _                           => false
    }

  override def preConditions: Set[StepSequencer.Condition] = Set(CompilationContains[PlannerQuery]())

  override def postConditions: Set[StepSequencer.Condition] = Set(CompilationContains[CypherEagerAnalyzerOption]())

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}

object UnsupportedSetClause {

  def unapply(node: ASTNode): Option[InvalidEagerReason] =
    node match {
      case _ @SetClause(setItems) => setItems.collectFirst {
          case UnsupportedSetClause(invalidEagerReason) => invalidEagerReason
        }
      case _ @SetLabelItem(_, _, dynamicLabels, _) if dynamicLabels.nonEmpty =>
        Some(InvalidEagerReason("setting dynamic labels"))
      case _: SetDynamicPropertyItem => Some(InvalidEagerReason("setting dynamic properties"))
      case _                         => None
    }
}

case class InvalidEagerReason(operation: String)

object UnsupportedRemoveClause {

  def unapply(node: ASTNode): Option[InvalidEagerReason] =
    node match {
      case _ @Remove(removeItems) => removeItems.collectFirst {
          case UnsupportedRemoveClause(invalidEagerReason) => invalidEagerReason
        }
      case _ @RemoveLabelItem(_, _, dynamicLabels, _) if dynamicLabels.nonEmpty =>
        Some(InvalidEagerReason("removing dynamic labels"))
      case _: RemoveDynamicPropertyItem => Some(InvalidEagerReason("removing dynamic properties"))
      case _                            => None
    }
}

object UnsupportedMatchClause {

  def unapply(node: ASTNode): Option[InvalidEagerReason] = {
    node match {
      case _ @Match(_, _, _, _, maybeWhere) if maybeWhere.exists(_.expression.containsDynamicExpression) =>
        Some(InvalidEagerReason("Matching on dynamic Label or Types"))
      case _ => None
    }
  }
}

object UnsupportedCreateClause {

  def unapply(node: ASTNode): Option[InvalidEagerReason] = {
    node match {
      case _ @Create(pattern) if pattern.patternParts.exists(_.containsDynamicPattern) =>
        Some(InvalidEagerReason("Creating dynamic Label or Types"))
      case _ => None
    }
  }
}

object UnsupportedMergeClause {

  def unapply(node: ASTNode): Option[InvalidEagerReason] = {
    node match {
      case _ @Merge(pattern, mergeActions, maybeWhere)
        if maybeWhere.exists(
          _.expression.containsDynamicExpression
        ) || pattern.containsDynamicPattern || mergeActions.exists(unsupportedMergeAction) =>
        Some(InvalidEagerReason("Merge using dynamic Label or Types"))
      case _ =>
        None
    }
  }

  private def unsupportedMergeAction(action: MergeAction): Boolean = {
    action match {
      case OnCreate(createAction) => unsupportedClauseForIREagerness(createAction)
      case OnMatch(matchAction)   => unsupportedClauseForIREagerness(matchAction)
      case _                      => false
    }
  }

  object UnsupportedForEachClause {

    def unapply(node: ASTNode): Option[InvalidEagerReason] = {
      node match {
        case _ @Foreach(_, expression, updates)
          if expression.containsDynamicExpression || updates.exists(unsupportedClauseForIREagerness) =>
          Some(InvalidEagerReason("Foreach using dynamic Label or Types"))
        case _ =>
          None
      }
    }
  }
}
