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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition

sealed trait QueryHorizon extends Foldable {

  def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable]

  def dependingExpressions: Iterable[Expression]

  def dependencies: Set[LogicalVariable] = dependingExpressions.folder.findAllByClass[LogicalVariable].toSet

  def readOnly = true

  def allHints: Set[Hint]
  def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon

  /**
   * @return whether this horizon is the final projection of a single top-level planner query.
   */
  def isProjectionInFinalPosition: Boolean =
    this match {
      case qp: QueryProjection => qp.position.isFinal
      case _                   => false
    }

  /**
   * If dependingExpressions is empty, or only contains variables, we can assume that it doesn't contain any reads
   * @return 'true' if this horizon might do database reads. 'false' otherwise.
   */
  def couldContainRead: Boolean = dependingExpressions.exists(!_.isInstanceOf[Variable]) || returnsNodesOrRelationships

  private def returnsNodesOrRelationships: Boolean = {
    this match {
      case qp: QueryProjection => qp.position.isFinal && qp.projections.values.exists(_.isInstanceOf[Variable])
      case _                   => false
    }
  }

  /**
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  protected def getAllQGsWithLeafInfo: Seq[QgWithLeafInfo] = {
    val filtered = dependingExpressions.filter(!_.isInstanceOf[Variable]).toSeq
    val iRExpressions: Seq[QgWithLeafInfo] = filtered.folder.findAllByClass[IRExpression].flatMap((e: IRExpression) =>
      e.query.allQGsWithLeafInfo
    )
    QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves(
      getQueryGraphFromDependingExpressions,
      isProjectionInFinalPosition
    ) +: iRExpressions
  }

  protected def getQueryGraphFromDependingExpressions: QueryGraph = {
    val dependencies = dependingExpressions
      .flatMap(_.dependencies)
      .toSet

    QueryGraph(
      argumentIds = dependencies,
      selections = Selections.from(dependingExpressions)
    )
  }

  lazy val allQueryGraphs: Seq[QgWithLeafInfo] = getAllQGsWithLeafInfo
}

final case class PassthroughAllHorizon() extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = Seq.empty

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

case class UnwindProjection(variable: LogicalVariable, exp: Expression) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(exp)

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

case class LoadCSVProjection(
  variable: LogicalVariable,
  url: Expression,
  format: CSVFormat,
  fieldTerminator: Option[StringLiteral]
) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(url)

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

case class CallSubqueryHorizon(
  callSubquery: PlannerQuery,
  correlated: Boolean,
  yielding: Boolean,
  inTransactionsParameters: Option[InTransactionsParameters],
  optional: Boolean,
  importedVariables: Set[LogicalVariable]
) extends QueryHorizon {

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = {
    val maybeReportAs = inTransactionsParameters.flatMap(_.reportParams.map(_.reportAs))
    coveredIds ++ callSubquery.returns ++ maybeReportAs.toSeq
  }

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override def readOnly: Boolean = callSubquery.readOnly

  override def allHints: Set[Hint] = callSubquery.allHints

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon =
    copy(callSubquery = callSubquery.withoutHints(hintsToIgnore))

  /**
   * We don't analyze the subquery but just assume that it's doing reads.
   */
  override def couldContainRead: Boolean = true

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = super.getAllQGsWithLeafInfo ++ callSubquery.allQGsWithLeafInfo
}

sealed abstract class QueryProjection extends QueryHorizon {
  def selections: Selections
  def projections: Map[LogicalVariable, Expression]
  def queryPagination: QueryPagination
  def keySet: Set[LogicalVariable]
  def position: QueryProjection.Position
  def withSelection(selections: Selections): QueryProjection
  def withAddedProjections(projections: Map[LogicalVariable, Expression]): QueryProjection
  def withPagination(queryPagination: QueryPagination): QueryProjection
  def markAsFinal: QueryProjection

  def localExposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable]
  def importedExposedSymbols: Set[LogicalVariable]
  def withImportedExposedSymbols(symbols: Set[LogicalVariable]): QueryProjection

  final override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] =
    localExposedSymbols(coveredIds) ++ importedExposedSymbols

  override def dependingExpressions: Iterable[Expression] = projections.view.values ++ selections.predicates.map(_.expr)

  def updatePagination(f: QueryPagination => QueryPagination): QueryProjection = withPagination(f(queryPagination))

  def addPredicates(predicates: Expression*): QueryProjection = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    withSelection(selections = selections ++ newSelections)
  }

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

object QueryProjection {

  /**
   * Position relative to the top-level (single) planner query.
   * Final if at the end of a single top-level query.
   * Intermediate if not at the end of a query, part of a union, or inside a sub-query.
   * Used for eagerness analysis.
   */
  sealed trait Position {

    /**
     * @return whether it is at the end of the top-level single planner query.
     */
    def isFinal: Boolean

    /**
     * @param other the other position to combine with.
     * @return Final if both positions are final.
     */
    def combine(other: Position): Position
  }

  object Position {

    case object Intermediate extends Position {
      override def isFinal: Boolean = false
      override def combine(other: Position): Position = Intermediate
    }

    /**
     * Signifies the last projection that will conclude the query.
     */
    case object Final extends Position {
      override def isFinal: Boolean = true
      override def combine(other: Position): Position = other
    }
  }

  def empty: RegularQueryProjection = RegularQueryProjection(importedExposedSymbols = Set.empty)

  def forVariables(variables: Set[LogicalVariable]): Seq[AliasedReturnItem] =
    variables.toIndexedSeq.map(variable =>
      AliasedReturnItem(variable, variable)(InputPosition.NONE)
    )
}

final case class RegularQueryProjection(
  projections: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  position: QueryProjection.Position = QueryProjection.Position.Intermediate,
  importedExposedSymbols: Set[LogicalVariable] = Set.empty
) extends QueryProjection {
  def keySet: Set[LogicalVariable] = projections.keySet

  def ++(other: RegularQueryProjection): RegularQueryProjection =
    RegularQueryProjection(
      projections = projections ++ other.projections,
      queryPagination = queryPagination ++ other.queryPagination,
      selections = selections ++ other.selections,
      position = position.combine(other.position),
      importedExposedSymbols = importedExposedSymbols ++ other.importedExposedSymbols
    )

  override def withAddedProjections(projections: Map[LogicalVariable, Expression]): RegularQueryProjection =
    copy(projections = this.projections ++ projections)

  def withPagination(queryPagination: QueryPagination): RegularQueryProjection =
    copy(queryPagination = queryPagination)

  override def localExposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = projections.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)

  /**
   * @return a copy of the projection marked as being at the end of a top-level single planner query.
   */
  override def markAsFinal: QueryProjection = copy(position = QueryProjection.Position.Final)

  override def withImportedExposedSymbols(symbols: Set[LogicalVariable]): QueryProjection =
    copy(importedExposedSymbols = symbols)
}

final case class AggregatingQueryProjection(
  groupingExpressions: Map[LogicalVariable, Expression] = Map.empty,
  aggregationExpressions: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  position: QueryProjection.Position = QueryProjection.Position.Intermediate,
  importedExposedSymbols: Set[LogicalVariable] = Set.empty
) extends QueryProjection {

  assert(
    !(groupingExpressions.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  override def projections: Map[LogicalVariable, Expression] = groupingExpressions ++ aggregationExpressions

  override def keySet: Set[LogicalVariable] = groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def dependingExpressions: Iterable[Expression] = super.dependingExpressions ++ aggregationExpressions.values

  override def withAddedProjections(groupingKeys: Map[LogicalVariable, Expression]): AggregatingQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): AggregatingQueryProjection =
    copy(queryPagination = queryPagination)

  override def localExposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] =
    groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)

  override def markAsFinal: QueryProjection = copy(position = QueryProjection.Position.Final)

  override def withImportedExposedSymbols(symbols: Set[LogicalVariable]): QueryProjection =
    copy(importedExposedSymbols = symbols)
}

final case class DistinctQueryProjection(
  groupingExpressions: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  position: QueryProjection.Position = QueryProjection.Position.Intermediate,
  importedExposedSymbols: Set[LogicalVariable] = Set.empty
) extends QueryProjection {

  def projections: Map[LogicalVariable, Expression] = groupingExpressions

  def keySet: Set[LogicalVariable] = groupingExpressions.keySet

  override def withAddedProjections(groupingKeys: Map[LogicalVariable, Expression]): DistinctQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): DistinctQueryProjection =
    copy(queryPagination = queryPagination)

  override def localExposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = groupingExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)

  override def markAsFinal: QueryProjection = copy(position = QueryProjection.Position.Final)

  override def withImportedExposedSymbols(symbols: Set[LogicalVariable]): QueryProjection =
    copy(importedExposedSymbols = symbols)
}

/**
 * Query fragment, part of a composite query.
 *
 * @param graphReference the graph on which to execute the query fragment
 * @param queryString the query to execute, serialised as a standalone Cypher query string
 * @param parameters query parameters used inside of the query fragment
 * @param importsAsParameters variables imported from the outer query inside of the query fragment are passed via additional parameters; mapping from the parameters to the original variables
 * @param columns the variables returned by the query fragment
 */
case class RunQueryAtProjection(
  graphReference: GraphReference,
  queryString: String,
  parameters: Set[Parameter],
  importsAsParameters: Map[Parameter, LogicalVariable],
  columns: Set[LogicalVariable],
  importedExposedSymbols: Set[LogicalVariable] = Set.empty
) extends QueryProjection {
  override def localExposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds ++ columns
  override def selections: Selections = Selections.empty
  override def projections: Map[LogicalVariable, Expression] = columns.view.map(column => column -> column).toMap
  override def queryPagination: QueryPagination = QueryPagination.empty
  override def keySet: Set[LogicalVariable] = columns

  override def position: QueryProjection.Position =
    QueryProjection.Position.Intermediate // No eagerness analysis for composite queries as it stands

  override def withSelection(selections: Selections): QueryProjection =
    throw new UnsupportedOperationException("Cannot modify the selections of a RunQueryAt projection")

  override def withAddedProjections(projections: Map[LogicalVariable, Expression]): QueryProjection =
    throw new UnsupportedOperationException("Cannot add projections to a RunQueryAt projection")

  override def withPagination(queryPagination: QueryPagination): QueryProjection =
    throw new UnsupportedOperationException("Cannot modify the pagination of a RunQueryAt projection")
  override def markAsFinal: QueryProjection = this // No eagerness analysis for composite queries as it stands

  override def withImportedExposedSymbols(symbols: Set[LogicalVariable]): QueryProjection =
    copy(importedExposedSymbols = symbols)
}

case class CommandProjection(clause: CommandClause) extends QueryHorizon {

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = {
    val columns = clause match {
      case t: CommandClause if t.yieldItems.nonEmpty =>
        t.yieldItems.map(_.aliasedVariable)
      case _ => clause.unfilteredColumns.columns.map(_.variable)
    }
    coveredIds ++ columns
  }

  override def dependingExpressions: Seq[Expression] = Seq()

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

abstract class AbstractProcedureCallProjection extends QueryHorizon {
  val call: ResolvedCall
}
