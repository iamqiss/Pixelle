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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentOrder
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Percentiles
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * If multiple percentile functions take the same input, rewrites them to [[Percentiles]].
 *
 * For example,
 * {{{
 *   percentileDisc(a,0.5) AS p1,
 *   percentileDisc(a,0.6) AS p2,
 *   percentileDisc(b,0.7) AS p3,
 *   percentileCont(b,0.7) AS p4,
 *   percentileDisc(c,0.8) AS p5,
 *   percentileDisc(DISTINCT c,0.8) AS p6,
 *   percentileCont(DISTINCT c,0.9) AS p7
 * }}}
 * Would become,
 * {{{
 *   percentiles(distinct=false, a,[0.5,0.6],['p1','p2'],[true,true]) AS map1,
 *   percentiles(distinct=false, b,[0.7,0.7],['p3','p4'],[true,false]) AS map2,
 *   percentileDisc(distinct=false, c,0.8) AS p5,
 *   percentiles(distinct=true, c,[0.8,0.9],['p6','p7'],[true,false]) AS map3,
 * }}}
 */
case class groupPercentileFunctions(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  attributes: Attributes[LogicalPlan]
) extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val pos: InputPosition = InputPosition.NONE

  override val innerRewriter: Rewriter = Rewriter.lift {
    case aggregation @ OrderedAggregation(_, _, aggregations: Map[LogicalVariable, Expression], _) =>
      val groupedFunctions = groupFunctions(aggregations)
      if (groupedFunctions.isEmpty) {
        aggregation
      } else {
        val (newAggregations, projectExpressions) = newExpressions(groupedFunctions, aggregations)
        val newAggregation = aggregation.copy(aggregationExpressions = newAggregations)(SameId(aggregation.id))
        val id = attributes.copy(aggregation.id).id()
        Projection(newAggregation, projectExpressions)(SameId(id))
      }
    case aggregation @ Aggregation(_, _, aggregations: Map[LogicalVariable, Expression]) =>
      val groupedFunctions = groupFunctions(aggregations)
      if (groupedFunctions.isEmpty) {
        aggregation
      } else {
        val (newAggregations, projectExpressions) = newExpressions(groupedFunctions, aggregations)
        val newAggregation = aggregation.copy(aggregationExpressions = newAggregations)(SameId(aggregation.id))
        val id = attributes.copy(aggregation.id).id()
        Projection(newAggregation, projectExpressions)(SameId(id))
      }
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  private def newExpressions(
    groupedPercentileFunctions: Map[(Expression, Boolean, ArgumentOrder), Map[LogicalVariable, FunctionInvocation]],
    aggregations: Map[LogicalVariable, Expression]
  ): (Map[LogicalVariable, Expression], Map[LogicalVariable, Property]) = {
    val functionVariableMappings: Map[FunctionInvocation, Seq[LogicalVariable]] =
      toPercentilesAndVariables(groupedPercentileFunctions)

    val (percentilesExpressions, variableMappings) =
      functionVariableMappings.foldLeft(
        (Map.empty[LogicalVariable, Expression], Map.empty[LogicalVariable, Expression])
      ) {
        case ((accAggregations, accMappings), (percentilesFun, variables)) =>
          val mapVar = varFor(anonymousVariableNameGenerator.nextName)
          (
            accAggregations + (mapVar -> percentilesFun),
            accMappings ++ variables.map(v => (v, mapVar))
          )
      }

    val aggregationsToRemove = groupedPercentileFunctions.flatMap { case (_, fs) => fs.keys }.toSet
    val newAggregations = (aggregations -- aggregationsToRemove) ++ percentilesExpressions

    val projectExpressions = variableMappings.map {
      case (projectTo: LogicalVariable, map: Expression) =>
        projectTo -> Property(map, PropertyKeyName(varToKey(projectTo))(pos))(pos)
    }
    (newAggregations, projectExpressions)
  }

  /**
   * Filters out all aggregation expressions other than [[PercentileDisc]] and returns those (if any) expressions
   * grouped on: input variable + distinctness + input order.
   *
   * For example, if 'a' is order ASC
   * {{{
   *   percentileDisc(a,0.5) AS p1,
   *   percentileDisc(a,0.6) AS p2,
   *   percentileDisc(b,0.7) AS p3
   *   percentileCont(b,0.7) AS p4
   *   percentileDisc(c,0.7) AS p5           <--- not returned because not part of any group
   *   percentileDisc(DISTINCT c,0.8) AS p6
   *   percentileCont(DISTINCT c,0.9) AS p7
   * }}}
   * Would become,
   * {{{
   *   Map(
   *      (a,false,ASC) -> Map(p1 -> 'percentileDisc(a,0.5)', p2 -> 'percentileDisc(a,0.6)'),
   *      (b,false,NONE) -> Map(p3 -> 'percentileDisc(b,0.7)', p4 -> 'percentileCont(b,0.7)'),
   *      (c,true,NONE)  -> Map(p6 -> 'percentileDisc(DISTINCT c,0.8)', p7 -> 'percentileCont(DISTINCT c,0.9)')
   *   )
   * }}}
   *
   * @return groups that contain at least two expressions.
   */
  private def groupFunctions(aggregationExpressions: Map[LogicalVariable, Expression])
    : Map[(Expression, Boolean, ArgumentOrder), Map[LogicalVariable, FunctionInvocation]] = {
    aggregationExpressions.collect {
      case (v, f @ FunctionInvocation(FunctionName(_, name), _, _, _, _))
        if name.equalsIgnoreCase(PercentileDisc.name) || name.equalsIgnoreCase(PercentileCont.name) => (v, f)
    }.groupBy { case (_, f: FunctionInvocation) => (f.args(0), f.distinct, f.order) }
      .filter { case (_, fs) => fs.size > 1 }
  }

  /**
   * Given a map of grouped percentile expressions, create a [[Percentiles]] expression for each group.
   *
   * For example,
   * {{{
   *   Map(
   *      (a,false) -> Map(p1 -> 'percentileDisc(a,0.5)', p2 -> 'percentileDisc(a,0.6)'),
   *      (b,false) -> Map(p3 -> 'percentileDisc(b,0.7)', p4 -> 'percentileCont(b,0.7)'),
   *      (c,true)  -> Map(p6 -> 'percentileDisc(DISTINCT c,0.8)', p7 -> 'percentileCont(DISTINCT c,0.9)')
   *   )
   * }}}
   * Would become,
   * {{{
   *   Map(
   *      percentile(distinct=false, a,[0.5,0.6],['p1','p2'],[true,true]) -> [p1,p2],
   *      percentile(distinct=false, b,[0.7,0.7],['p3','p4'],[true,false]) -> [p3,p4],
   *      percentile(distinct=true,  c,[0.8,0.9],['p6','p7'],[true,false]) -> [p6,p7]
   *   )
   * }}}
   *
   * @return mapping of percentiles functions to the variables which eventually need to be projected.
   */
  private def toPercentilesAndVariables(groupedFunctions: Map[
    (Expression, Boolean, ArgumentOrder),
    Map[LogicalVariable, FunctionInvocation]
  ]): Map[FunctionInvocation, Seq[LogicalVariable]] = {
    groupedFunctions.map { case ((input, distinct, order), percentileGroup: Map[LogicalVariable, FunctionInvocation]) =>
      val (variables, percentiles, isDiscretes) = toVariablePercentilePairs(percentileGroup)

      val percentilesLiteral = ListLiteral(percentiles)(pos)
      val propertyKeys = ListLiteral(variables.map(v => StringLiteral(varToKey(v))(pos.withInputLength(0))))(pos)
      val isDiscretesLiteral = ListLiteral(isDiscretes)(pos)
      val percentilesFunction = FunctionInvocation(
        FunctionName(Percentiles.name)(pos),
        distinct,
        IndexedSeq(input, percentilesLiteral, propertyKeys, isDiscretesLiteral),
        order
      )(pos)

      (percentilesFunction, variables)
    }
  }

  /**
   * For each entry in a map from variables to percentile functions,
   * this method will return a three-tuple of variable:percentile:isDiscrete
   *
   * For example,
   * {{{
   *   Map(p3 -> 'percentileDisc(b,0.7)', p4 -> 'percentileCont(b,0.7)')
   * }}}
   * Would become,
   * {{{
   *   ([p3,p4],[0.7,0.7],[true,false])
   * }}}
   *
   * @return variable:percentile pairs
   */
  private def toVariablePercentilePairs(percentileGroup: Map[LogicalVariable, FunctionInvocation])
    : (Seq[LogicalVariable], Seq[Expression], Seq[BooleanLiteral]) = {
    percentileGroup.foldLeft((Seq.empty[LogicalVariable], Seq.empty[Expression], Seq.empty[BooleanLiteral])) {
      case ((accVars, accPercentiles, accIsDiscretes), (v, FunctionInvocation(functionName, _, args, _, _))) =>
        val name = functionName.name
        val isDiscrete =
          if (name.equalsIgnoreCase(PercentileDisc.name)) {
            True()(pos)
          } else if (name.equalsIgnoreCase(PercentileCont.name)) {
            False()(pos)
          } else {
            throw new IllegalArgumentException(s"Unexpected function name: $name")
          }
        (accVars :+ v, accPercentiles :+ args(1), accIsDiscretes :+ isDiscrete)
    }
  }

  private def varToKey(variable: LogicalVariable): String = variable.name
}
