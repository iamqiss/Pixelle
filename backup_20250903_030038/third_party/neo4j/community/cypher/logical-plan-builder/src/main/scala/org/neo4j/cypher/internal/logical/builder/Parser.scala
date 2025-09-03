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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.removeSyntaxTracking
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.topDown

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Parser {

  val injectCachedProperties: Rewriter = topDown(Rewriter.lift {
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cache" || name == "cacheN" =>
      CachedProperty(v, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cacheFromStore" || name == "cacheNFromStore" =>
      CachedProperty(v, v, pkn, NODE_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable("cacheR"), Property(v: Variable, pkn: PropertyKeyName)) =>
      CachedProperty(v, v, pkn, RELATIONSHIP_TYPE)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable("cacheRFromStore"), Property(v: Variable, pkn: PropertyKeyName)) =>
      CachedProperty(v, v, pkn, RELATIONSHIP_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cacheNHasProperty" =>
      CachedHasProperty(v, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cacheRHasProperty" =>
      CachedHasProperty(v, v, pkn, RELATIONSHIP_TYPE)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cacheNHasPropertyFromStore" =>
      CachedHasProperty(v, v, pkn, NODE_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
    case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
      if name == "cacheRHasPropertyFromStore" =>
      CachedHasProperty(v, v, pkn, RELATIONSHIP_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)

  })

  val invalidateInputPositions: Rewriter = topDown(Rewriter.lift {
    // Special handling of PatternPartWithSelector because it happens to not include an argument for InputPosition.
    // If more cases ends up being added this should probably be refactored. But that is left as an exercise to the reader.
    case x: PatternPartWithSelector => x
    case a: ASTNode                 => a.dup(a.treeChildren.toSeq :+ AbstractLogicalPlanBuilder.pos)
  })

  val replaceWrongFunctionInvocation: Rewriter = topDown(Rewriter.lift {
    case FunctionInvocation(FunctionName(Namespace(List()), "CoerceToPredicate"), _, Seq(expression), _, _) =>
      CoerceToPredicate(expression)
  })

  def cleanup[T <: ASTNode](in: T): T = inSequence(
    removeSyntaxTracking.instance,
    injectCachedProperties,
    invalidateInputPositions,
    replaceWrongFunctionInvocation,
    LabelExpressionPredicateNormalizer.instance,
    // Flattening boolean operators otherwise it is impossible to create instances of Ands / Ors
    flattenBooleanOperators.instance(CancellationChecker.NeverCancelled)
  )(in).asInstanceOf[T]

  private val regex = s"(.+) [Aa][Ss] (.+)".r

  def parseProjections(projections: String*): Map[String, Expression] = {
    projections.map {
      case regex(Parser(expression), VariableParser(alias)) => (alias, expression)
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a projection")
    }.toMap
  }

  def parseAggregationProjections(projections: String*): Map[String, Expression] = {
    projections.map {
      case regex(AggregationParser(expression), VariableParser(alias)) => (alias, expression)
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as an aggregation projection")
    }.toMap
  }

  // Note, only supports cypher 5 for now.
  def astParser(cypher: String) = new Cypher5AstParser(cypher, Neo4jCypherExceptionFactory(cypher, None), None)

  def parseExpression(text: String): Expression = {
    Try(astParser(text).expression()) match {
      case Success(expression) =>
        Parser.cleanup(expression)
      case Failure(exception) =>
        println(s"Failed parsing expression `$text``")
        throw exception
    }
  }

  def parsePatternElement(text: String): PatternElement = {
    Try(astParser(text).parse[PatternElement](_.patternElement())) match {
      case Success(patternElement) =>
        Parser.cleanup(patternElement)
      case Failure(exception) =>
        println(s"Failed parsing pattern element `$text``")
        throw exception
    }
  }

  def parseProcedureCall(text: String): UnresolvedCall = {
    astParser(s"CALL $text").parse[ASTNode](_.callClause()) match {
      case u: UnresolvedCall => Parser.cleanup(u)
      case c                 => throw new IllegalArgumentException(s"Expected UnresolvedCall but got: $c")
    }
  }

  private val sortRegex = "(.+) (?i)(ASC|DESC)".r

  def parseSort(text: Seq[String]): Seq[ColumnOrder] = {
    text.map(parseSort)
  }

  def parseSort(text: String): ColumnOrder = {
    text match {
      case sortRegex(VariableParser(variable), direction) =>
        if ("ASC".equalsIgnoreCase(direction)) Ascending(varFor(variable))
        else if ("DESC".equalsIgnoreCase(direction)) Descending(varFor(variable))
        else throw new IllegalArgumentException(s"Invalid direction $direction")
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a sort item")
    }
  }

  def parseGraphReference(text: String): GraphReference =
    astParser(s"USE $text").parse[UseGraph](_.useClause()).graphReference

  def unapply(arg: String): Option[Expression] = Some(parseExpression(arg))
}
