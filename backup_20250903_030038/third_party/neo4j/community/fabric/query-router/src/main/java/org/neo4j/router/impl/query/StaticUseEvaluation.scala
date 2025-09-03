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
package org.neo4j.router.impl.query

import org.neo4j.cypher.internal.ast._
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.dbms.api.DatabaseNotFoundExceptionCreator
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.router.util.Errors
import org.neo4j.values.ElementIdDecoder
import org.neo4j.values.storable.StringValue
import org.neo4j.values.virtual.MapValue

import scala.jdk.OptionConverters.RichOptional

class StaticUseEvaluation {

  case class CatalogInfo(catalogName: CatalogName, canBeCached: Boolean)

  /**
   * This function finds graph selections only on the highest level without descending into sub-queries.
   * It returns one item for Single Queries and multiple items for Union Queries.
   */
  def evaluateStaticTopQueriesGraphSelections(statement: Statement, databaseIdRepository: DatabaseIdRepository, params: MapValue): Seq[Option[CatalogInfo]] =
    topQueriesGraphSelections(statement).map(maybeGraphSelection => maybeGraphSelection.map(evaluateStatic(_, databaseIdRepository, params)))

  private def evaluateStaticOption(graphSelection: GraphSelection, databaseIdRepository: DatabaseIdRepository, params: MapValue): Option[CatalogInfo] =
    graphSelection.graphReference match {
      case s: GraphDirectReference => Some(CatalogInfo(s.catalogName, canBeCached = true))
      case s: GraphFunctionReference if s.functionInvocation.function.equals(GraphByElementId) =>
        val evaluator = new SimpleInternalExpressionEvaluator()
        val elementIdArgument = graphSelection.graphReference.arguments.head.asInstanceOf[FunctionInvocation].args.head
        val elementId = evaluator.evaluate(elementIdArgument, params = params)
          .asInstanceOf[StringValue]
          .stringValue()
        val uuid = ElementIdDecoder.database(elementId)

        /* There is two reasons for not caching the result of target graph when using element id:
        * 1) If the elementId is evaluated with a parameter we can not cache it based on the query string,
        *     e.g. USE graph.byElementId($elementID).
        * 2) If the elementId is given as a raw string in the query, we would have one cache entry per element id,
        *    so we would probably never hit the cache. E.g. USE graph.byElementId(`23023-asd9231-23fe9-312123`)
         */

        databaseIdRepository.getById(DatabaseIdFactory.from(uuid)).toScala
          .orElse(throw DatabaseNotFoundExceptionCreator.byElementIdFunction(elementId))
          .map(name => CatalogInfo(CatalogName.of(name.name()), canBeCached = false))
      case _                       => None
    }

  private def evaluateStatic(graphSelection: GraphSelection, databaseIdRepository: DatabaseIdRepository, params: MapValue): CatalogInfo =
    evaluateStaticOption(graphSelection, databaseIdRepository, params).getOrElse(Errors.dynamicGraphNotAllowed(graphSelection))

  private def singleQueries(statement: Statement): Seq[SingleQuery] =
    statement match {
      case sq: SingleQuery => Seq(sq)
      case union: Union    => singleQueries(union.lhs) ++ singleQueries(union.rhs)
      case _               => Seq()
    }

  private def topQueriesGraphSelections(statement: Statement): Seq[Option[GraphSelection]] = {
    statement match {
      case sc: SchemaCommand => Seq(sc.useGraph)
      case _                 => queryTopGraphSelections(statement)
    }
  }

  private def queryTopGraphSelections(statement: Statement): Seq[Option[GraphSelection]] = {
    singleQueries(statement).map(singleQuery => {
      val clause = singleQuery.clauses.headOption
      clause.collect {
        case gs: GraphSelection => gs
      }
    })
  }
}
