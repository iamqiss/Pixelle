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

import org.neo4j.cypher.internal.compiler.helpers.predicatesPushedDownToRemote
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter will
 */
object RemoteBatchPropertiesFilterMergeRewriter extends Rewriter with TopDownMergeableRewriter {

  override val innerRewriter: Rewriter = {
    Rewriter.lift {
      case filter @ Selection(
          predicate,
          remoteBatchProperties: RemoteBatchProperties
        ) =>
        val remoteBatchPropertiesDependencies = remoteBatchProperties.properties.flatMap(_.dependencies)
        val (inlinablePreds, nonInlinablePreds) = predicate.exprs
          .partition(expr =>
            expr.dependencies.subsetOf(remoteBatchPropertiesDependencies) && predicatesPushedDownToRemote(expr)
          )
        if (inlinablePreds.nonEmpty) {
          val newPlan = RemoteBatchPropertiesWithFilter(
            remoteBatchProperties.source,
            inlinablePreds,
            remoteBatchProperties.properties
          )(SameId(remoteBatchProperties.id))
          if (nonInlinablePreds.nonEmpty)
            Selection(nonInlinablePreds.toSeq, newPlan)(SameId(filter.id))
          else
            newPlan
        } else
          filter
    }
  }

  private val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
