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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.javacompat.ResultSubscriber
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.READ_ONLY
import org.neo4j.cypher.internal.runtime.WRITE
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.exceptions.ProfilerStatisticsNotReadyException
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.notifications.NotificationImplementation
import org.neo4j.notifications.StandardGqlStatusObject

class StandardInternalExecutionResult(
  runtimeResult: RuntimeResult,
  taskCloser: TaskCloser,
  outerCloseable: AutoCloseable,
  override val queryType: InternalQueryType,
  override val executionMode: ExecutionMode,
  planDescriptionBuilder: PlanDescriptionBuilder,
  subscriber: QuerySubscriber,
  internalNotifications: () => Seq[NotificationImplementation]
) extends InternalExecutionResult {

  self =>

  override def initiate(): Unit = {
    // For write only queries and queries that return no rows, execute all
    // work immediately, and close all resources.
    if (queryType == WRITE || fieldNames().isEmpty) {
      request(1)
      await()
      close(Success)
    }

    subscriber match {
      case coreAPI: ResultSubscriber =>
        // OBS: check before materialization
        val consumedBeforeMaterialize = runtimeResult.consumptionState == ConsumptionState.EXHAUSTED

        // By policy we materialize the result directly unless it's a read only query.
        if (queryType != READ_ONLY) {
          coreAPI.materialize(this)
        }

        // ... and if we do not return any rows, we close all resources.
        if (consumedBeforeMaterialize) {
          close(Success)
        }

      case _ => // do nothing
    }
  }

  /*
  ======= OPEN / CLOSE ==========
   */

  protected def isOpen: Boolean = !isClosed

  override def isClosed: Boolean = taskCloser.isClosed

  override def close(reason: CloseReason): Unit = {
    val closer: TaskCloser = taskCloser

    runtimeResult match {
      case result: AsyncCleanupOnClose =>
        val onFinishedCallback = () => {
          closer.close(reason)
        }
        result.registerOnFinishedCallback(onFinishedCallback)
        runtimeResult.cancel()

      case _ =>
        runtimeResult.cancel()
        closer.close(reason)
        outerCloseable.close()
    }
  }

  override def request(numberOfRows: Long): Unit =
    runtimeResult.request(numberOfRows)

  override def cancel(): Unit = {
    close()
  }

  override def await(): Boolean = runtimeResult.await()

  override def awaitCleanup(): Unit = {
    try {
      runtimeResult.awaitCleanup()
    } finally {
      outerCloseable.close()
    }
  }

  /*
  ======= META DATA ==========
   */

  override def fieldNames(): Array[String] = runtimeResult.fieldNames()

  override def executionMetadataAvailable(): Boolean = {
    executionMode != ProfileMode || runtimeResult.consumptionState == ConsumptionState.EXHAUSTED
  }

  override lazy val executionPlanDescription: InternalPlanDescription = {

    if (executionMode == ProfileMode) {
      if (runtimeResult.consumptionState != ConsumptionState.EXHAUSTED) {
        // TODO: Do we really need to close here?
        val error = ProfilerStatisticsNotReadyException.invalidUseOfProfile()
        taskCloser.close(Error(error))
        outerCloseable.close()
        throw error
      }
      planDescriptionBuilder.profile(runtimeResult.queryProfile)
    } else {
      planDescriptionBuilder.explain()
    }

  }

  override def notifications: Iterable[NotificationImplementation] = internalNotifications()

  override def gqlStatusObjects: Iterable[GqlStatusObject] = {
    val gqlStatusObjectsNotifications = internalNotifications().asInstanceOf[Iterable[GqlStatusObject]]

    val allGqlStatusObjects: Seq[GqlStatusObject] = {

      if (fieldNames().isEmpty) {
        // No result columns =>  OMITTED RESULT
        gqlStatusObjectsNotifications ++ Seq(StandardGqlStatusObject.OMITTED_RESULT)
      } else if (!runtimeResult.hasServedRows) {
        if (runtimeResult.consumptionState() == ConsumptionState.EXHAUSTED) {
          // Exhausted without result rows => NO DATA
          gqlStatusObjectsNotifications ++ Seq(StandardGqlStatusObject.NO_DATA)
        } else {
          // Not exhausted without result rows => UNKNOWN NO DATA
          gqlStatusObjectsNotifications ++ Seq(StandardGqlStatusObject.UNKNOWN_NO_DATA)
        }
      } else {
        // At least one result row => SUCCESS
        gqlStatusObjectsNotifications ++ Seq(StandardGqlStatusObject.SUCCESS)
      }
    }.toSeq

    // Sort according to GQL, so the most severe GqlStatusObject is first in the list
    // NO DATA < WARNING < SUCCESSFUL RESULT < INFORMATION
    allGqlStatusObjects.sortBy {
      case x: NotificationImplementation => x.getCondition
      case x: StandardGqlStatusObject    => x.getCondition
      case x => throw new IllegalArgumentException(
          s"Expected a NotificationImplementation or StandardGqlStatusObject but got ${x.getClass}."
        )
    }
  }

  override def getError: Option[Throwable] = Option(runtimeResult.getErrorOrNull)
}

object StandardInternalExecutionResult {
  final val NoOuterCloseable: AutoCloseable = () => ()
}
