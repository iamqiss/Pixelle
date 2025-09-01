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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class NodesByLabelScanSlottedPipe(
  ident: String,
  label: LazyLabel,
  slots: SlotConfiguration,
  indexOrder: IndexOrder
)(val id: Id = Id.INVALID_ID) extends Pipe {

  private val offset = slots.longOffset(ident)

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {

    val labelId = label.getId(state.query)
    if (labelId == LazyLabel.UNKNOWN) ClosingIterator.empty
    else {
      PrimitiveLongHelper.map(
        state.query.getNodesByLabel(state.nodeLabelTokenReadSession.get, labelId, indexOrder),
        { nodeId =>
          val context = state.newRowWithArgument(rowFactory)
          context.setLongAt(offset, nodeId)
          context
        }
      )
    }
  }
}
