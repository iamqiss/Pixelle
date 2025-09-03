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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class CastSupportTest extends CypherFunSuite {

  test("downcastAppMatchTest") {
    val one: AnyValue = Values.intValue(1)
    CastSupport.castOrFail[IntValue](one) should equal(one)
  }

  test("downcastAppMismatchTest") {
    val seqOne: AnyValue = VirtualValues.list(Values.intValue(1))
    intercept[CypherTypeException](CastSupport.castOrFail[IntValue](seqOne))
  }
}
