/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.util.test_helpers.CompareAsPrettyStrings.Wrapper

trait CompareAsPrettyStrings {
  self: CypherFunSuite =>

  implicit class AnyHasCompareAsPrettyStrings(lhs: Any) {

    def asPrettyString: String = {
      pprint.PPrinter.BlackWhite(lhs).render
    }

    def compareAsPrettyStrings(rhs: Any): Unit = {
      // wrap to prevent Scalatest from minimizing the String diff
      Wrapper(lhs.asPrettyString) shouldEqual Wrapper(rhs.asPrettyString)

      fail("compareAsPrettyStrings is only for debugging and should not be committed")
    }
  }
}

object CompareAsPrettyStrings {

  private case class Wrapper(str: String) {
    override def toString: String = str
  }
}
