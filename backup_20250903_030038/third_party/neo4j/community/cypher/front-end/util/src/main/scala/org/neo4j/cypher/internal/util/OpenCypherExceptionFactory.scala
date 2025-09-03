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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.ArithmeticException
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.gqlstatus.CommonGqlStatusObjectImplementation
import org.neo4j.gqlstatus.ErrorGqlStatusObject

object OpenCypherExceptionFactory {

  class ArithmeticException private[OpenCypherExceptionFactory] (
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    cause: Throwable = null
  ) extends CypherException(message, cause) {
    def this(message: String, cause: Throwable) = this(null, message, cause)
  }

  class SyntaxException private[OpenCypherExceptionFactory] (
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    val pos: InputPosition
  ) extends CypherException(message) {

    def this(message: String, pos: InputPosition) = this(null, message, pos)

    override def getMessage: String = {
      s"$message ($pos)"
    }
  }

}

case class OpenCypherExceptionFactory(preParserOffset: Option[InputPosition]) extends CypherExceptionFactory {

  override def arithmeticException(message: String, cause: Exception): CypherException =
    new ArithmeticException(message, cause)

  override def arithmeticException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    cause: Exception
  ): CypherException = {
    new ArithmeticException(gqlStatusObject, message, cause)
  }

  override def syntaxException(message: String, pos: InputPosition): CypherException = {
    val adjustedPosition = pos.withOffset(preParserOffset)
    new SyntaxException(message, adjustedPosition)
  }

  override def syntaxException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    pos: InputPosition
  ): CypherException = {
    val adjustedPosition = pos.withOffset(preParserOffset)

    // Adjust the position in the GQL object
    val gqlWithAdjustedPosition =
      if (gqlStatusObject != null) {
        val gqlImpl = gqlStatusObject.asInstanceOf[CommonGqlStatusObjectImplementation]
        gqlImpl.adjustPosition(
          pos.line,
          pos.column,
          pos.offset,
          adjustedPosition.line,
          adjustedPosition.column,
          adjustedPosition.offset
        )
        gqlImpl.asInstanceOf[ErrorGqlStatusObject]
      } else {
        gqlStatusObject
      }

    new SyntaxException(gqlWithAdjustedPosition, message, adjustedPosition)
  }
}
