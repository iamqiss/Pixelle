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

import org.neo4j.exceptions.ArithmeticException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.CommonGqlStatusObjectImplementation
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

trait CypherExceptionFactory {
  def arithmeticException(message: String, cause: Exception): RuntimeException

  def arithmeticException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    cause: Exception
  ): RuntimeException
  def syntaxException(message: String, pos: InputPosition): RuntimeException
  def syntaxException(gqlStatusObject: ErrorGqlStatusObject, message: String, pos: InputPosition): RuntimeException

  def unsupportedRequestOnSystemDatabaseException(
    invalidInput: String,
    legacyMessage: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N17)
          .withParam(GqlParams.StringParam.input, invalidInput)
          .build()
      )
      .build()
    syntaxException(gql, legacyMessage, pos)
  }

  def invalidInputException(
    wrongInput: String,
    forField: String,
    expectedInput: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .withParam(GqlParams.StringParam.context, forField)
          .withParam(GqlParams.ListParam.inputList, java.util.List.of(expectedInput))
          .build()
      )
      .build()
    syntaxException(gql, legacyMessage, pos)
  }
}

case class Neo4jCypherExceptionFactory(queryText: String, preParserOffset: Option[InputPosition])
    extends CypherExceptionFactory {

  override def arithmeticException(message: String, cause: Exception): Neo4jException =
    new ArithmeticException(message, cause)

  override def arithmeticException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    cause: Exception
  ): Neo4jException = {
    new ArithmeticException(gqlStatusObject, message, cause)
  }

  override def syntaxException(message: String, pos: InputPosition): Neo4jException = {
    val adjustedPosition = pos.withOffset(preParserOffset)
    new SyntaxException(s"$message ($adjustedPosition)", queryText, adjustedPosition.offset)
  }

  override def syntaxException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    pos: InputPosition
  ): Neo4jException = {
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

    new SyntaxException(
      gqlWithAdjustedPosition,
      s"$message ($adjustedPosition)",
      queryText,
      adjustedPosition.offset
    )
  }

}
