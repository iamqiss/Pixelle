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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition

object AdministrationCommandSemanticAnalysis {

  def unsupportedRequestErrorOnSystemDatabase(
    invalidInput: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.unsupportedRequestOnSystemDatabase(invalidInput, legacyMessage, position))

  def invalidInputError(
    wrongInput: String,
    forField: String,
    expectedInput: List[String],
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.invalidInput(wrongInput, forField, expectedInput, legacyMessage, position))

  def missingMandatoryAuthClauseError(
    clause: String,
    authProvider: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.missingMandatoryAuthClause(clause, authProvider, legacyMessage, position))

  def duplicateClauseError(
    clause: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.duplicateClause(clause, legacyMessage, position))

  def inputContainsInvalidCharactersError(
    invalidInput: String,
    context: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.inputContainsInvalidCharacters(
      invalidInput,
      context,
      legacyMessage,
      position
    ))
}
