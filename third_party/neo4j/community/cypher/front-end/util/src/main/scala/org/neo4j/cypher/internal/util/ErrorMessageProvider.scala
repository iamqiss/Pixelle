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

/**
 * Formats error messages.
 */
trait ErrorMessageProvider {

  def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String

  def createSelfReferenceError(name: String, clauseName: String): String

  def createSelfReferenceError(name: String, variableType: String, clauseName: String): String

  def createUseClauseUnsupportedError(): String

  def createDynamicGraphReferenceUnsupportedError(graphName: String): String

  def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean = false): String
}

object NotImplementedErrorMessageProvider extends ErrorMessageProvider {

  override def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String = ???

  override def createSelfReferenceError(name: String, clauseName: String): String = ???

  override def createSelfReferenceError(name: String, variableType: String, clauseName: String): String = ???

  override def createUseClauseUnsupportedError(): String = ???

  override def createDynamicGraphReferenceUnsupportedError(graphName: String): String = ???

  override def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean): String = ???
}

object EmptyErrorMessageProvider extends ErrorMessageProvider {

  override def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String = ""

  override def createSelfReferenceError(name: String, clauseName: String): String = ""

  override def createSelfReferenceError(name: String, variableType: String, clauseName: String): String = ""

  override def createUseClauseUnsupportedError(): String = ""

  override def createDynamicGraphReferenceUnsupportedError(graphName: String): String = ""

  override def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean): String = ""
}
