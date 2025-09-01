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

import java.lang

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Describes a notification
 */
trait InternalNotification {
  def notificationName: String = this.getClass.getSimpleName.stripSuffix("$")
}

object InternalNotification {

  val allNotifications: Set[String] = Set(
    "CartesianProductNotification",
    "UnboundedShortestPathNotification",
    "DeprecatedFunctionNotification",
    "DeprecatedRelTypeSeparatorNotification",
    "DeprecatedNodesOrRelationshipsInSetClauseNotification",
    "DeprecatedPropertyReferenceInCreate",
    "DeprecatedPropertyReferenceInMerge",
    "SubqueryVariableShadowing",
    "RedundantOptionalProcedure",
    "RedundantOptionalSubquery",
    "DeprecatedImportingWithInSubqueryCall",
    "DeprecatedWhereVariableInNodePattern",
    "DeprecatedWhereVariableInRelationshipPattern",
    "DeprecatedPrecedenceOfLabelExpressionPredicate",
    "DeprecatedKeywordVariableInWhenOperand",
    "HomeDatabaseNotPresent",
    "FixedLengthRelationshipInShortestPath",
    "DeprecatedDatabaseNameNotification",
    "DeprecatedRuntimeNotification",
    "DeprecatedTextIndexProvider",
    "DeprecatedIdentifierWhitespaceUnicode",
    "DeprecatedIdentifierUnicode",
    "UnsatisfiableRelationshipTypeExpression",
    "RuntimeUnsatisfiableRelationshipTypeExpression",
    "RepeatedRelationshipReference",
    "RepeatedVarLengthRelationshipReference",
    "DeprecatedConnectComponentsPlannerPreParserOption",
    "AssignPrivilegeCommandHasNoEffectNotification",
    "RevokePrivilegeCommandHasNoEffectNotification",
    "GrantRoleCommandHasNoEffectNotification",
    "RevokeRoleCommandHasNoEffectNotification",
    "ImpossibleRevokeCommandWarning",
    "ServerAlreadyEnabled",
    "ServerAlreadyCordoned",
    "NoDatabasesReallocated",
    "CordonedServersExistedDuringAllocation",
    "RuntimeUnsupportedNotification",
    "IndexHintUnfulfillableNotification",
    "JoinHintUnfulfillableNotification",
    "NodeIndexLookupUnfulfillableNotification",
    "RelationshipIndexLookupUnfulfillableNotification",
    "EagerLoadCsvNotification",
    "LargeLabelWithLoadCsvNotification",
    "MissingLabelNotification",
    "MissingRelTypeNotification",
    "MissingPropertyNameNotification",
    "ExhaustiveShortestPathForbiddenNotification",
    "DeprecatedProcedureNotification",
    "ProcedureWarningNotification",
    "DeprecatedProcedureReturnFieldNotification",
    "MissingParametersNotification",
    "CodeGenerationFailedNotification",
    "RequestedTopologyMatchedCurrentTopology",
    "IndexOrConstraintAlreadyExistsNotification",
    "IndexOrConstraintDoesNotExistNotification",
    "DeprecatedFunctionFieldNotification",
    "DeprecatedProcedureFieldNotification",
    "AuthProviderNotDefined",
    "ExternalAuthNotEnabled",
    "AggregationSkippedNull",
    "DeprecatedOptionInOptionMap",
    "DeprecatedIndexProviderOption",
    "DeprecatedSeedingOption",
    "DeprecatedStoreFormat",
    "DeprecatedBooleanCoercion",
    "InsecureProtocol"
  )

  def allNotificationsAsJavaIterable(): lang.Iterable[String] = allNotifications.asJava
}

case class CartesianProductNotification(position: InputPosition, isolatedVariables: Set[String], pattern: String)
    extends InternalNotification

case class UnboundedShortestPathNotification(position: InputPosition, pattern: String) extends InternalNotification

case class DeprecatedFunctionNotification(position: InputPosition, oldName: String, newName: Option[String])
    extends InternalNotification

case class DeprecatedRelTypeSeparatorNotification(
  position: InputPosition,
  oldExpression: String,
  rewrittenExpression: String
) extends InternalNotification

case class DeprecatedNodesOrRelationshipsInSetClauseNotification(
  position: InputPosition,
  deprecated: String,
  replacement: String
) extends InternalNotification

case class DeprecatedPropertyReferenceInCreate(position: InputPosition, varName: String) extends InternalNotification

case class DeprecatedPropertyReferenceInMerge(position: InputPosition, varName: String) extends InternalNotification

case class SubqueryVariableShadowing(position: InputPosition, varName: String) extends InternalNotification

case class RedundantOptionalProcedure(position: InputPosition, proc: String) extends InternalNotification

case class RedundantOptionalSubquery(position: InputPosition) extends InternalNotification

case class DeprecatedImportingWithInSubqueryCall(position: InputPosition, variable: String) extends InternalNotification

case class DeprecatedWhereVariableInNodePattern(position: InputPosition, variableName: String, properties: String)
    extends InternalNotification

case class DeprecatedWhereVariableInRelationshipPattern(
  position: InputPosition,
  variableName: String,
  properties: String
) extends InternalNotification

case class DeprecatedPrecedenceOfLabelExpressionPredicate(position: InputPosition, labelExpression: String)
    extends InternalNotification

case class DeprecatedKeywordVariableInWhenOperand(
  position: InputPosition,
  variableName: String,
  remainingExpression: String
) extends InternalNotification

case class HomeDatabaseNotPresent(databaseName: String) extends InternalNotification

case class FixedLengthRelationshipInShortestPath(position: InputPosition, deprecated: String, replacement: String)
    extends InternalNotification

case class DeprecatedDatabaseNameNotification(databaseName: String, position: Option[InputPosition])
    extends InternalNotification

case class DeprecatedRuntimeNotification(msg: String, oldOption: String, newOption: String)
    extends InternalNotification

case class DeprecatedTextIndexProvider(position: InputPosition) extends InternalNotification
case class DeprecatedIndexProviderOption() extends InternalNotification

case class DeprecatedIdentifierWhitespaceUnicode(position: InputPosition, unicode: Char, identifier: String)
    extends InternalNotification

case class DeprecatedIdentifierUnicode(position: InputPosition, unicode: Char, identifier: String)
    extends InternalNotification

case class UnsatisfiableRelationshipTypeExpression(position: InputPosition, labelExpression: String)
    extends InternalNotification

case class RepeatedRelationshipReference(position: InputPosition, relName: String, pattern: String)
    extends InternalNotification

case class RepeatedVarLengthRelationshipReference(position: InputPosition, relName: String, pattern: String)
    extends InternalNotification

case class DeprecatedConnectComponentsPlannerPreParserOption(position: InputPosition) extends InternalNotification

case class AuthProviderNotDefined(provider: String) extends InternalNotification
case class ExternalAuthNotEnabled() extends InternalNotification

case class AssignPrivilegeCommandHasNoEffectNotification(command: String) extends InternalNotification
case class RevokePrivilegeCommandHasNoEffectNotification(command: String) extends InternalNotification
case class GrantRoleCommandHasNoEffectNotification(command: String) extends InternalNotification
case class RevokeRoleCommandHasNoEffectNotification(command: String) extends InternalNotification

case class ImpossibleRevokeCommandWarning(command: String, cause: String) extends InternalNotification

case class ServerAlreadyEnabled(server: String) extends InternalNotification
case class ServerAlreadyCordoned(server: String) extends InternalNotification
case class NoDatabasesReallocated() extends InternalNotification
case class CordonedServersExistedDuringAllocation(servers: Seq[String]) extends InternalNotification
case class RequestedTopologyMatchedCurrentTopology() extends InternalNotification

case class IndexOrConstraintAlreadyExistsNotification(command: String, conflicting: String)
    extends InternalNotification
case class IndexOrConstraintDoesNotExistNotification(command: String, name: String) extends InternalNotification
case object AggregationSkippedNull extends InternalNotification

case object DeprecatedBooleanCoercion extends InternalNotification {
  def instance: DeprecatedBooleanCoercion.type = this
}

case class DeprecatedOptionInOptionMap(oldOption: String, replacmentOption: String) extends InternalNotification
case class DeprecatedSeedingOption(oldOption: String) extends InternalNotification
case class DeprecatedStoreFormat(format: String) extends InternalNotification

case object InsecureProtocol extends InternalNotification

case class RuntimeUnsatisfiableRelationshipTypeExpression(types: List[String]) extends InternalNotification {

  def this(types: java.util.List[String]) =
    this(types.asScala.toList)
}
