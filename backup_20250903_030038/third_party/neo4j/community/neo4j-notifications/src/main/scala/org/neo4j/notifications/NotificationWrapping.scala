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
package org.neo4j.notifications

import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.util.AggregationSkippedNull
import org.neo4j.cypher.internal.util.AssignPrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.AuthProviderNotDefined
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.CordonedServersExistedDuringAllocation
import org.neo4j.cypher.internal.util.DeprecatedBooleanCoercion
import org.neo4j.cypher.internal.util.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedIdentifierUnicode
import org.neo4j.cypher.internal.util.DeprecatedIdentifierWhitespaceUnicode
import org.neo4j.cypher.internal.util.DeprecatedImportingWithInSubqueryCall
import org.neo4j.cypher.internal.util.DeprecatedIndexProviderOption
import org.neo4j.cypher.internal.util.DeprecatedKeywordVariableInWhenOperand
import org.neo4j.cypher.internal.util.DeprecatedNodesOrRelationshipsInSetClauseNotification
import org.neo4j.cypher.internal.util.DeprecatedOptionInOptionMap
import org.neo4j.cypher.internal.util.DeprecatedPrecedenceOfLabelExpressionPredicate
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInMerge
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.DeprecatedRuntimeNotification
import org.neo4j.cypher.internal.util.DeprecatedSeedingOption
import org.neo4j.cypher.internal.util.DeprecatedStoreFormat
import org.neo4j.cypher.internal.util.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.util.DeprecatedWhereVariableInNodePattern
import org.neo4j.cypher.internal.util.DeprecatedWhereVariableInRelationshipPattern
import org.neo4j.cypher.internal.util.ExternalAuthNotEnabled
import org.neo4j.cypher.internal.util.FixedLengthRelationshipInShortestPath
import org.neo4j.cypher.internal.util.GrantRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.util.ImpossibleRevokeCommandWarning
import org.neo4j.cypher.internal.util.IndexOrConstraintAlreadyExistsNotification
import org.neo4j.cypher.internal.util.IndexOrConstraintDoesNotExistNotification
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InsecureProtocol
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.NoDatabasesReallocated
import org.neo4j.cypher.internal.util.RedundantOptionalProcedure
import org.neo4j.cypher.internal.util.RedundantOptionalSubquery
import org.neo4j.cypher.internal.util.RepeatedRelationshipReference
import org.neo4j.cypher.internal.util.RepeatedVarLengthRelationshipReference
import org.neo4j.cypher.internal.util.RequestedTopologyMatchedCurrentTopology
import org.neo4j.cypher.internal.util.RevokePrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.RuntimeUnsatisfiableRelationshipTypeExpression
import org.neo4j.cypher.internal.util.ServerAlreadyCordoned
import org.neo4j.cypher.internal.util.ServerAlreadyEnabled
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.UnsatisfiableRelationshipTypeExpression
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType
import org.neo4j.graphdb

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

object NotificationWrapping {

  def asKernelNotificationJava(
    offset: Option[InputPosition],
    notification: InternalNotification
  ): NotificationImplementation = {
    asKernelNotification(offset)(notification)
  }

  def asKernelNotification(offset: Option[InputPosition])(notification: InternalNotification)
    : NotificationImplementation = notification match {
    case CartesianProductNotification(pos, variables, pattern) =>
      NotificationCodeWithDescription.cartesianProduct(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.cartesianProductDescription(variables.asJava),
        pattern
      )
    case RuntimeUnsupportedNotification(failingConf, fallbackRuntimeConf, cause) =>
      NotificationCodeWithDescription.runtimeUnsupported(
        graphdb.InputPosition.empty,
        failingConf,
        fallbackRuntimeConf,
        cause
      )
    case IndexHintUnfulfillableNotification(variableName, label, propertyKeys, entityType, indexType) =>
      val indexHintType = indexType match {
        case UsingAnyIndexType   => IndexHintIndexType.ANY
        case UsingTextIndexType  => IndexHintIndexType.TEXT
        case UsingRangeIndexType => IndexHintIndexType.RANGE
        case UsingPointIndexType => IndexHintIndexType.POINT
      }
      NotificationCodeWithDescription.indexHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.indexHint(entityType, indexHintType, variableName, label, propertyKeys: _*),
        NotificationDetail.index(indexHintType, label, propertyKeys.asJava)
      )
    case JoinHintUnfulfillableNotification(variables) =>
      val javaVariables = variables.asJava
      NotificationCodeWithDescription.joinHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.joinKey(javaVariables),
        javaVariables
      )
    case NodeIndexLookupUnfulfillableNotification(labels) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.nodeIndexSeekOrScan(labels.asJava),
        labels.toSeq.asJava
      )
    case RelationshipIndexLookupUnfulfillableNotification(relTypes) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.relationshipIndexSeekOrScan(relTypes.asJava),
        relTypes.toSeq.asJava
      )
    case EagerLoadCsvNotification =>
      NotificationCodeWithDescription.eagerLoadCsv(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification(labelName) =>
      NotificationCodeWithDescription.largeLabelLoadCsv(
        graphdb.InputPosition.empty,
        labelName
      )
    case MissingLabelNotification(pos, label) =>
      NotificationCodeWithDescription.missingLabel(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingLabel(label),
        label
      )
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCodeWithDescription.missingRelType(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingRelationshipType(relType),
        relType
      )
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCodeWithDescription.missingPropertyName(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.propertyName(name),
        name
      )
    case UnboundedShortestPathNotification(pos, pattern) =>
      NotificationCodeWithDescription.unboundedShortestPath(pos.withOffset(offset).asInputPosition, pattern)
    case ExhaustiveShortestPathForbiddenNotification(pos, pathPredicates) =>
      NotificationCodeWithDescription.exhaustiveShortestPath(
        pos.withOffset(offset).asInputPosition,
        pathPredicates.toSeq.asJava
      )
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      if (newName.isEmpty || newName.get.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedFunctionWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName.get),
          oldName,
          newName.get
        )
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      if (newName.isEmpty || newName.get.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedProcedureWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName.get),
          oldName,
          newName.get
        )
    case DeprecatedProcedureReturnFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureReturnField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedField(procedure, field),
        procedure,
        field
      )
    case DeprecatedProcedureFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedInputField(procedure, field),
        procedure,
        field
      )
    case DeprecatedFunctionFieldNotification(pos, function, field) =>
      NotificationCodeWithDescription.deprecatedFunctionField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedInputField(function, field),
        function,
        field
      )
    case DeprecatedRelTypeSeparatorNotification(pos, oldExpression, rewrittenExpression) =>
      NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecationNotificationDetail(rewrittenExpression),
        oldExpression,
        rewrittenExpression
      )
    case DeprecatedNodesOrRelationshipsInSetClauseNotification(pos, deprecated, replacement) =>
      NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause(
        pos.withOffset(offset).asInputPosition,
        deprecated,
        replacement
      )
    case DeprecatedPropertyReferenceInCreate(pos, name) =>
      NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate(
        pos.withOffset(offset).asInputPosition,
        name
      )
    case DeprecatedPropertyReferenceInMerge(pos, name) =>
      NotificationCodeWithDescription.deprecatedPropertyReferenceInMerge(
        pos.withOffset(offset).asInputPosition,
        name
      )
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCodeWithDescription.procedureWarning(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.procedureWarning(name, warning),
        warning,
        name
      )
    case MissingParametersNotification(parameters) =>
      NotificationCodeWithDescription.missingParameterForExplain(
        graphdb.InputPosition.empty,
        NotificationDetail.missingParameters(parameters.asJava),
        parameters.asJava
      )
    case CodeGenerationFailedNotification(failingConf, fallbackRuntimeConf, cause) =>
      NotificationCodeWithDescription.codeGenerationFailed(
        graphdb.InputPosition.empty,
        failingConf,
        fallbackRuntimeConf,
        cause
      )
    case SubqueryVariableShadowing(pos, varName) =>
      NotificationCodeWithDescription.subqueryVariableShadowing(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.shadowingVariable(varName),
        varName
      )
    case RedundantOptionalProcedure(pos, proc) =>
      NotificationCodeWithDescription.redundantOptionalProcedure(
        pos.withOffset(offset).asInputPosition,
        proc
      )
    case RedundantOptionalSubquery(pos) =>
      NotificationCodeWithDescription.redundantOptionalSubquery(
        pos.withOffset(offset).asInputPosition
      )
    case DeprecatedImportingWithInSubqueryCall(pos, varName) =>
      NotificationCodeWithDescription.deprecatedImportingWithInSubqueryCall(
        pos.withOffset(offset).asInputPosition,
        varName
      )
    case DeprecatedWhereVariableInNodePattern(pos, variableName, properties) =>
      NotificationCodeWithDescription.deprecatedWhereVariableInNodePattern(
        pos.withOffset(offset).asInputPosition,
        variableName,
        properties
      )
    case DeprecatedWhereVariableInRelationshipPattern(pos, variableName, properties) =>
      NotificationCodeWithDescription.deprecatedWhereVariableInRelationshipPattern(
        pos.withOffset(offset).asInputPosition,
        variableName,
        properties
      )
    case DeprecatedPrecedenceOfLabelExpressionPredicate(pos, labelExpressionPredicate) =>
      NotificationCodeWithDescription.deprecatedPrecedenceOfLabelExpressionPredicate(
        pos.withOffset(offset).asInputPosition,
        labelExpressionPredicate
      )
    case DeprecatedKeywordVariableInWhenOperand(pos, variableName, remainingExpression) =>
      NotificationCodeWithDescription.deprecatedKeywordVariableInWhenOperand(
        pos.withOffset(offset).asInputPosition,
        variableName,
        remainingExpression
      )
    case HomeDatabaseNotPresent(name) => NotificationCodeWithDescription.homeDatabaseNotPresent(
        InputPosition.NONE.asInputPosition,
        s"HOME DATABASE: $name",
        name
      )
    case FixedLengthRelationshipInShortestPath(pos, deprecated, replacement) =>
      NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship(
        pos.withOffset(offset).asInputPosition,
        deprecated,
        replacement
      )

    case DeprecatedTextIndexProvider(pos) =>
      NotificationCodeWithDescription.deprecatedTextIndexProvider(
        pos.withOffset(offset).asInputPosition
      )

    case DeprecatedIndexProviderOption() =>
      NotificationCodeWithDescription.deprecatedIndexProviderOption(
        graphdb.InputPosition.empty
      )

    case DeprecatedDatabaseNameNotification(name, pos) =>
      NotificationCodeWithDescription.deprecatedDatabaseName(
        pos.map(_.withOffset(offset).asInputPosition).getOrElse(graphdb.InputPosition.empty),
        s"Name: $name"
      )

    case DeprecatedRuntimeNotification(msg, oldOption, newOption) =>
      NotificationCodeWithDescription.deprecatedRuntimeOption(
        graphdb.InputPosition.empty,
        msg,
        oldOption,
        newOption
      )

    case UnsatisfiableRelationshipTypeExpression(position, relTypeExpression) =>
      NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.unsatisfiableRelTypeExpression(relTypeExpression),
        relTypeExpression
      )

    case RuntimeUnsatisfiableRelationshipTypeExpression(types) =>
      val stringified = types.mkString("&")
      NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression(
        InputPosition.NONE.asInputPosition,
        NotificationDetail.unsatisfiableRelTypeExpression(stringified),
        stringified
      )

    case RepeatedRelationshipReference(position, relName, pattern) =>
      NotificationCodeWithDescription.repeatedRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedRelationship(relName),
        relName,
        pattern
      )

    case RepeatedVarLengthRelationshipReference(position, relName, pattern) =>
      NotificationCodeWithDescription.repeatedVarLengthRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedVarLengthRel(relName),
        relName,
        pattern
      )

    case DeprecatedIdentifierWhitespaceUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode(
        position.asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedIdentifierUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierUnicode(
        position.asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedConnectComponentsPlannerPreParserOption(position) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption(
        position.asInputPosition
      )

    case DeprecatedOptionInOptionMap(oldOption, newOption) =>
      NotificationCodeWithDescription.deprecatedOptionInOptionMap(oldOption, newOption)

    case DeprecatedSeedingOption(option) => NotificationCodeWithDescription.deprecatedSeedingOption(option)

    case DeprecatedStoreFormat(format) =>
      NotificationCodeWithDescription.deprecatedStoreFormat(format)

    case AuthProviderNotDefined(provider) =>
      NotificationCodeWithDescription.authProviderNotDefined(
        graphdb.InputPosition.empty,
        provider
      )

    case _: ExternalAuthNotEnabled =>
      NotificationCodeWithDescription.externalAuthNotEnabled(graphdb.InputPosition.empty)

    case AssignPrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectAssignPrivilege(
        graphdb.InputPosition.empty,
        command
      )

    case RevokePrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectRevokePrivilege(
        graphdb.InputPosition.empty,
        command
      )

    case GrantRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectGrantRole(
        graphdb.InputPosition.empty,
        command
      )

    case RevokeRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectRevokeRole(
        graphdb.InputPosition.empty,
        command
      )

    case IndexOrConstraintAlreadyExistsNotification(command, conflicting) =>
      NotificationCodeWithDescription.indexOrConstraintAlreadyExists(
        graphdb.InputPosition.empty,
        command,
        conflicting
      )

    case IndexOrConstraintDoesNotExistNotification(command, name) =>
      NotificationCodeWithDescription.indexOrConstraintDoesNotExist(
        graphdb.InputPosition.empty,
        command,
        name
      )

    case ImpossibleRevokeCommandWarning(command, cause) =>
      NotificationCodeWithDescription.impossibleRevokeCommand(
        graphdb.InputPosition.empty,
        command,
        cause
      )

    case ServerAlreadyEnabled(server) =>
      NotificationCodeWithDescription.serverAlreadyEnabled(
        graphdb.InputPosition.empty,
        server
      )

    case ServerAlreadyCordoned(server) =>
      NotificationCodeWithDescription.serverAlreadyCordoned(
        graphdb.InputPosition.empty,
        server
      )

    case NoDatabasesReallocated() =>
      NotificationCodeWithDescription.noDatabasesReallocated(
        graphdb.InputPosition.empty
      )

    case CordonedServersExistedDuringAllocation(servers) =>
      NotificationCodeWithDescription.cordonedServersExist(
        graphdb.InputPosition.empty,
        servers.asJava
      )

    case RequestedTopologyMatchedCurrentTopology() =>
      NotificationCodeWithDescription.requestedTopologyMatchedCurrentTopology(
        graphdb.InputPosition.empty
      )

    case AggregationSkippedNull    => NotificationCodeWithDescription.aggregationSkippedNull()
    case DeprecatedBooleanCoercion => NotificationCodeWithDescription.deprecatedBooleanCoercion()
    case InsecureProtocol          => NotificationCodeWithDescription.insecureProtocol()

    case _ => throw new IllegalStateException("Missing mapping for notification detail.")
  }

  implicit private class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
