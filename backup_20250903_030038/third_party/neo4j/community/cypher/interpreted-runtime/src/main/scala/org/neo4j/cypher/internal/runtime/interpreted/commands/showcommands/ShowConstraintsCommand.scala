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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllExistsConstraints
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.NodeAllExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodeLabelExistenceConstraints
import org.neo4j.cypher.internal.ast.NodePropExistsConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.PropExistsConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RelAllExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropExistsConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.RelationshipSourceLabelConstraints
import org.neo4j.cypher.internal.ast.RelationshipTargetLabelConstraints
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.createStatementColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.entityTypeColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.idColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.labelsOrTypesColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.optionsColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.ownedIndexColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.propertiesColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.propertyTypeColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.typeColumn
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.createConstraintStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.getConstraintType
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createNodeConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createRelConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.GraphTypeDependence
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.SeqHasAsJava

// SHOW [
//   ALL
//   | NODE UNIQUE | RELATIONSHIP UNIQUE | UNIQUE
//   | NODE EXIST | RELATIONSHIP EXIST | EXIST
//   | NODE PROPERTY EXIST | RELATIONSHIP PROPERTY EXIST | PROPERTY EXIST
//   | NODE KEY | RELATIONSHIP KEY | KEY
//   | NODE PROPERTY TYPE | RELATIONSHIP PROPERTY TYPE | PROPERTY TYPE
// ] CONSTRAINT[S] [WHERE clause | YIELD clause]
case class ShowConstraintsCommand(
  constraintType: ShowConstraintType,
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  cypherVersion: CypherVersion
) extends Command(columns, yieldColumns) {
  private val returnCypher5Values: Boolean = cypherVersion == CypherVersion.Cypher5

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowConstraintAllowed()
    val constraints = ctx.getAllConstraints()
    val indexIdToName = ctx.getAllIndexes()
      .map { case (descriptor, _) => descriptor.getId -> descriptor.getName }

    val predicate: ConstraintDescriptor => Boolean = constraintType match {
      case _: UniqueConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE)
      case _: NodeUniqueConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE) && c.schema.entityType.equals(EntityType.NODE)
      case _: RelUniqueConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case KeyConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS)
      case NodeKeyConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS) && c.schema.entityType.equals(EntityType.NODE)
      case RelKeyConstraints => c =>
          c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case _: PropExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS)
      case _: NodePropExistsConstraints =>
        c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.NODE)
      case _: RelPropExistsConstraints =>
        c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case AllExistsConstraints =>
        c =>
          c.`type`().equals(schema.ConstraintType.EXISTS) ||
            c.`type`().equals(schema.ConstraintType.NODE_LABEL_EXISTENCE)
      case NodeAllExistsConstraints =>
        c =>
          (
            c.`type`().equals(schema.ConstraintType.EXISTS) ||
              c.`type`().equals(schema.ConstraintType.NODE_LABEL_EXISTENCE)
          ) && c.schema.entityType.equals(EntityType.NODE)
      case RelAllExistsConstraints =>
        c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case PropTypeConstraints => c => c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE)
      case NodePropTypeConstraints =>
        c => c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE) && c.schema.entityType.equals(EntityType.NODE)
      case RelPropTypeConstraints =>
        c =>
          c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case AllConstraints => _ => true // Should keep all and not filter away any constraints
      case c              => throw new IllegalStateException(s"Unknown constraint type: $c")
    }

    val relevantConstraints = constraints.filter {
      case (constraintDescriptor, _) => predicate(constraintDescriptor)
    }
    val sortedRelevantConstraints: ListMap[ConstraintDescriptor, ConstraintInfo] =
      ListMap(relevantConstraints.toSeq.sortBy(_._1.getName): _*)

    val rows = sortedRelevantConstraints.map {
      case (constraintDescriptor: ConstraintDescriptor, constraintInfo: ConstraintInfo) =>
        val propertyType =
          if (
            constraintDescriptor.isPropertyTypeConstraint &&
            // only fetch value if we need it
            (requestedColumnsNames.contains(propertyTypeColumn) ||
              requestedColumnsNames.contains(createStatementColumn))
          )
            Some(
              constraintDescriptor.asPropertyTypeConstraint().propertyType().userDescription()
            )
          else None
        // These don't really have a default/fallback and is used in multiple columns
        // so let's keep them as is regardless of if they are actually needed or not
        val entityType = constraintDescriptor.schema.entityType
        val constraintType = getConstraintType(
          constraintDescriptor.`type`,
          entityType,
          constraintInfo.endPointType,
          returnCypher5Values
        )

        requestedColumnsNames.map {
          // The id of the constraint, or null if created in transaction
          case `idColumn` =>
            val id =
              if (constraintIsAddedInTransaction(ctx, constraintDescriptor)) Values.NO_VALUE
              else Values.longValue(constraintDescriptor.getId)
            idColumn -> id
          // Name of the constraint, for example "myConstraint"
          case `nameColumn` => nameColumn -> Values.stringValue(constraintDescriptor.getName)
          // The ConstraintType of this constraint, one of "NODE_PROPERTY_UNIQUENESS", "RELATIONSHIP_PROPERTY_UNIQUENESS",
          //   "NODE_KEY", "RELATIONSHIP_KEY", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE",
          //   "NODE_PROPERTY_TYPE", "RELATIONSHIP_PROPERTY_TYPE", "NODE_LABEL_EXISTENCE",
          //   "RELATIONSHIP_SOURCE_LABEL", "RELATIONSHIP_TARGET_LABEL"
          case `typeColumn` => typeColumn -> Values.stringValue(constraintType.output)
          // Type of entities this constraint represents, either "NODE" or "RELATIONSHIP"
          case `entityTypeColumn` => entityTypeColumn -> Values.stringValue(entityType.name)
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"]
          case `labelsOrTypesColumn` => labelsOrTypesColumn -> VirtualValues.fromList(
              constraintInfo.labelsOrTypes.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava
            )
          // The properties of this constraint, for example ["propKey", "propKey2"], or null for label enforcing constraints
          case `propertiesColumn` =>
            val properties = Some(constraintInfo.properties)
              .filterNot(_.isEmpty) // empty list should return NO_VALUE instead of empty list
              .map(_.map(prop => Values.of(prop).asInstanceOf[AnyValue])) // map the properties to Values
              .map(props => VirtualValues.fromList(props.asJava)) // make it a ListValue
              .getOrElse(Values.NO_VALUE) // empty list should be NO_VALUE instead of empty list
            propertiesColumn -> properties
          // The name of the index associated to the constraint
          case `ownedIndexColumn` =>
            val ownedIndex =
              if (constraintDescriptor.isIndexBackedConstraint)
                indexIdToName.get(constraintDescriptor.asIndexBackedConstraint().ownedIndexId())
                  .map(Values.stringValue)
                  .getOrElse(Values.NO_VALUE)
              else Values.NO_VALUE
            ownedIndexColumn -> ownedIndex
          // The Cypher type this constraint restricts its property to
          case `propertyTypeColumn` =>
            propertyTypeColumn -> propertyType.map(Values.stringValue).getOrElse(Values.NO_VALUE)
          // The options for this constraint, shows index provider and config of the backing index
          case `optionsColumn` => optionsColumn -> getOptions(constraintDescriptor, constraintInfo)
          // The statement to recreate the constraint, or null for dependent constraints
          case `createStatementColumn` =>
            val createString = createConstraintStatement(
              constraintDescriptor.getName,
              constraintType,
              constraintInfo.labelsOrTypes,
              constraintInfo.properties,
              propertyType,
              constraintDescriptor.graphTypeDependence(),
              returnCypher5Values
            )
            createStatementColumn -> Values.stringOrNoValue(createString)
          case unknown =>
            // This match should cover all existing columns but we get scala warnings
            // on non-exhaustive match due to it being string values
            throw new IllegalStateException(s"Missing case for column: $unknown")
        }.toMap[String, AnyValue]
    }
    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows.toList)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getOptions(
    constraintDescriptor: ConstraintDescriptor,
    constraintInfo: ConstraintInfo
  ) = {
    if (constraintDescriptor.isIndexBackedConstraint) {
      val index = constraintInfo.maybeIndex.getOrElse(
        throw new IllegalStateException(
          s"Expected to find an index for index backed constraint ${constraintDescriptor.getName}"
        )
      )
      extractOptionsMap(index.getIndexType, index.getIndexProvider, index.getIndexConfig)
    } else Values.NO_VALUE
  }

  private def constraintIsAddedInTransaction(ctx: QueryContext, constraintDescriptor: ConstraintDescriptor): Boolean =
    Option(ctx.transactionalContext.kernelQueryContext.getTransactionStateOrNull)
      .map(_.constraintsChanges)
      .exists(_.isAdded(constraintDescriptor))
}

object ShowConstraintsCommand {

  private def createConstraintStatement(
    name: String,
    constraintType: ShowConstraintType,
    labelsOrTypes: List[String],
    properties: List[String],
    propertyType: Option[String],
    graphTypeDependence: GraphTypeDependence,
    returnCypher5Values: Boolean
  ): String = {
    if (graphTypeDependence.equals(GraphTypeDependence.DEPENDENT)) null
    else constraintType match {
      case _: NodeUniqueConstraints =>
        createNodeConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
      case _: RelUniqueConstraints =>
        createRelConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
      case NodeKeyConstraints =>
        val predicate = if (returnCypher5Values) "IS NODE KEY" else "IS KEY"
        createNodeConstraintCommand(name, labelsOrTypes, properties, predicate)
      case RelKeyConstraints =>
        val predicate = if (returnCypher5Values) "IS RELATIONSHIP KEY" else "IS KEY"
        createRelConstraintCommand(name, labelsOrTypes, properties, predicate)
      case _: NodePropExistsConstraints =>
        createNodeConstraintCommand(name, labelsOrTypes, properties, "IS NOT NULL")
      case _: RelPropExistsConstraints =>
        createRelConstraintCommand(name, labelsOrTypes, properties, "IS NOT NULL")
      case NodePropTypeConstraints =>
        val typeString = propertyType.getOrElse(
          throw new IllegalArgumentException(s"Expected a property type for $constraintType constraint.")
        )
        createNodeConstraintCommand(name, labelsOrTypes, properties, s"IS :: $typeString")
      case RelPropTypeConstraints =>
        val typeString = propertyType.getOrElse(
          throw new IllegalArgumentException(s"Expected a property type for $constraintType constraint.")
        )
        createRelConstraintCommand(name, labelsOrTypes, properties, s"IS :: $typeString")
      case RelationshipSourceLabelConstraints =>
        // Should not get here as they are always dependent, but lets have the cases anyway for security
        // and if we ever want to add them as independent constraints as well
        null
      case RelationshipTargetLabelConstraints =>
        // Should not get here as they are always dependent, but lets have the cases anyway for security
        // and if we ever want to add them as independent constraints as well
        null
      case NodeLabelExistenceConstraints =>
        // Should not get here as they are always dependent, but lets have the cases anyway for security
        // and if we ever want to add them as independent constraints as well
        null
      case _ => throw new IllegalArgumentException(
          s"Did not expect constraint type ${constraintType.prettyPrint} for constraint create command."
        )
    }
  }

  private def getConstraintType(
    internalConstraintType: schema.ConstraintType,
    entityType: EntityType,
    endpointType: Option[EndpointType],
    returnCypher5Values: Boolean
  ): ShowConstraintType = {
    (internalConstraintType, entityType) match {
      case (schema.ConstraintType.UNIQUE, EntityType.NODE) =>
        if (returnCypher5Values) NodeUniqueConstraints.cypher5 else NodeUniqueConstraints.cypher25
      case (schema.ConstraintType.UNIQUE, EntityType.RELATIONSHIP) =>
        if (returnCypher5Values) RelUniqueConstraints.cypher5 else RelUniqueConstraints.cypher25
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.NODE)         => NodeKeyConstraints
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.RELATIONSHIP) => RelKeyConstraints
      case (schema.ConstraintType.EXISTS, EntityType.NODE) =>
        if (returnCypher5Values) NodePropExistsConstraints.cypher5 else NodePropExistsConstraints.cypher25
      case (schema.ConstraintType.EXISTS, EntityType.RELATIONSHIP) =>
        if (returnCypher5Values) RelPropExistsConstraints.cypher5 else RelPropExistsConstraints.cypher25
      case (schema.ConstraintType.PROPERTY_TYPE, EntityType.NODE)         => NodePropTypeConstraints
      case (schema.ConstraintType.PROPERTY_TYPE, EntityType.RELATIONSHIP) => RelPropTypeConstraints
      case (schema.ConstraintType.RELATIONSHIP_ENDPOINT_LABEL, EntityType.RELATIONSHIP) if endpointType.isDefined =>
        endpointType.get match {
          case EndpointType.START => RelationshipSourceLabelConstraints
          case EndpointType.END   => RelationshipTargetLabelConstraints
        }
      case (schema.ConstraintType.NODE_LABEL_EXISTENCE, EntityType.NODE) => NodeLabelExistenceConstraints
      case _ => throw new IllegalStateException(
          s"Invalid constraint combination: ConstraintType $internalConstraintType, EntityType $entityType and EndpointType $endpointType."
        )
    }
  }
}
