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
package org.neo4j.cypher.internal

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.CreateConstraintType
import org.neo4j.cypher.internal.ast.NodeKey
import org.neo4j.cypher.internal.ast.NodePropertyExistence
import org.neo4j.cypher.internal.ast.NodePropertyType
import org.neo4j.cypher.internal.ast.NodePropertyUniqueness
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.RelationshipKey
import org.neo4j.cypher.internal.ast.RelationshipPropertyExistence
import org.neo4j.cypher.internal.ast.RelationshipPropertyType
import org.neo4j.cypher.internal.ast.RelationshipPropertyUniqueness
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.optionsmap.CreateFulltextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateIndexProviderOnlyOptions
import org.neo4j.cypher.internal.optionsmap.CreateIndexWithFullOptions
import org.neo4j.cypher.internal.optionsmap.CreateLookupIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreatePointIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateRangeIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateTextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateVectorIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.IndexBackedConstraintsOptionsConverter
import org.neo4j.cypher.internal.optionsmap.Nothing
import org.neo4j.cypher.internal.optionsmap.ParsedOptions
import org.neo4j.cypher.internal.optionsmap.ParsedWithNotifications
import org.neo4j.cypher.internal.optionsmap.PropertyExistenceOrTypeConstraintOptionsConverter
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.getPrettyStringName
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.prettyOptions
import org.neo4j.cypher.internal.plandescription.PrettyString
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.PropertyTypeMapper
import org.neo4j.cypher.internal.procs.SchemaExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.util.IndexOrConstraintAlreadyExistsNotification
import org.neo4j.cypher.internal.util.IndexOrConstraintDoesNotExistNotification
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.gqlstatus.GqlHelper.getGql22G03_22N27
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.graphdb.schema.IndexType.POINT
import org.neo4j.graphdb.schema.IndexType.RANGE
import org.neo4j.graphdb.schema.IndexType.TEXT
import org.neo4j.graphdb.schema.IndexType.VECTOR
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.ConstraintType.EXISTS
import org.neo4j.internal.schema.ConstraintType.NODE_LABEL_EXISTENCE
import org.neo4j.internal.schema.ConstraintType.PROPERTY_TYPE
import org.neo4j.internal.schema.ConstraintType.RELATIONSHIP_ENDPOINT_LABEL
import org.neo4j.internal.schema.ConstraintType.UNIQUE
import org.neo4j.internal.schema.ConstraintType.UNIQUE_EXISTS
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode
import org.neo4j.values.storable.StringValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

import scala.language.implicitConversions
import scala.util.Try

/**
 * This runtime takes on queries that require no planning such as schema commands
 */
object SchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "schema"

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  override def compileToExecutable(
    state: LogicalQuery,
    context: RuntimeContext,
    databaseMode: DatabaseMode
  ): ExecutionPlan = {
    logicalToExecutable.applyOrElse(state.logicalPlan, throwCantCompile).apply(context)
  }

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw CantCompileQueryException.planNotSchemaCommand(unknownPlan.getClass.getSimpleName)
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      Some(SCHEMA_WRITE)
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, RuntimeContext => ExecutionPlan] = {
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
    case CreateConstraint(source, _: NodeKey, label: LabelName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodeKeyConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val (maybeIndexProvider, notifications) =
              IndexBackedConstraintsOptionsConverter("node key constraint", indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val indexProvider = maybeIndexProvider.flatMap(_.provider)
            val labelId = ctx.getOrCreateLabelId(label.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createNodeKeyConstraint(labelId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS RELATIONSHIP KEY [OPTIONS {...}]
    case CreateConstraint(source, _: RelationshipKey, relType: RelTypeName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateRelationshipKeyConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val (maybeIndexProvider, notifications) =
              IndexBackedConstraintsOptionsConverter("relationship key constraint", indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val indexProvider = maybeIndexProvider.flatMap(_.provider)
            val relId = ctx.getOrCreateRelTypeId(relType.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createRelationshipKeyConstraint(relId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateConstraint(source, NodePropertyUniqueness, label: LabelName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyUniquenessConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val (maybeIndexProvider, notifications) =
              IndexBackedConstraintsOptionsConverter("uniqueness constraint", indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val indexProvider = maybeIndexProvider.flatMap(_.provider)
            val labelId = ctx.getOrCreateLabelId(label.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createNodeUniqueConstraint(labelId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE rel.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateConstraint(source, RelationshipPropertyUniqueness, relType: RelTypeName, props, name, options) =>
      context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyUniquenessConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val (maybeIndexProvider, notifications) =
              IndexBackedConstraintsOptionsConverter("relationship uniqueness constraint", indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val indexProvider = maybeIndexProvider.flatMap(_.provider)
            val relTypeId = ctx.getOrCreateRelTypeId(relType.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createRelationshipUniqueConstraint(relTypeId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS NOT NULL
    case CreateConstraint(source, NodePropertyExistence, label: LabelName, prop, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyExistenceConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", indexContext(ctx))
              .convert(context.cypherVersion, options, params)
            (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(
              label,
              prop.head.propertyKey,
              constraintName
            ))
            SuccessResult()
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL
    case CreateConstraint(source, RelationshipPropertyExistence, relType: RelTypeName, prop, name, options) =>
      context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyExistenceConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", indexContext(ctx))
              .convert(context.cypherVersion, options, params)
            (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(
              relType,
              prop.head.propertyKey,
              constraintName
            ))
            SuccessResult()
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS TYPED ...
    case CreateConstraint(source, NodePropertyType(propertyType), label: LabelName, prop, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyTypeConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", indexContext(ctx))
              .convert(context.cypherVersion, options, params)
            val (labelId, propId, _) = labelPropWithName(ctx)(label, prop.head.propertyKey, constraintName)
            ctx.createNodePropertyTypeConstraint(
              labelId,
              propId,
              PropertyTypeMapper.asPropertyTypeSet(propertyType),
              constraintName
            )
            SuccessResult()
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS TYPED ...
    case CreateConstraint(source, RelationshipPropertyType(propertyType), relType: RelTypeName, prop, name, options) =>
      context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyTypeConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", indexContext(ctx))
              .convert(context.cypherVersion, options, params)
            val (relTypeId, propId, _) = typePropWithName(ctx)(relType, prop.head.propertyKey, constraintName)
            ctx.createRelationshipPropertyTypeConstraint(
              relTypeId,
              propId,
              PropertyTypeMapper.asPropertyTypeSet(propertyType),
              constraintName
            )
            SuccessResult()
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // DROP CONSTRAINT name [IF EXISTS]
    case DropConstraintOnName(name, ifExists) => _ =>
        SchemaExecutionPlan(
          "DropConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val notifications: Set[InternalNotification] = if (!ifExists || ctx.constraintExists(constraintName)) {
              ctx.dropNamedConstraint(constraintName)
              Set.empty
            } else {
              // Notify on non-existing constraint, replace potential parameter names with their actual value
              Set(IndexOrConstraintDoesNotExistNotification(
                s"DROP CONSTRAINT ${Prettifier.escapeName(Left(constraintName))} IF EXISTS",
                constraintName
              ))
            }
            SuccessResult(notifications)
          }
        )

      // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, RANGE, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val schemaType = entityType match {
              case EntityType.NODE         => "range node property index"
              case EntityType.RELATIONSHIP => "range relationship property index"
            }
            val (maybeProvider, notifications) =
              CreateRangeIndexOptionsConverter(schemaType, indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val provider = maybeProvider.flatMap(_.provider)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addRangeIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR (n) ON EACH labels(n)
    // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR ()-[r]-() ON [EACH] type(r)
    case CreateLookupIndex(source, entityType, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (maybeProvider, notifications) = CreateLookupIndexOptionsConverter(indexContext(ctx))
              .convert(context.cypherVersion, options, params)
              .toOptionNotification
            val provider = maybeProvider.flatMap(_.provider)
            ctx.addLookupIndexRule(entityType, indexName, provider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON EACH (n.prop) [OPTIONS {...}]
    // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON EACH (n.prop) [OPTIONS {...}]
    case CreateFulltextIndex(source, entityNames, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (indexProvider, indexConfig, notifications) =
              CreateFulltextIndexOptionsConverter(indexContext(ctx))
                .convert(context.cypherVersion, options, params) match {
                case Nothing => (None, IndexConfig.empty(), Set.empty[InternalNotification])
                case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
                  (provider, config, Set.empty[InternalNotification])
                case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
                  (provider, config, notifications)
              }
            val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, TEXT, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (maybeProvider: Option[CreateIndexProviderOnlyOptions], notifications) =
              CreateTextIndexOptionsConverter(indexContext(ctx))
                .convert(context.cypherVersion, options, params)
                .toOptionNotification
            val provider = maybeProvider.flatMap(_.provider)
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addTextIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, POINT, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (indexProvider, indexConfig, notifications) =
              CreatePointIndexOptionsConverter(indexContext(ctx))
                .convert(context.cypherVersion, options, params) match {
                case Nothing => (None, IndexConfig.empty(), Set.empty[InternalNotification])
                case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
                  (provider, config, Set.empty[InternalNotification])
                case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
                  (provider, config, notifications)
              }
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addPointIndexRule(entityId, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) OPTIONS {...}
    // CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) OPTIONS {...}
    case CreateIndex(source, VECTOR, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (indexProvider, indexConfig, notifications) =
              CreateVectorIndexOptionsConverter(indexContext(ctx), vectorIndexVersion(ctx))
                .convert(context.cypherVersion, options, params) match {
                case Nothing =>
                  (None, IndexConfig.empty(), Set.empty[InternalNotification])
                case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
                  (provider, config, Set.empty[InternalNotification])
                case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
                  (provider, config, notifications)
              }
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addVectorIndexRule(entityId, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult(notifications)
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // DROP INDEX name [IF EXISTS]
    case DropIndexOnName(name, ifExists) => _ =>
        SchemaExecutionPlan(
          "DropIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val notifications: Set[InternalNotification] = if (!ifExists || ctx.indexExists(indexName)) {
              ctx.dropIndexRule(indexName)
              Set.empty
            } else {
              // Notify on non-existing index, replace potential parameter names with their actual value
              Set(IndexOrConstraintDoesNotExistNotification(
                s"DROP INDEX ${Prettifier.escapeName(Left(indexName))} IF EXISTS",
                indexName
              ))
            }
            SuccessResult(notifications)
          }
        )

    case DoNothingIfExistsForIndex(entityName, propertyKeyNames, indexType, name, options) => context =>
        val (innerIndexType, optionsConverter) = indexType match {
          case POINT => (IndexType.POINT, CreatePointIndexOptionsConverter)
          case RANGE => (
              IndexType.RANGE,
              (ctx: QueryContext) =>
                CreateRangeIndexOptionsConverter("range index", indexContext(ctx))
            )
          case TEXT => (IndexType.TEXT, CreateTextIndexOptionsConverter)
          case VECTOR => (
              IndexType.VECTOR,
              (ctx: QueryContext) =>
                CreateVectorIndexOptionsConverter(indexContext(ctx), vectorIndexVersion(ctx))
            )
          case it =>
            throw new IllegalStateException(
              s"Did not expect index type $it here: only point, range, text or vector indexes."
            )
        }
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            val optionConverterNotifications = optionsConverter(ctx)
              .convert(context.cypherVersion, options, params)
              .toOptionNotification
              ._2

            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
            val existingIndexDescriptor =
              Try(ctx.indexReference(innerIndexType, entityId, entityType, propertyKeyIds: _*))
            if (existingIndexDescriptor.isSuccess) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = indexInfo(indexType.name(), indexName, entityName, propertyKeyNames, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else if (indexName.exists(ctx.indexExists)) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = indexInfo(indexType.name(), indexName, entityName, propertyKeyNames, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else {
              SuccessResult(optionConverterNotifications)
            }
          },
          None
        )

    case DoNothingIfExistsForLookupIndex(entityType, name, options) => context =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            val optionConverterNotifications = CreateLookupIndexOptionsConverter(ctx)
              .convert(context.cypherVersion, options, params)
              .toOptionNotification
              ._2

            val existingIndexDescriptor = Try(ctx.lookupIndexReference(entityType))
            if (existingIndexDescriptor.isSuccess) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = lookupIndexInfo(indexName, entityType, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else if (indexName.exists(ctx.indexExists)) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = lookupIndexInfo(indexName, entityType, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else {
              SuccessResult(optionConverterNotifications)
            }
          },
          None
        )

    case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, name, options) => context =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            val optionConverterNotifications = CreateFulltextIndexOptionsConverter(ctx)
              .convert(context.cypherVersion, options, params)
              .toOptionNotification
              ._2

            val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
            val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
            val existingIndexDescriptor = Try(ctx.fulltextIndexReference(entityIds, entityType, propertyKeyIds: _*))
            if (existingIndexDescriptor.isSuccess) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = fulltextIndexInfo(indexName, entityNames, propertyKeyNames, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else if (indexName.exists(ctx.indexExists)) {
              // Notify on pre-existing index, replace potential parameter names with their actual value
              val indexDescription = fulltextIndexInfo(indexName, entityNames, propertyKeyNames, options)
              val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $indexDescription",
                conflictingIndex
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else {
              SuccessResult(optionConverterNotifications)
            }
          },
          None
        )

    case DoNothingIfExistsForConstraint(entityName, props, assertion, name, options) => context =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert correct options to get errors even if matching constraint already exists
            val conversionResult = assertion match {
              case _: NodeKey =>
                IndexBackedConstraintsOptionsConverter("node key constraint", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case _: RelationshipKey =>
                IndexBackedConstraintsOptionsConverter("relationship key constraint", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case NodePropertyUniqueness =>
                IndexBackedConstraintsOptionsConverter("uniqueness constraint", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case RelationshipPropertyUniqueness =>
                IndexBackedConstraintsOptionsConverter("relationship uniqueness constraint", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case NodePropertyExistence =>
                PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case RelationshipPropertyExistence =>
                PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case _: NodePropertyType =>
                PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
              case _: RelationshipPropertyType =>
                PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", indexContext(ctx))
                  .convert(context.cypherVersion, options, params)
            }

            val optionConverterNotifications = conversionResult.toOptionNotification._2

            val (entityId, _) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            val constraintMatcher = convertConstraintTypeToConstraintMatcher(assertion)
            if (ctx.constraintExists(constraintMatcher, entityId, propertyKeyIds: _*)) {
              // Notify on pre-existing constraint, replace potential parameter names with their actual value
              val constraintDescription = constraintInfo(constraintName, entityName, props, assertion, options)
              val conflictingConstraint = existingConstraintInfo(
                ctx,
                () => ctx.getConstraintInformation(constraintMatcher, entityId, propertyKeyIds: _*),
                context.cypherVersion
              )

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $constraintDescription",
                conflictingConstraint
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else if (constraintName.exists(ctx.constraintExists)) {
              // Notify on pre-existing constraint, replace potential parameter names with their actual value
              val constraintDescription = constraintInfo(constraintName, entityName, props, assertion, options)
              val conflictingConstraint = existingConstraintInfo(
                ctx,
                () => ctx.getConstraintInformation(constraintName.get),
                context.cypherVersion
              )

              val notification = IndexOrConstraintAlreadyExistsNotification(
                s"CREATE $constraintDescription",
                conflictingConstraint
              )
              IgnoredResult(Set(notification) ++ optionConverterNotifications)
            } else {
              SuccessResult(optionConverterNotifications)
            }
          },
          None
        )
  }

  private def getName(name: Option[Either[String, Parameter]], params: MapValue): Option[String] =
    name.map(getName(_, params))

  private def getName(name: Either[String, Parameter], params: MapValue): String = name match {
    case Left(stringName) => stringName
    case Right(paramName) =>
      params.get(paramName.name) match {
        case s: StringValue => s.stringValue()
        case x =>
          val pp = new PrettyPrinter
          x.writeTo(pp)
          val gql =
            getGql22G03_22N27(
              pp.value,
              GqlParams.StringParam.param.process(paramName.name),
              java.util.List.of("STRING")
            )
          throw new ParameterWrongTypeException(gql, s"Expected String, but got: ${x.getTypeName}")
      }
  }

  private def getEntityInfo(entityName: ElementTypeName, ctx: QueryContext) = entityName match {
    // returns (entityId, EntityType)
    case label: LabelName     => (ctx.getOrCreateLabelId(label.name), EntityType.NODE)
    case relType: RelTypeName => (ctx.getOrCreateRelTypeId(relType.name), EntityType.RELATIONSHIP)
    case _: DynamicLabelExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Labels here"
      )
    case _: DynamicRelTypeExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Relationships here"
      )
  }

  private def getMultipleEntityInfo(entityName: Either[List[LabelName], List[RelTypeName]], ctx: QueryContext) =
    entityName match {
      // returns (entityIds, EntityType)
      case Left(labels)    => (labels.map(label => ctx.getOrCreateLabelId(label.name)), EntityType.NODE)
      case Right(relTypes) => (relTypes.map(relType => ctx.getOrCreateRelTypeId(relType.name)), EntityType.RELATIONSHIP)
    }

  def isApplicable(logicalPlanState: LogicalPlanState): Boolean =
    logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)

  private def convertConstraintTypeToConstraintMatcher(assertion: CreateConstraintType)
    : ConstraintDescriptor => Boolean =
    assertion match {
      case NodePropertyExistence          => c => c.isNodePropertyExistenceConstraint
      case RelationshipPropertyExistence  => c => c.isRelationshipPropertyExistenceConstraint
      case NodePropertyUniqueness         => c => c.isNodeUniquenessConstraint
      case RelationshipPropertyUniqueness => c => c.isRelationshipUniquenessConstraint
      case _: NodeKey                     => c => c.isNodeKeyConstraint
      case _: RelationshipKey             => c => c.isRelationshipKeyConstraint
      case NodePropertyType(propType) =>
        c => c.isNodePropertyTypeConstraint && checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
      case RelationshipPropertyType(propType) =>
        c =>
          c.isRelationshipPropertyTypeConstraint &&
            checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
    }

  // Checks if the pre-existing constraints property type (preExistingTypes)
  // is the same as the property type of the constraint to be created (askedForType)
  private def checkTypes(askedForType: CypherType, preExistingTypes: PropertyTypeSet): Boolean =
    preExistingTypes.equals(PropertyTypeMapper.asPropertyTypeSet(askedForType))

  implicit private def propertyToId(ctx: QueryContext)(property: PropertyKeyName): PropertyKeyId =
    PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name))

  private def labelPropWithName(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name), name)

  private def typePropWithName(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name), name)

  private def indexInfo(
    indexType: String,
    nameOption: Option[String],
    entityName: ElementTypeName,
    properties: Seq[PropertyKeyName],
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = getPrettyEntityPattern(entityName)
    val propertyString = getPrettyPropertyPattern(properties, "(", ")")
    pretty"${asPrettyString.raw(indexType)} INDEX$name IF NOT EXISTS FOR $pattern ON $propertyString${prettyOptions(options)}".prettifiedString
  }

  private def fulltextIndexInfo(
    nameOption: Option[String],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString("e:", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString("e:", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    val propertyString = getPrettyPropertyPattern(properties, "[", "]")
    pretty"FULLTEXT INDEX$name IF NOT EXISTS FOR $pattern ON EACH $propertyString${prettyOptions(options)}".prettifiedString
  }

  def lookupIndexInfo(
    nameOption: Option[String],
    entityType: EntityType,
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val (pattern, function) = getPrettyLookupIndexPatternAndFunction(entityType == EntityType.NODE)
    pretty"LOOKUP INDEX$name IF NOT EXISTS FOR $pattern ON EACH $function${prettyOptions(options)}".prettifiedString
  }

  private def existingIndexInfo(
    ctx: QueryContext,
    getInfoParts: () => IndexInformation
  ): String = {
    try {
      // Assert we are allowed to see the index description
      ctx.assertShowIndexAllowed()

      // Fetch the relevant index parts
      val IndexInformation(isNode, indexType, name, entityNames, properties) = getInfoParts()

      // Create string description
      val nameString = getPrettyName(Some(name))
      val (pattern, on) = indexType match {
        case IndexType.LOOKUP =>
          val (pattern, function) = getPrettyLookupIndexPatternAndFunction(isNode)
          (pattern, pretty"EACH $function")
        case IndexType.FULLTEXT =>
          val innerPattern = entityNames.map(e => asPrettyString(e)).mkPrettyString("e:", "|", "")
          val pattern = if (isNode) pretty"($innerPattern)" else pretty"()-[$innerPattern]-()"
          val propertyString = getPrettyPropertyPattern(properties, "[", "]")
          (pattern, pretty"EACH $propertyString")
        case _ =>
          // indexes have exactly one label/relType, unless FULLTEXT or LOOKUP which is handled above
          val pattern = getPrettyEntityPattern(isNode, entityNames.head)
          val propertyString = getPrettyPropertyPattern(properties, "(", ")")
          (pattern, propertyString)
      }
      pretty"${asPrettyString.raw(indexType.name())} INDEX$nameString FOR $pattern ON $on".prettifiedString
    } catch {
      // Not allowed to see index description, only show `index`
      case _: AuthorizationViolationException => "index"
    }
  }

  private def constraintInfo(
    nameOption: Option[String],
    entityName: ElementTypeName,
    properties: Seq[Property],
    constraintType: CreateConstraintType,
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = getPrettyEntityPattern(entityName)
    val propertyString = getPrettyPropertyPattern(properties.map(p => p.propertyKey), "(", ")")
    val prettyAssertion = asPrettyString.raw(constraintType.predicate)
    pretty"CONSTRAINT$name IF NOT EXISTS FOR $pattern REQUIRE $propertyString $prettyAssertion${prettyOptions(options)}".prettifiedString
  }

  private def existingConstraintInfo(
    ctx: QueryContext,
    getInfoParts: () => ConstraintInformation,
    cypherVersion: CypherVersion
  ): String = {
    try {
      // Assert we are allowed to see the constraint description
      ctx.assertShowConstraintAllowed()

      // Fetch the relevant constraint parts
      val ConstraintInformation(isNode, constraintType, name, entityName, properties, propertyType) = getInfoParts()

      // Create string description
      val nameString = getPrettyName(Some(name))
      val pattern = getPrettyEntityPattern(isNode, entityName)
      val propertyString = getPrettyPropertyPattern(properties, "(", ")")
      val assertion = constraintType match {
        case EXISTS => "IS NOT NULL"
        case UNIQUE_EXISTS =>
          if (cypherVersion == CypherVersion.Cypher5)
            if (isNode) "IS NODE KEY" else "IS RELATIONSHIP KEY"
          else "IS KEY"
        case UNIQUE                      => "IS UNIQUE"
        case PROPERTY_TYPE               => s"IS :: ${propertyType.get}"
        case RELATIONSHIP_ENDPOINT_LABEL => ""
        case NODE_LABEL_EXISTENCE        => ""
      }
      val prettyAssertion = asPrettyString.raw(assertion)
      // Currently don't have a constraint command for endpoint and node label existence constraints so let's return the same as if the user wasn't allowed to see the constraint for now
      // Once we have graph type commands, lets use `(:Label1 => :Label2)`, `(:Label)-[:REL_TYPE =>]->()` and `()-[:REL_TYPE =>]->(:Label)` with correct labels and relationship types
      // as they don't have constraint commands and that would be their representation in the graph type
      if (constraintType == RELATIONSHIP_ENDPOINT_LABEL || constraintType == NODE_LABEL_EXISTENCE) "constraint"
      else pretty"CONSTRAINT$nameString FOR $pattern REQUIRE $propertyString $prettyAssertion".prettifiedString
    } catch {
      // Not allowed to see constraint description, only show `constraint`
      case _: AuthorizationViolationException => "constraint"
    }
  }

  private def vectorIndexVersion(ctx: QueryContext): VectorIndexVersion =
    VectorIndexVersion.latestSupportedVersion(KernelVersion.getLatestVersion(ctx.getConfig))

  private def getPrettyName(nameOption: Option[String]): PrettyString =
    getPrettyStringName(nameOption.map(Left(_)))

  private def getPrettyEntityPattern(entityName: ElementTypeName): PrettyString = entityName match {
    case label: LabelName     => pretty"(e:${asPrettyString(label)})"
    case relType: RelTypeName => pretty"()-[e:${asPrettyString(relType)}]-()"
    case _: DynamicLabelExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Labels here"
      )
    case _: DynamicRelTypeExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Labels here"
      )
  }

  private def getPrettyEntityPattern(isNode: Boolean, entityName: String): PrettyString =
    if (isNode) {
      pretty"(e:${asPrettyString(entityName)})"
    } else {
      pretty"()-[e:${asPrettyString(entityName)}]-()"
    }

  private def getPrettyLookupIndexPatternAndFunction(isNode: Boolean): (PrettyString, PrettyString) =
    if (isNode) {
      (pretty"(e)", pretty"${asPrettyString.raw(Labels.name)}(e)")
    } else {
      (pretty"()-[e]-()", pretty"${asPrettyString.raw(Type.name)}(e)")
    }

  private def getPrettyPropertyPattern(properties: Seq[PropertyKeyName], start: String, end: String): PrettyString =
    properties.map(asPrettyString(_)).mkPrettyString(s"${start}e.", ", e.", end)

  private def getPrettyPropertyPattern(properties: List[String], start: String, end: String): PrettyString =
    properties.map(asPrettyString(_)).mkPrettyString(s"${start}e.", ", e.", end)

  private def indexContext(ctx: QueryContext): IndexProviderContext = ctx.asInstanceOf[IndexProviderContext]
}
