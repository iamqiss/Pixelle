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
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.cypher.internal.util.DeprecatedIndexProviderOption
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.graphdb.schema.IndexSetting
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_EF_CONSTRUCTION
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_M
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_QUANTIZATION_ENABLED
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.lang.String.CASE_INSENSITIVE_ORDER

import scala.collection.immutable.SortedSet
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.math.Ordering.comparatorToOrdering

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  protected def context: IndexProviderContext

  protected def getOptionsParts(
    options: MapValue,
    schemaType: String,
    indexType: IndexType,
    version: CypherVersion
  ): (Option[IndexProviderDescriptor], IndexConfig, Set[InternalNotification]) = {
    var optionName = ""
    if (
      options.exists { case (k, _) =>
        optionName = k
        !optionName.equalsIgnoreCase("indexProvider") && !optionName.equalsIgnoreCase("indexConfig")
      }
    ) {
      throw InvalidArgumentsException.invalidIndexOptionValue(optionName, schemaType)
    }
    val maybeIndexProvider = options.getOption("indexprovider")
    // If there are mandatory options we should call convert with empty options to throw expected errors
    val maybeConfig = options.getOption("indexconfig").orElse(Option.when(hasMandatoryOptions)(VirtualValues.EMPTY_MAP))

    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, schemaType, indexType, version))
    val indexConfig =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType, indexProvider)).getOrElse(IndexConfig.empty)
    if (indexProvider.nonEmpty) {
      (indexProvider, indexConfig, Set(DeprecatedIndexProviderOption()))
    } else {
      (indexProvider, indexConfig, Set())
    }
  }

  protected def toIndexConfig: java.util.Map[String, Object] => IndexConfig =
    IndexSettingUtil.toIndexConfigFromStringObjectMap

  protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): IndexConfig

  private def assertValidIndexProvider(
    indexProvider: AnyValue,
    schemaType: String,
    indexType: IndexType,
    version: CypherVersion
  ): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      context.validateIndexProvider(schemaType, indexProviderValue.stringValue(), indexType, version)
    case _ =>
      val pp = new PrettyPrinter
      indexProvider.writeTo(pp)
      val gql = GqlHelper.getGql22G03_22N27(
        pp.value,
        GqlParams.StringParam.cmd.process("indexProvider"),
        java.util.List.of("STRING")
      )
      throw new InvalidArgumentsException(
        gql,
        s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value."
      )
  }

  protected val validPointConfigSettingNames: SortedSet[String] = indexSettingsToCaseInsensitiveNames(
    SPATIAL_CARTESIAN_MIN,
    SPATIAL_CARTESIAN_MAX,
    SPATIAL_CARTESIAN_3D_MIN,
    SPATIAL_CARTESIAN_3D_MAX,
    SPATIAL_WGS84_MIN,
    SPATIAL_WGS84_MAX,
    SPATIAL_WGS84_3D_MIN,
    SPATIAL_WGS84_3D_MAX
  )

  def getValidConfigNames(idxType: IndexType): java.util.List[String] = {
    idxType match {
      case IndexType.FULLTEXT => validFulltextConfigSettingNames.toList.asJava
      case IndexType.VECTOR   => validVectorConfigSettingNames.toList.asJava
      case IndexType.POINT    => validPointConfigSettingNames.toList.asJava
      case _ => java.util.List.of("no values") // this should not happen if the method is called correctly
    }
  }

  protected def checkForPointConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validPointConfigSettingNames.contains(p) }) {
      foundPointConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  protected def foundPointConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    throw InvalidArgumentsException.pointOptionsInConfig(pp, itemsMap, schemaType, getValidConfigNames(originIndexType))
  }

  protected val validFulltextConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(FULLTEXT_ANALYZER, FULLTEXT_EVENTUALLY_CONSISTENT)

  protected def checkForFulltextConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validFulltextConfigSettingNames.contains(p) }) {
      foundFulltextConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  protected def foundFulltextConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    val prettyVal = new PrettyPrinter()
    itemsMap.writeTo(prettyVal)
    throw InvalidArgumentsException.fulltextOptionsInConfig(
      pp,
      itemsMap,
      schemaType,
      getValidConfigNames(originIndexType)
    )
  }

  private val validVectorConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(
      VECTOR_DIMENSIONS,
      VECTOR_SIMILARITY_FUNCTION,
      VECTOR_QUANTIZATION_ENABLED,
      VECTOR_HNSW_M,
      VECTOR_HNSW_EF_CONSTRUCTION
    )

  protected def checkForVectorConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validVectorConfigSettingNames.contains(p) }) {

      foundVectorConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  private def foundVectorConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    throw InvalidArgumentsException.vectorOptionsInConfig(
      pp,
      itemsMap,
      schemaType,
      getValidConfigNames(originIndexType)
    )
  }

  protected def invalidConfigValueString(pp: PrettyPrinter, value: AnyValue, schemaType: String): String = {
    value.writeTo(pp)
    invalidConfigValueString(pp.value(), schemaType)
  }

  protected def invalidConfigValueString(value: String, schemaType: String): String =
    s"Could not create $schemaType with specified index config '$value'"

  protected def assertEmptyConfig(
    config: AnyValue,
    schemaType: String,
    indexType: IndexType
  ): IndexConfig = {
    // no available config settings, throw nice error when existing config settings for other index types

    val indexString = indexType match {
      case IndexType.TEXT   => "text"
      case IndexType.LOOKUP => "lookup"
      case IndexType.RANGE  => "range"
      case _                => indexType.toString.toLowerCase
    }

    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue if !itemsMap.isEmpty =>
        checkForFulltextConfigValues(
          null,
          pp,
          itemsMap,
          schemaType
        )
        checkForPointConfigValues(indexType, pp, itemsMap, schemaType)
        checkForVectorConfigValues(indexType, pp, itemsMap, schemaType)

        itemsMap.writeTo(pp)
        throw InvalidArgumentsException.invalidIndexConfig(schemaType, pp.value(), indexString)
      case _: MapValue => IndexConfig.empty
      case unknown =>
        unknown.writeTo(pp)
        val gql = GqlHelper.getGql22G03_22N27(
          pp.value,
          GqlParams.StringParam.cmd.process("indexConfig"),
          java.util.List.of("MAP")
        )
        throw new InvalidArgumentsException(
          gql,
          s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map."
        )
    }
  }

  private def indexSettingsToCaseInsensitiveNames(settings: IndexSetting*): SortedSet[String] =
    SortedSet.from(settings.iterator.map(_.getSettingName))(comparatorToOrdering(CASE_INSENSITIVE_ORDER))
}

case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])

case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateWithNoOptions()
