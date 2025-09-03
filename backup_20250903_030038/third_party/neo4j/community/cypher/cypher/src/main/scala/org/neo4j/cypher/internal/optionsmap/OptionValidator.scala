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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.dbms.systemgraph.allocation.DatabaseAllocationHints
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.StorageEngineFactory.allAvailableStorageEngines
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

import java.lang.Boolean.FALSE
import java.util.Locale
import java.util.UUID

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

sealed trait OptionValidator[T] {

  val KEY: String

  protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): T

  def findIn(optionsMap: MapValue, config: Option[Config])(implicit operation: String): Option[T] = {
    optionsMap
      .find(_._1.equalsIgnoreCase(KEY))
      .map(_._2)
      .flatMap {
        case _: NoValue => None
        case value      => Some(value)
      }
      .map(validate(_, config))
  }
}

trait MapOptionValidator extends OptionValidator[MapValue] {

  protected def validateContent(value: MapValue, config: Option[Config])(implicit operation: String): Unit

  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): MapValue = {
    value match {
      case mapValue: MapValue =>
        validateContent(mapValue, config)
        mapValue
      case _ =>
        throw new InvalidArgumentsException(s"Could not $operation with specified $KEY '$value', Map expected.")
    }
  }
}

trait StringOptionValidator extends OptionValidator[String] {

  protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit

  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String = {
    value match {
      case textValue: TextValue =>
        validateContent(textValue.stringValue(), config)
        textValue.stringValue()
      case _ =>
        val pp = new PrettyPrinter
        value.writeTo(pp)
        val gql = GqlHelper.getGql22G03_22N27(pp.value, KEY, java.util.List.of("STRING"))
        throw new InvalidArgumentsException(gql, s"Could not $operation with specified $KEY '$value', String expected.")
    }
  }
}

trait IntOptionValidator extends OptionValidator[Int] {

  protected def validateContent(value: Int, config: Option[Config])(implicit operation: String): Unit

  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): Int = {
    value match {
      case intValue: IntValue =>
        validateContent(intValue.intValue, config)
        intValue.intValue
      case _ =>
        throw new InvalidArgumentsException(s"Could not $operation with specified $KEY '$value', Integer expected.")
    }
  }
}

object ExistingDataOption extends StringOptionValidator {
  val KEY = "existingData"

  // possible options:
  val VALID_VALUE = "use"

  // override to keep legacy behaviour. ExistingDataOption is parsed to lowercase, other options keep input casing.
  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String =
    super.validate(value, config).toLowerCase(Locale.ROOT)

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    if (!value.equalsIgnoreCase(VALID_VALUE)) {
      throw InvalidArgumentsException.unrecognisedOptionGivenValue(operation, value, KEY, VALID_VALUE, true)
    }
  }
}

object ExistingSeedInstanceOption extends StringOptionValidator {
  override val KEY: String = "existingDataSeedInstance"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit =
    try {
      UUID.fromString(value)
    } catch {
      case _: IllegalArgumentException =>
        throw InvalidArgumentsException.unrecognisedOptionGivenValue(operation, value, KEY, "server uuid string", false)
    }
}

object ExistingSeedServerOption extends StringOptionValidator {
  override val KEY: String = "existingDataSeedServer"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit =
    try {
      UUID.fromString(value)
    } catch {
      case _: IllegalArgumentException =>
        throw InvalidArgumentsException.unrecognisedOptionGivenValue(operation, value, KEY, "server uuid string", false)
    }
}

object StoreFormatOption extends StringOptionValidator {
  override val KEY: String = "storeFormat"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    try {
      // Validate the format by looking for a storage engine that supports it - will throw if none was found
      val versionsUnderDev =
        config.fold(FALSE)(_.get(GraphDatabaseInternalSettings.include_versions_under_development))
      val selectEngineConfig = Config.newBuilder()
        .set(GraphDatabaseSettings.db_format, value)
        .set(GraphDatabaseInternalSettings.include_versions_under_development, versionsUnderDev)
        .build()
      StorageEngineFactory.selectStorageEngine(selectEngineConfig)
    } catch {
      case _: Exception =>
        val allFormatsList: java.util.List[String] = allAvailableStorageEngines().asScala
          .flatMap(sef => sef.supportedFormats(false).asScala.toSeq.sorted)
          .toSeq.distinct.asJava
        throw InvalidArgumentsException.invalidOptionFormat(operation, KEY, value, allFormatsList)
    }
  }
}

object SeedURIOption extends StringOptionValidator {
  override val KEY: String = "seedURI"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object SeedCredentialsOption extends StringOptionValidator {
  override val KEY: String = "seedCredentials"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object SeedConfigOption extends StringOptionValidator {
  override val KEY: String = "seedConfig"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object ExistingMetadataOption extends StringOptionValidator {
  val KEY = "existingMetadata"

  // possible options:
  val VALID_VALUE = "use"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    if (!value.equalsIgnoreCase(VALID_VALUE)) {
      throw InvalidArgumentsException.unrecognisedOptionGivenValue(operation, KEY, value, VALID_VALUE, true)
    }
  }
}

object LogEnrichmentOption extends StringOptionValidator {
  override val KEY: String = "txLogEnrichment"

  private val FULL: String = "FULL"
  private val DIFF: String = "DIFF"
  private val OFF: String = "OFF"
  private val validValues = Seq(FULL, DIFF, OFF)

  // override to normalize to uppercase.
  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String =
    super.validate(value, config).toUpperCase(Locale.ROOT)

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    if (!validValues.exists(value.equalsIgnoreCase)) {
      throw InvalidArgumentsException.unrecognisedOptionGivenValue(operation, KEY, value, validValues.asJava)
    }
  }

}

object AllocationHintsOption extends MapOptionValidator {
  override val KEY: String = "allocationHints"

  override protected def validateContent(value: MapValue, config: Option[Config])(implicit operation: String): Unit = {
    value.foreach((k, v) => {
      try {
        DatabaseAllocationHints.validate(k, v)
      } catch {
        case e: IllegalArgumentException => // TODO should not this have gql-code too?
          throw new InvalidArgumentsException(s"Could not $operation with specified $KEY '$value'. ${e.getMessage}'")
      }
    })
  }
}
