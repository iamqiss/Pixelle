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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.frontend.phases.DeprecationInfo
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureAccessMode
import org.neo4j.cypher.internal.frontend.phases.ProcedureDbmsAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSchemaWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage.toKernelScope
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.ProcedureHandle
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle
import org.neo4j.kernel.api
import org.neo4j.kernel.api.procedure.ProcedureView
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.procedure.Mode

import java.util.Optional

import scala.jdk.CollectionConverters.ListHasAsScala

trait ProcedureLookup {
  def function(name: procs.QualifiedName, scope: org.neo4j.kernel.api.QueryLanguage): UserFunctionHandle
  def procedure(name: procs.QualifiedName, scope: org.neo4j.kernel.api.QueryLanguage): ProcedureHandle
  def signatureVersion: Long
}

final class SignatureResolver(lookup: ProcedureLookup) extends ProcedureSignatureResolver {

  override def functionSignature(name: QualifiedName, scope: QueryLanguage): Option[UserFunctionSignature] =
    Option(lookup.function(SignatureResolver.asKernelQualifiedName(name), toKernelScope(scope)))
      .map(fcn => SignatureResolver.toCypherFunction(fcn))

  override def procedureSignature(name: QualifiedName, scope: QueryLanguage): ProcedureSignature = {
    val kn = new procs.QualifiedName(name.namespace.toArray, name.name)
    SignatureResolver.toCypherProcedure(lookup.procedure(kn, toKernelScope(scope)))
  }

  override def procedureSignatureVersion: Long = lookup.signatureVersion
}

object SignatureResolver {

  def from(p: Procedures): ProcedureSignatureResolver = new SignatureResolver(new ProcedureLookup {
    override def function(n: procs.QualifiedName, s: api.QueryLanguage): UserFunctionHandle = p.functionGet(n, s)
    override def procedure(n: procs.QualifiedName, s: api.QueryLanguage): ProcedureHandle = p.procedureGet(n, s)
    override def signatureVersion: Long = p.signatureVersion()
  })

  def from(p: ProcedureView): ProcedureSignatureResolver = new SignatureResolver(new ProcedureLookup {
    override def function(n: procs.QualifiedName, s: api.QueryLanguage): UserFunctionHandle = p.function(n, s)
    override def procedure(n: procs.QualifiedName, s: api.QueryLanguage): ProcedureHandle = p.procedure(n, s)
    override def signatureVersion: Long = p.signatureVersion()
  })

  def from(p: ProcedureView, v: CypherVersion): ScopedProcedureSignatureResolver =
    ScopedProcedureSignatureResolver.from(from(p), QueryLanguage.from(v.actualVersion))

  def toCypherProcedure(handle: ProcedureHandle): ProcedureSignature = {
    val signature = handle.signature()
    val deprecatedBy: Option[String] = signature.deprecated().asScala
    ProcedureSignature(
      name = asCypherQualifiedName(signature.name()),
      inputSignature = signature.inputSignature().asScala.toIndexedSeq.map(s =>
        FieldSignature(
          name = s.name(),
          typ = asCypherType(s.neo4jType()),
          default = s.defaultValue().asScala.map(asCypherValue),
          deprecated = s.isDeprecated,
          sensitive = s.isSensitive,
          description = s.getDescription
        )
      ),
      outputSignature =
        if (signature.isVoid)
          None
        else
          Some(signature.outputSignature().asScala.toIndexedSeq.map(s =>
            FieldSignature(
              name = s.name(),
              typ = asCypherType(s.neo4jType()),
              deprecated = s.isDeprecated,
              description = s.getDescription
            )
          )),
      deprecationInfo = Some(DeprecationInfo(signature.isDeprecated, deprecatedBy)),
      accessMode = {
        asCypherProcMode(signature.name().name(), signature.mode())
      },
      description = signature.description().asScala,
      warning = signature.warning().asScala,
      eager = signature.eager(),
      id = handle.id(),
      systemProcedure = signature.systemProcedure(),
      allowExpiredCredentials = signature.allowedExpiredCredentials(),
      threadSafe = signature.threadSafe()
    )
  }

  def toCypherFunction(fcn: UserFunctionHandle): UserFunctionSignature = {
    val signature = fcn.signature()
    val deprecatedBy: Option[String] = signature.deprecated().asScala
    UserFunctionSignature(
      name = asCypherQualifiedName(signature.name()),
      inputSignature = signature.inputSignature().asScala.toIndexedSeq.map(s =>
        FieldSignature(
          name = s.name(),
          typ = asCypherType(s.neo4jType()),
          default = s.defaultValue().asScala.map(asCypherValue),
          deprecated = s.isDeprecated,
          sensitive = s.isSensitive,
          description = s.getDescription
        )
      ),
      outputType = asCypherType(signature.outputType()),
      deprecationInfo = Some(DeprecationInfo(signature.isDeprecated, deprecatedBy)),
      description = signature.description().asScala,
      isAggregate = false,
      id = fcn.id(),
      fcn.signature().isBuiltIn,
      threadSafe = fcn.threadSafe()
    )
  }

  private def asKernelQualifiedName(name: QualifiedName): procs.QualifiedName =
    new procs.QualifiedName(name.namespace.toArray, name.name)

  private def asCypherQualifiedName(name: procs.QualifiedName): QualifiedName =
    QualifiedName(name.namespace().toSeq, name.name())

  private def asCypherValue(neo4jValue: DefaultParameterValue) = ValueUtils.of(neo4jValue.value())

  private def asCypherType(neoType: AnyType): CypherType = neoType match {
    case Neo4jTypes.NTString        => CTString
    case Neo4jTypes.NTInteger       => CTInteger
    case Neo4jTypes.NTFloat         => CTFloat
    case Neo4jTypes.NTNumber        => CTNumber
    case Neo4jTypes.NTBoolean       => CTBoolean
    case l: Neo4jTypes.ListType     => CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTByteArray     => CTList(CTAny)
    case Neo4jTypes.NTDateTime      => CTDateTime
    case Neo4jTypes.NTLocalDateTime => CTLocalDateTime
    case Neo4jTypes.NTDate          => CTDate
    case Neo4jTypes.NTTime          => CTTime
    case Neo4jTypes.NTLocalTime     => CTLocalTime
    case Neo4jTypes.NTDuration      => CTDuration
    case Neo4jTypes.NTPoint         => CTPoint
    case Neo4jTypes.NTNode          => CTNode
    case Neo4jTypes.NTRelationship  => CTRelationship
    case Neo4jTypes.NTPath          => CTPath
    case Neo4jTypes.NTGeometry      => CTGeometry
    case Neo4jTypes.NTMap           => CTMap
    case Neo4jTypes.NTAny           => CTAny
    case _ => throw new CypherExecutionException(
        "Unable to execute procedure, because the signature has an unrecognized type: " + neoType.toString,
        null
      )
  }

  private def asCypherProcMode(signature: String, mode: Mode): ProcedureAccessMode = mode match {
    case Mode.READ    => ProcedureReadOnlyAccess
    case Mode.DEFAULT => ProcedureReadOnlyAccess
    case Mode.WRITE   => ProcedureReadWriteAccess
    case Mode.SCHEMA  => ProcedureSchemaWriteAccess
    case Mode.DBMS    => ProcedureDbmsAccess

    case _ => throw CypherExecutionException.unrecognisedExecutionMode(signature, mode.name())
  }

  implicit private class OptionalOps[T](optional: Optional[T]) {

    def asScala: Option[T] =
      if (optional.isPresent) Some(optional.get()) else None
  }
}
