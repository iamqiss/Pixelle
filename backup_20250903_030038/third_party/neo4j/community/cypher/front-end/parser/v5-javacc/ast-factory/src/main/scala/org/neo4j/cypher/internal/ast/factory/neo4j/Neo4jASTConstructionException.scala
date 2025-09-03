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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlRuntimeException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus

class Neo4jASTConstructionException(errorGqlStatusObject: ErrorGqlStatusObject, msg: String)
    extends GqlRuntimeException(errorGqlStatusObject, msg) with HasStatus {
  def this(msg: String) = this(null, msg)
  override def status(): Status = Status.Statement.SyntaxError
  override def legacyMessage: String = msg
}

object Neo4jASTConstructionException {

  def apply(msg: String): Neo4jASTConstructionException = {
    new Neo4jASTConstructionException(null, msg)
  }

  def apply(gql: ErrorGqlStatusObject, legacyMessage: String): Neo4jASTConstructionException = {
    new Neo4jASTConstructionException(gql, legacyMessage)
  }

  def unsupportedIndexOrConstraint(constraintType: String, legacyMessage: String): Neo4jASTConstructionException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N16)
        .withParam(GqlParams.StringParam.idxType, constraintType)
        .build())
      .build()

    Neo4jASTConstructionException(gql, legacyMessage)
  }
}
