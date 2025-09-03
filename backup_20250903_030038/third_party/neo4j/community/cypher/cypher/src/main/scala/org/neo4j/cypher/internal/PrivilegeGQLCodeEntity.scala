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

import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Label
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.ROLE_LABEL
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL

/*
  Represents those privilege entities for which GQL codes exist for these error scenarios:
  |           | already exists | doesn't exist |
  |-----------|----------------|---------------|
  | User      | 42N12          | 42N09         |
  | Role      | 42N13          | 42N10         |
  | Database  | 42N11          | 22N51         |

  This allows for compile time type checking
 */
sealed abstract class PrivilegeGQLCodeEntity(val label: Label) {
  override def toString: String = label.name()
}

object PrivilegeGQLCodeEntity {
  case class Role() extends PrivilegeGQLCodeEntity(ROLE_LABEL)
  case class User() extends PrivilegeGQLCodeEntity(USER_LABEL)
  case class Database() extends PrivilegeGQLCodeEntity(DATABASE_LABEL)

  def entityNotFoundGqlStatus(
    privilegeGQLCodeEntity: PrivilegeGQLCodeEntity,
    value: String
  ): ErrorGqlStatusObject = {
    privilegeGQLCodeEntity match {
      case PrivilegeGQLCodeEntity.User() =>
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N09)
          .withParam(GqlParams.StringParam.user, value)
          .build()
      case PrivilegeGQLCodeEntity.Role() =>
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N10)
          .withParam(GqlParams.StringParam.role, value)
          .build()
      case PrivilegeGQLCodeEntity.Database() =>
        ErrorGqlStatusObjectImplementation.from(
          GqlStatusInfoCodes.STATUS_42N00
        )
          .withParam(GqlParams.StringParam.db, value)
          .build()
    }
  }

  def entityAlreadyExistsGqlStatus(
    privilegeGQLCodeEntity: PrivilegeGQLCodeEntity,
    value: String
  ): ErrorGqlStatusObject = {
    privilegeGQLCodeEntity match {
      case PrivilegeGQLCodeEntity.User() =>
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N12)
          .withParam(GqlParams.StringParam.user, value)
          .build()
      case PrivilegeGQLCodeEntity.Role() =>
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N13)
          .withParam(GqlParams.StringParam.role, value)
          .build()
      case PrivilegeGQLCodeEntity.Database() =>
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N11)
          .withParam(GqlParams.StringParam.db, value)
          .build()

    }

  }

}
