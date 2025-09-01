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
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class AbsTest extends FunctionTestBase("abs") {

  test("shouldFailIfWrongArguments") {
    val dummy = 5
    testInvalidApplicationWithGql()("Insufficient parameters for function 'abs'")(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(DummyPosition(dummy).line, DummyPosition(dummy).column, DummyPosition(dummy).offset)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
          .atPosition(DummyPosition(dummy).line, DummyPosition(dummy).column, DummyPosition(dummy).offset)
          .withParam(GqlParams.NumberParam.count1, 1)
          .withParam(GqlParams.NumberParam.count2, 0)
          .withParam(GqlParams.StringParam.procFun, "abs")
          .withParam(GqlParams.StringParam.sig, "abs(input :: INTEGER | FLOAT) :: INTEGER | FLOAT")
          .build())
        .build()
    )
    testInvalidApplicationWithGql(CTFloat, CTFloat)("Too many parameters for function 'abs'")(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(DummyPosition(dummy).line, DummyPosition(dummy).column, DummyPosition(dummy).offset)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
          .atPosition(DummyPosition(dummy).line, DummyPosition(dummy).column, DummyPosition(dummy).offset)
          .withParam(GqlParams.NumberParam.count1, 1)
          .withParam(GqlParams.NumberParam.count2, 2)
          .withParam(GqlParams.StringParam.procFun, "abs")
          .withParam(GqlParams.StringParam.sig, "abs(input :: INTEGER | FLOAT) :: INTEGER | FLOAT")
          .build())
        .build()
    )
  }

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTInteger)(CTFloat | CTInteger)
    testValidTypes(CTFloat)(CTFloat | CTInteger)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTInteger)(CTFloat | CTInteger)
  }

  test("shouldReturnErrorIfInvalidArgumentTypes") {
    testInvalidApplication(CTNode)("Type mismatch: expected Float or Integer but was Node")
  }
}
