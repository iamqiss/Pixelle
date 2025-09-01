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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class SubqueryCallSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  private val pipelineWithUseAsMultipleGraphsSelector = pipelineWithSemanticFeatures(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsMultipleGraphsSelector
  )

  private val pipelineWithUseAsSingleGraphSelector = pipelineWithSemanticFeatures(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsSingleGraphSelector
  )

  test("Returning a variable that is already bound outside should give a useful error with scope") {
    val query =
      """WITH 1 AS i
        |CALL () {
        |  WITH 2 AS i
        |  RETURN i
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysisWithCypherVersion(CypherVersion.values(), query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `i` already declared in outer scope", 4, 10)
    ))
  }

  test("Returning a variable that is already bound outside should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN i
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysisWithCypherVersion(CypherVersion.values(), query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `i` already declared in outer scope", 4, 10)
    ))
  }

  test("Returning a variable that is already bound outside, from a union, should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN i
        |    UNION
        |  WITH 3 AS i
        |  RETURN 2 AS i
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `i` already declared in outer scope", 4, 10),
      ("Variable `i` already declared in outer scope", 7, 15)
    ))
  }

  test("Returning a variable implicitly that is already bound outside scoped call should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL () {
        |  WITH 2 AS i
        |  RETURN *
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysisWithCypherVersion(CypherVersion.values(), query)

    result.errors.map(e => (e.msg, e.position.line)) should equal(List(
      ("Variable `i` already declared in outer scope", 4)
    ))
  }

  test("Returning a variable implicitly that is already bound outside should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN *
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysisWithCypherVersion(CypherVersion.values(), query)

    result.errors.map(e => (e.msg, e.position.line)) should equal(List(
      ("Variable `i` already declared in outer scope", 4)
    ))
  }

  test("Returning a variable implicitly that is already bound outside, from a union, should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN *
        |    UNION
        |  WITH 3 AS i
        |  RETURN *
        |}
        |RETURN i
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line)) should equal(List(
      ("Variable `i` already declared in outer scope", 4),
      ("Variable `i` already declared in outer scope", 7)
    ))
  }

  test("Should warn about variable shadowing in a scoped subquery") {
    val query =
      """MATCH (shadowed)
        |CALL () {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(36, 3, 10), "shadowed")))
  }

  test("Should warn about variable shadowing in a subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(33, 3, 10), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about variable shadowing in a scoped subquery when aliasing") {
    val query =
      """MATCH (shadowed)
        |CALL () {
        |  MATCH (n)-[:REL]->(m)
        |  WITH m AS shadowed // warning here
        |  WITH shadowed AS m
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(63, 4, 13), "shadowed")))
  }

  test("Should warn about variable shadowing in a subquery when aliasing") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (n)-[:REL]->(m)
        |  WITH m AS shadowed // warning here
        |  WITH shadowed AS m
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(60, 4, 13), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about variable shadowing in a nested scoped subquery") {
    val query =
      """MATCH (shadowed)
        |CALL () {
        |  MATCH (n)-[:REL]->(m)
        |  CALL () {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(74, 5, 12), "shadowed")))
  }

  test("Should warn about variable shadowing in a nested subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (n)-[:REL]->(m)
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(68, 5, 12), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about variable shadowing from enclosing scoped subquery") {
    val query =
      """MATCH (shadowed)
        |CALL (shadowed) {
        |  MATCH (shadowed)-[:REL]->(m)
        |  CALL () {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(89, 5, 12), "shadowed")))
  }

  test("Should warn about variable shadowing from enclosing subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  WITH shadowed
        |  MATCH (shadowed)-[:REL]->(m)
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(91, 6, 12), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about multiple shadowed variables in a scoped subquery") {
    val query =
      """MATCH (shadowed)-->(alsoShadowed)
        |CALL () {
        |  MATCH (shadowed)-->(alsoShadowed) // multiple warnings here
        |  RETURN shadowed AS n, alsoShadowed AS m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(
        SubqueryVariableShadowing(InputPosition(53, 3, 10), "shadowed"),
        SubqueryVariableShadowing(InputPosition(66, 3, 23), "alsoShadowed")
      )
    )
  }

  test("Should warn about multiple shadowed variables in a subquery") {
    val query =
      """MATCH (shadowed)-->(alsoShadowed)
        |CALL {
        |  MATCH (shadowed)-->(alsoShadowed) // multiple warnings here
        |  RETURN shadowed AS n, alsoShadowed AS m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(
        SubqueryVariableShadowing(InputPosition(50, 3, 10), "shadowed"),
        SubqueryVariableShadowing(InputPosition(63, 3, 23), "alsoShadowed")
      ),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about multiple shadowed variables in a nested scoped subquery") {
    val query =
      """MATCH (shadowed)
        |CALL () {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  CALL () {
        |    MATCH (shadowed)-[:REL]->(x) // and also here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(
        SubqueryVariableShadowing(InputPosition(36, 3, 10), "shadowed"),
        SubqueryVariableShadowing(InputPosition(97, 5, 12), "shadowed")
      )
    )
  }

  test("Should warn about multiple shadowed variables in a nested subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // and also here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(
        SubqueryVariableShadowing(InputPosition(33, 3, 10), "shadowed"),
        SubqueryVariableShadowing(InputPosition(91, 5, 12), "shadowed")
      ),
      Seq(CypherVersion.Cypher5)
    )
  }

  test(
    "Should not warn about variable shadowing in a subquery if it has been removed from scope by WITH - scoped subquery"
  ) {
    val query =
      """MATCH (notShadowed)
        |WITH notShadowed AS n
        |CALL () {
        |  MATCH (notShadowed)-[:REL]->(m)
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set.empty)
  }

  test("Should not warn about variable shadowing in a subquery if it has been removed from scope by WITH") {
    val query =
      """MATCH (notShadowed)
        |WITH notShadowed AS n
        |CALL {
        |  MATCH (notShadowed)-[:REL]->(m)
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set.empty, Seq(CypherVersion.Cypher5))
  }

  test("Should not allow redeclaration of imported variable in a scoped subquery") {
    val query =
      """MATCH (notShadowed)
        |CALL (notShadowed) {
        |  MATCH (notShadowed)-[:REL]->(m)
        |  WITH m AS notShadowed
        |  RETURN notShadowed AS x
        |}
        |RETURN *""".stripMargin
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "The variable `notShadowed` is shadowing an imported variable with the same name and needs to be renamed",
        InputPosition(87, 4, 13)
      )),
      versions = CypherVersion.values()
    )
  }

  test("Should not warn about variable shadowing in a subquery if it has been imported previously") {
    val query =
      """MATCH (notShadowed)
        |CALL {
        |  WITH notShadowed
        |  MATCH (notShadowed)-[:REL]->(m)
        |  WITH m AS notShadowed
        |  RETURN notShadowed AS x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set.empty, Seq(CypherVersion.Cypher5))
  }

  test("Should warn about variable shadowing in an union scoped subquery") {
    val query =
      """MATCH (shadowed)
        |CALL () {
        |  MATCH (m) RETURN m
        | UNION
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(64, 5, 10), "shadowed")))
  }

  test("Should warn about variable shadowing in an union subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (m) RETURN m
        | UNION
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(61, 5, 10), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Should warn about variable shadowing in one of the union subquery branches") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  WITH shadowed
        |  MATCH (shadowed)-[:REL]->(m)
        |  RETURN m
        | UNION
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        | UNION
        |  MATCH (x) RETURN x AS m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(
      query,
      Set(SubqueryVariableShadowing(InputPosition(98, 7, 10), "shadowed")),
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Subquery with only importing WITH") {
    val query = "WITH 1 AS a CALL { WITH a } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 20, 19),
          "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
          InputPosition(19, 1, 20)
        )
      ),
      versions = Seq(CypherVersion.Cypher5)
    )
  }

  test("Scoped Subquery with only USE") {
    val query = "WITH 1 AS a CALL () { USE x } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 23, 22),
          "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
          InputPosition(22, 1, 23)
        )
      ),
      pipelineWithUseAsMultipleGraphsSelector
    )
  }

  test("Subquery with only USE") {
    val query = "WITH 1 AS a CALL { USE x } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 20, 19),
          "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
          InputPosition(19, 1, 20)
        )
      ),
      pipelineWithUseAsMultipleGraphsSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Subquery with only USE and importing WITH") {
    val query = "WITH 1 AS a CALL { USE x WITH a } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 20, 19),
          "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
          InputPosition(19, 1, 20)
        )
      ),
      pipelineWithUseAsMultipleGraphsSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test(
    "should allow Multiple USE referencing the same graph when UseAsSingleGraphSelector feature is set - scoped subquery"
  ) {
    val query =
      """
        |USE x
        |WITH 1 AS a
        |CALL () {
        |  USE x
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test("should allow Multiple USE referencing the same graph when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE x
        |WITH 1 AS a
        |CALL {
        |  USE x
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector, Seq(CypherVersion.Cypher5))
  }

  test(
    "should allow Multiple USE with qualified identifier referencing the same graph when UseAsSingleGraphSelector feature is set - scoped subquery"
  ) {
    val query =
      """
        |USE x.y.z
        |WITH 1 AS a
        |CALL () {
        |  USE x.y.z
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test(
    "should allow Multiple USE with qualified identifier referencing the same graph when UseAsSingleGraphSelector feature is set"
  ) {
    val query =
      """
        |USE x.y.z
        |WITH 1 AS a
        |CALL {
        |  USE x.y.z
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector, Seq(CypherVersion.Cypher5))
  }

  test(
    "should not allow Multiple USE referencing different graphs when UseAsSingleGraphSelector feature is set - scoped subquery"
  ) {
    val query =
      """
        |USE x
        |WITH 1 AS a
        |CALL () {
        |  USE y
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(5, 3, 31)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA5)
              .atPosition(5, 3, 31)
              .build())
            .build(),
          messageProvider.createMultipleGraphReferencesError("y"),
          InputPosition(31, 5, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should not allow Multiple USE referencing different graphs when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE x
        |WITH 1 AS a
        |CALL {
        |  USE y
        |  RETURN 2 AS b
        |}
        |RETURN *
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(5, 3, 28)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA5)
              .atPosition(5, 3, 28)
              .build())
            .build(),
          messageProvider.createMultipleGraphReferencesError("y"),
          InputPosition(28, 5, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Allow view invocation in USE when UseAsMultipleGraphsSelector feature is set in scoped subquery") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL (g, k) {
        |  USE graph.byName(g, w(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Allow view invocation in USE when UseAsMultipleGraphsSelector feature is set") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL {
        |  USE graph.byName(g, w(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector, Seq(CypherVersion.Cypher5))
  }

  test("Don't allow view invocation in USE when UseAsSingleGraphSelector feature is set in scoped subquery") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL (g, k) {
        |  USE graph.byName(g, w(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createDynamicGraphReferenceUnsupportedError("graph.byName(g, w(k))"),
          InputPosition(37, 4, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("Don't allow view invocation in USE when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL {
        |  USE graph.byName(g, w(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createDynamicGraphReferenceUnsupportedError("graph.byName(g, w(k))"),
          InputPosition(30, 4, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Allow qualified view invocation in USE in scoped subquery") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL (g, k){
        |  USE graph.byName(g, x.g(), x.v(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Allow qualified view invocation in USE") {
    val query =
      """
        |WITH 1 AS g, 2 AS k
        |CALL {
        |  USE graph.byName(g, x.g(), x.v(k))
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector, Seq(CypherVersion.Cypher5))
  }

  test("Allow expressions in view invocations (with feature flag) in scoped subquery") {
    val query =
      """
        |WITH 1 AS x
        |CALL (x) {
        |  USE graph.byName(2, 'x', x, x+3)
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Allow expressions in view invocations (with feature flag)") {
    val query =
      """
        |WITH 1 AS x
        |CALL {
        |  USE graph.byName(2, 'x', x, x+3)
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector, Seq(CypherVersion.Cypher5))
  }

  test("Expressions in view invocations are checked (with feature flag) in scoped subquery") {
    val query =
      """
        |WITH 1 AS x
        |CALL () {
        |  USE graph.byName(2, 'x', y, x+3)
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `y` not defined", InputPosition(50, 4, 28)),
        SemanticError("Variable `x` not defined", InputPosition(53, 4, 31))
      ),
      pipelineWithUseAsMultipleGraphsSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("Expressions in view invocations are checked (with feature flag)") {
    val query =
      """
        |WITH 1 AS x
        |CALL {
        |  USE graph.byName(2, 'x', y, x+3)
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(SemanticError("Variable `y` not defined", InputPosition(47, 4, 28))),
      pipelineWithUseAsMultipleGraphsSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("should allow USE only in leading sub-query position in scoped subquery") {
    val query =
      """
        |WITH 1 AS x
        |CALL () {
        |  MATCH (n)
        |  USE g
        |  RETURN n
        |}
        |RETURN n
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          SemanticError.invalidPlacementOfUseClause(
            InputPosition(37, 5, 3)
          ).gqlStatusObject,
          "USE clause must be the first clause in a (sub-)query.",
          InputPosition(37, 5, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should allow USE only in leading sub-query position") {
    val query =
      """
        |WITH 1 AS x
        |CALL {
        |  MATCH (n)
        |  USE g
        |  RETURN n
        |}
        |RETURN n
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
          InputPosition(34, 5, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("should not allow non-importing WITH before USE in sub-query position") {
    val query =
      """
        |WITH 1 AS x
        |CALL {
        |  WITH 1 AS y
        |  USE g
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
          InputPosition(36, 5, 3)
        )
      ),
      pipelineWithUseAsSingleGraphSelector,
      Seq(CypherVersion.Cypher5)
    )
  }

  test("should allow importing WITH before USE in sub-query position") {
    val query =
      """
        |WITH 1 AS x
        |CALL {
        |  WITH x
        |  USE g
        |  RETURN 1 AS a
        |}
        |RETURN a
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector, Seq(CypherVersion.Cypher5))
  }

  test("Scoped Subquery with only MATCH") {
    val query = "WITH 1 AS a CALL () { MATCH (n) } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 23, 22),
          "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
          InputPosition(22, 1, 23)
        )
      )
    )
  }

  test("Subquery with only MATCH") {
    val query = "WITH 1 AS a CALL { MATCH (n) } RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          GqlHelper.getGql42001_42N71(1, 20, 19),
          "Query cannot conclude with MATCH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
          InputPosition(19, 1, 20)
        )
      ),
      versions = Seq(CypherVersion.Cypher5)
    )
  }

  // Utilities for the following tests
  private val returnStarNMCombinations = Seq(
    "n",
    "m",
    "n, m",
    "*",
    "*, n",
    "*, m",
    "*, n, m"
  )

  // Utilities for the following tests
  private val returnStarOrderedNMCombinations = Seq(
    "n",
    "m",
    "n, m",
    "*"
  )

  private def containedVariables(returnStarNMCombination: String): Set[String] = {
    val strings = returnStarNMCombination.split(',').map(_.trim)
    if (strings.contains("*")) {
      Set("n", "m")
    } else {
      strings.toSet
    }
  }

  test("RETURN * in a CALL () should export variables") {
    for {
      subqueryReturn <- returnStarNMCombinations
      subqueryReturnVars = containedVariables(subqueryReturn)
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(subqueryReturnVars)
    } {
      val query =
        s"""
           |CALL () {
           |  MATCH (n), (m)
           |  RETURN $subqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(CypherVersion.values(), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in a CALL should export variables") {
    for {
      subqueryReturn <- returnStarNMCombinations
      subqueryReturnVars = containedVariables(subqueryReturn)
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(subqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $subqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher5), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in UNION in a CALL should export variables") {
    for {
      firstSubqueryReturn <- returnStarNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $secondSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher5), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in UNION in a CALL () should export variables - with matching return columns") {
    for {
      firstSubqueryReturn <- returnStarOrderedNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarOrderedNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarOrderedNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL () {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $secondSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin

      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher25), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in 3-way-UNION in a CALL should export variables") {
    for {
      firstSubqueryReturn <- returnStarNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      thirdSubqueryReturn <- returnStarNMCombinations
      thirdSubqueryReturnVars = containedVariables(thirdSubqueryReturn)
      if thirdSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $thirdSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher5), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in 3-way-UNION in a CALL () should export variables - with matching return columns") {
    for {
      firstSubqueryReturn <- returnStarOrderedNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarOrderedNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      thirdSubqueryReturn <- returnStarOrderedNMCombinations
      thirdSubqueryReturnVars = containedVariables(thirdSubqueryReturn)
      if thirdSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarOrderedNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL () {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $thirdSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val result = runSemanticAnalysisWithCypherVersion(Seq(CypherVersion.Cypher25), query)
        result.errors should be(empty)

        val statement = result.state.statement().asInstanceOf[SingleQuery]
        val semanticState = result.state.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  override def messageProvider: ErrorMessageProvider = new ErrorMessageProviderAdapter {

    override def createDynamicGraphReferenceUnsupportedError(graphName: String): String =
      "A very nice message explaining why dynamic graph references are not allowed: " + graphName

    override def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean = false): String =
      "A very nice message explaining why multiple graph references are not allowed: " + graphName
  }
}
