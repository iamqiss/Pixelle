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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StatementHelper.RichStatement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.Optional
import java.util.UUID

trait RewritePhaseTest {
  self: CypherFunSuite with AstConstructionTestSupport =>

  def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState]

  def astRewriteAndAnalyze: Boolean = true

  def semanticFeatures: Seq[SemanticFeature] = Seq.empty

  def preProcessPhase(features: SemanticFeature*): Transformer[BaseContext, BaseState, BaseState] =
    SemanticAnalysis(false, semanticFeatures ++ features: _*)

  def rewriterPhaseForExpected: Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {
      override def transform(from: BaseState, context: BaseContext): BaseState = from

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "do nothing"
    }

  val prettifier: Prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  private val plannerName = new PlannerName {
    override def name: String = "fake"
    override def toTextOutput: String = "fake"
    override def version: String = "fake"
  }

  def assertNotRewritten(from: String): Unit = assertRewritten(from, from)

  def assertRewritten(from: String, to: String): Unit = assertRewritten(from, to, List.empty)

  def assertRewritten(
    from: String,
    to: String,
    semanticTableExpressions: List[Expression],
    features: SemanticFeature*
  ): Unit = {

    /**
     * To be able to just read Cypher when looking at a failure in a `RewritePhaseTest`,
     */
    case class StatementPrettifier(statement: Statement) {
      override def toString: String = {
        prettifier.asString(statement)
      }
    }
    val fromOutState: BaseState = prepareFrom(from, rewriterPhaseUnderTest, features: _*)

    val toOutState = prepareFrom(to, rewriterPhaseForExpected, features: _*)

    StatementPrettifier(fromOutState.statement()) should equal(StatementPrettifier(toOutState.statement()))
    if (astRewriteAndAnalyze) {
      semanticTableExpressions.foreach { e =>
        fromOutState.semanticTable().types.keys.map(_.node) should contain(e)
      }
    }
  }

  def assertRewritten(from: String, to: Statement): Unit = assertRewritten(from, to, List.empty)

  def assertRewritten(
    from: String,
    to: Statement,
    semanticTableExpressions: List[Expression],
    features: SemanticFeature*
  ): Unit = {
    val fromOutState: BaseState = prepareFrom(from, rewriterPhaseUnderTest, features: _*)

    fromOutState.statement() should equal(to)
    if (astRewriteAndAnalyze) {
      semanticTableExpressions.foreach { e =>
        fromOutState.semanticTable().types.keys.map(_.node) should contain(e)
      }
    }
  }

  private def parseAndRewrite(version: CypherVersion, queryText: String, features: SemanticFeature*): Statement = {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val nameGenerator = new AnonymousVariableNameGenerator
    val parsedAst = AstParserFactory(version)(queryText, exceptionFactory, None).singleStatement()
    val cleanedAst = parsedAst.endoRewrite(normalizeWithAndReturnClauses(exceptionFactory))
    if (astRewriteAndAnalyze) {
      val semanticState = cleanedAst.semanticState(semanticFeatures ++ features: _*)
      ASTRewriter.rewrite(
        cleanedAst.endoRewrite(computeDependenciesForExpressions(semanticState)),
        semanticState,
        Map.empty,
        exceptionFactory,
        nameGenerator,
        CancellationChecker.NeverCancelled
      )
    } else {
      cleanedAst
    }
  }

  private def parseAndRewrite(query: String, features: SemanticFeature*): Statement = {
    // Quick and dirty hack to try to make sure we have sufficient coverage of all cypher versions.
    // Feel free to improve ¯\_(ツ)_/¯.
    val defaultResult = rewriteOtherASTDifferences(parseAndRewrite(CypherVersion.Default, query, features: _*))
    CypherVersion.values().foreach { version =>
      if (version != CypherVersion.Default) {
        withClue(s"Parser $version") {
          rewriteOtherASTDifferences(parseAndRewrite(version, query, features: _*)) shouldBe defaultResult
        }
      }
    }
    defaultResult
  }

  /**
   * There are some AST changes done at the parser level for semantic analysis that won't affect the plan.
   * This rewriter can be expanded to update those parts.
   */
  def rewriteOtherASTDifferences(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case u: SetExactPropertiesFromMapItem     => u.copy(rhsMustBeMap = false)(u.position)
      case u: SetIncludingPropertiesFromMapItem => u.copy(rhsMustBeMap = false)(u.position)
    }))
  }

  def sessionDatabase: String = null
  def targetsComposite: Boolean = false

  def prepareFrom(
    from: String,
    transformer: Transformer[BaseContext, BaseState, BaseState],
    features: SemanticFeature*
  ): BaseState = {
    val fromAst = parseAndRewrite(from, features: _*)
    val initialState =
      InitialState(from, plannerName, new AnonymousVariableNameGenerator, maybeStatement = Some(fromAst))
    val databaseReference = new DatabaseReference {
      override def alias(): NormalizedDatabaseName = ???

      override def namespace(): Optional[NormalizedDatabaseName] = ???

      override def isPrimary: Boolean = ???

      override def id(): UUID = ???

      override def toPrettyString: String = ???

      override def fullName(): NormalizedDatabaseName = new NormalizedDatabaseName(sessionDatabase)

      override def isComposite: Boolean = targetsComposite

      override def compareTo(o: DatabaseReference): Int = ???

      override def owningDatabaseName: String = ???

      override def catalogEntry(): NormalizedCatalogEntry = ???
    }

    val fromInState = {
      if (astRewriteAndAnalyze) {
        preProcessPhase(features: _*).transform(
          initialState,
          TestContext(sessionDatabase = databaseReference)
        )
      } else {
        initialState
      }
    }
    transformer.transform(fromInState, ContextHelper.create(databaseReference))
  }
}
