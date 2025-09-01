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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck.TypeMismatchContext
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.DefaultTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.TypeSignature
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlParams

/**
 * This class holds methods for performing semantic analysis.
 */
trait SemanticAnalysisTooling {

  def semanticCheckFold[A](
    iterable: Iterable[A]
  )(
    f: A => SemanticCheck
  ): SemanticCheck = {
    iterable.foldLeft(SemanticCheck.success) {
      (accCheck, o: A) => accCheck chain f(o)
    }
  }

  def semanticCheck[A <: SemanticCheckable](iterable: IterableOnce[A]): SemanticCheck = {
    iterable.iterator.foldLeft(SemanticCheck.success) {
      (accCheck: SemanticCheck, o: A) => accCheck chain o.semanticCheck
    }
  }

  /** Runs `check` on `state`. Discards produced state, but retains produced errors */
  def withState(state: SemanticState)(check: SemanticCheck): SemanticCheck = {
    for {
      original <- SemanticCheck.getState
      _ <- SemanticCheck.setState(state)
      checked <- check
    } yield SemanticCheckResult(updateRecordedGraphs(original.state, checked.state), checked.errors)
  }

  def updateRecordedGraphs(originalState: SemanticState, newState: SemanticState): SemanticState = {
    val recordedTargetGraphState = newState.targetGraph match {
      case Some(targetGraph) => originalState.recordTargetGraph(targetGraph)
      case None              => originalState
    }
    recordedTargetGraphState.recordWorkingGraph(newState.workingGraph)
  }

  def specifyType(
    typeGen: TypeGenerator,
    expression: Expression
  ): SemanticState => Either[SemanticError, SemanticState] =
    (s: SemanticState) => specifyType(typeGen(s), expression)(s)

  def specifyType(
    possibleTypes: => TypeSpec,
    expression: Expression
  ): SemanticState => Either[SemanticError, SemanticState] =
    _.specifyType(expression, possibleTypes)

  def expectType(typeGen: TypeGenerator, expression: Expression): SemanticCheck =
    (s: SemanticState) => expectType(s, typeGen(s), expression, TypeMismatchContext.EMPTY)

  def expectType(possibleTypes: TypeSpec, opt: Option[Expression]): SemanticCheck =
    opt.foldSemanticCheck(expectType(possibleTypes, _))

  def expectType(
    typeGen: TypeGenerator,
    expression: Expression,
    typeMismatch: TypeMismatchContext.TypeMismatchContextVal,
    messageGen: (String, String) => String
  ): SemanticCheck =
    (s: SemanticState) => expectType(s, typeGen(s), expression, typeMismatch, messageGen)

  def expectType[Exp <: Expression](possibleTypes: TypeSpec, expressions: Iterable[Exp]): SemanticCheck =
    expectType(possibleTypes, expressions, TypeMismatchContext.EMPTY)

  def expectType[Exp <: Expression](
    possibleTypes: TypeSpec,
    expressions: Iterable[Exp],
    typeMismatchContext: TypeMismatchContext.TypeMismatchContextVal
  ): SemanticCheck =
    (state: SemanticState) =>
      expressions.foldLeft(SemanticCheckResult.success(state)) {
        (r1: SemanticCheckResult, o: Exp) =>
          {
            val r2 = o match {
              case v: LogicalVariable =>
                expectType(
                  r1.state,
                  possibleTypes,
                  o,
                  TypeMismatchContext.TypeMismatchContextVal(GqlParams.StringParam.ident.process(v.name))
                )
              case _ => expectType(r1.state, possibleTypes, o, typeMismatchContext)
            }
            SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
          }
      }

  def expectType(
    possibleTypes: => TypeSpec
  )(
    ctx: SemanticContext,
    expr: Expression
  ): SemanticCheck = expectType(possibleTypes, expr)

  def expectType(
    possibleTypes: => TypeSpec,
    expression: Expression
  ): SemanticCheck = (s: SemanticState) => {
    expression match {
      case v: LogicalVariable =>
        expectType(
          s,
          possibleTypes,
          expression,
          TypeMismatchContext.TypeMismatchContextVal(GqlParams.StringParam.ident.process(v.name))
        )
      case _ => expectType(s, possibleTypes, expression, TypeMismatchContext.EMPTY)
    }
  }

  def expectType(
    possibleTypes: => TypeSpec,
    expression: Expression,
    typeMismatchVal: TypeMismatchContext.TypeMismatchContextVal,
    messageGen: (String, String) => String
  ): SemanticCheck = (s: SemanticState) => {
    expectType(s, possibleTypes, expression, typeMismatchVal, messageGen)
  }

  def expectType(
    s: SemanticState,
    possibleTypes: => TypeSpec,
    expression: Expression,
    typeMismatchVal: TypeMismatchContext.TypeMismatchContextVal,
    messageGen: (String, String) => String = DefaultTypeMismatchMessageGenerator
  ): SemanticCheckResult = {
    s.expectType(expression, possibleTypes) match {
      case (ss, TypeSpec.none) =>
        val existingTypesString = ss.expressionType(expression).specified.mkString(", ", " or ")
        val expectedTypesString = possibleTypes.mkString(", ", " or ")
        expression match {
          case p: Parameter
            if !p.name.matches(
              """\s\sAUTO(INT|STRING|DOUBLE|LIST)\d+"""
            ) => // See literalReplacement for list of all AUTOs
            SemanticCheckResult.error(
              ss,
              SemanticError.invalidEntityType(
                existingTypesString,
                s"${typeMismatchVal.txt} parameter: ${p.name}",
                possibleTypes.toStrings.toList,
                "Type mismatch for parameter '" + p.name + "': " + messageGen(expectedTypesString, existingTypesString),
                expression.position
              )
            )
          case _ =>
            val semanticError =
              if (typeMismatchVal == TypeMismatchContext.EMPTY) {
                // No information is available about the context, so fall back to GQL code 22NB1:
                //   Type mismatch: expected to be one of { $s } but was { $s }."
                SemanticError.typeMismatch(
                  possibleTypes.toStrings.toList,
                  existingTypesString,
                  "Type mismatch: " + messageGen(expectedTypesString, existingTypesString),
                  expression.position
                )
              } else {
                // Information about the context is available, so use it using GQL code 22N27:
                //   Invalid input { %s } for { %s }. Expected to be one of { %s }.
                SemanticError.invalidEntityType(
                  existingTypesString,
                  typeMismatchVal.txt,
                  possibleTypes.toStrings.toList,
                  "Type mismatch: " + messageGen(expectedTypesString, existingTypesString),
                  expression.position
                )
              }

            SemanticCheckResult.error(
              ss,
              semanticError
            )
        }
      case (ss, _) =>
        SemanticCheckResult.success(ss)
    }
  }

  def checkTypes(expression: Expression, signatures: Seq[TypeSignature]): SemanticCheck = (s: SemanticState) => {
    val initSignatures = signatures.filter(_.argumentTypes.length == expression.arguments.length)

    val (remainingSignatures: Seq[TypeSignature], argIdx, result) =
      expression.arguments.foldLeft((initSignatures, 0, SemanticCheckResult.success(s))) {
        case (accumulator @ (Seq(), _, _), _) =>
          accumulator
        case ((possibilities, argIdx, r1), arg) =>
          val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }

          val info = expression match {
            case FunctionInvocation(functionName, _, _, _, _) =>
              TypeMismatchContext.TypeMismatchContextVal(
                s"argument at index $argIdx of function ${functionName.name}()"
              )
            case _ => TypeMismatchContext.EMPTY
          }

          val r2 = expectType(r1.state, argTypes, arg, info)

          val actualTypes = types(arg)(r2.state)
          val remainingPossibilities = possibilities.filter {
            sig => actualTypes containsAny sig.argumentTypes.head.covariant
          } map {
            sig => sig.removeFirstArgumentType
          }
          (remainingPossibilities, argIdx + 1, SemanticCheckResult(r2.state, r1.errors ++ r2.errors))
      }

    val outputType = remainingSignatures match {
      case Seq() => TypeSpec.all
      case _     => remainingSignatures.foldLeft(TypeSpec.none) { _ | _.outputType.invariant }
    }

    specifyType(outputType, expression)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }

  def whenState(condition: SemanticState => Boolean)(
    thenBranch: => SemanticCheck,
    elseBranch: => SemanticCheck = SemanticCheck.success
  ): SemanticCheck = {
    SemanticCheck.fromState { state =>
      if (condition(state))
        thenBranch
      else
        elseBranch
    }
  }

  def unless(condition: Boolean)(check: => SemanticCheck): SemanticCheck =
    if (condition)
      SemanticCheck.success
    else
      check

  def unionOfTypes(iterable: IterableOnce[Expression]): TypeGenerator = (state: SemanticState) =>
    TypeSpec.union(iterable.iterator.map(types(_)(state)).toSeq: _*)

  def leastUpperBoundsOfTypes(iterable: IterableOnce[Expression]): TypeGenerator =
    if (iterable.iterator.isEmpty)
      _ => CTAny.invariant
    else
      (state: SemanticState) => iterable.iterator.map { types(_)(state) } reduce { _ leastUpperBounds _ }

  def withScopedState(check: => SemanticCheck): SemanticCheck =
    SemanticAnalysisTooling.pushStateScope chain
      check chain
      SemanticAnalysisTooling.popStateScope

  def withScopedStateWithVariablesFromRecordedScope(
    astNode: ASTNode,
    exclude: Set[String] = Set.empty
  )(check: => SemanticCheck): SemanticCheck =
    SemanticAnalysisTooling.pushStateScopeWithVariablesFromRecordedScope(astNode, exclude) chain
      check chain
      SemanticAnalysisTooling.popStateScope

  def typeSwitch(expr: Expression)(choice: TypeSpec => SemanticCheck): SemanticCheck =
    SemanticCheck.fromState(state => choice(state.expressionType(expr).actual))

  def validNumber(long: IntegerLiteral): Boolean =
    try {
      long.value.isInstanceOf[Long]
    } catch {
      case _: java.lang.NumberFormatException => false
    }

  def validNumber(double: DoubleLiteral): Boolean =
    try {
      double.value.isInstanceOf[Double]
    } catch {
      case _: java.lang.NumberFormatException => false
    }

  def ensureDefined(v: LogicalVariable): SemanticState => Either[SemanticError, SemanticState] =
    (_: SemanticState).ensureVariableDefined(v)

  def declareVariable(
    v: LogicalVariable,
    possibleTypes: TypeSpec
  ): SemanticState => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareVariable(v, possibleTypes)

  def declareVariable(
    v: LogicalVariable,
    possibleTypes: TypeSpec,
    maybePreviousDeclaration: Option[Symbol]
  ): SemanticState => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareVariable(v, possibleTypes, maybePreviousDeclaration)

  /**
   * @param overriding if `true` then a previous occurrence of that variable is overridden.
   *                   if `false` then a previous occurrence of that variable leads to an error
   */
  def declareVariable(
    v: LogicalVariable,
    typeGen: TypeGenerator,
    maybePreviousDeclaration: Option[Symbol] = None,
    overriding: Boolean = false
  ): SemanticState => Either[SemanticError, SemanticState] =
    (s: SemanticState) =>
      s.declareVariable(v, typeGen(s), maybePreviousDeclaration, overriding)

  def implicitVariable(
    v: LogicalVariable,
    possibleType: CypherType
  ): SemanticState => Either[SemanticError, SemanticState] =
    (_: SemanticState).implicitVariable(v, possibleType)

  def declareVariables(symbols: Iterable[Symbol]): SemanticCheck =
    symbols.foldSemanticCheck(symbol => declareVariable(symbol.definition.asVariable, symbol.types))

  def recordCurrentScope(astNode: ASTNode): SemanticCheck = { (state: SemanticState) =>
    SemanticCheckResult.success(state.recordCurrentScope(astNode))
  }

  def importValuesFromRecordedScope(astNode: ASTNode): SemanticCheck = { (state: SemanticState) =>
    val scopeToImportFrom = state.scope(astNode).get
    SemanticCheckResult.success(state.importValuesFromScope(scopeToImportFrom))
  }

  def importValuesFromScope(scope: Scope): SemanticCheck = {
    SemanticCheck.fromFunction(state => SemanticCheckResult.success(state.importValuesFromScope(scope)))
  }

  def requireFeatureSupport(msg: String, feature: SemanticFeature, position: InputPosition): SemanticCheck =
    (s: SemanticState) => {
      if (!s.features(feature))
        SemanticCheckResult.error(
          s,
          FeatureError(
            s"$msg is not available in this implementation of Cypher " +
              s"due to lack of support for $feature.",
            feature,
            position
          )
        )
      else
        SemanticCheckResult.success(s)
    }

  def error(msg: String, position: InputPosition): SemanticCheck = SemanticCheck.error(SemanticError(msg, position))

  def error(gqlStatusObject: ErrorGqlStatusObject, msg: String, position: InputPosition): SemanticCheck =
    SemanticCheck.error(SemanticError(gqlStatusObject, msg, position))

  def error(semanticError: SemanticError): SemanticCheck = SemanticCheck.error(semanticError)

  def specifiedNumberOutOfRangeError(
    component: String,
    valueType: String,
    lower: Number,
    upper: Number,
    inputValue: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticCheck =
    SemanticCheck.error(SemanticError.specifiedNumberOutOfRange(
      component,
      valueType,
      lower,
      upper,
      inputValue,
      legacyMessage,
      position
    ))

  def invalidPlacementOfUseClauseError(position: InputPosition): SemanticCheck =
    SemanticCheck.error(SemanticError.invalidPlacementOfUseClause(position))

  def warn(notification: InternalNotification): SemanticCheck = SemanticCheck.warn(notification)

  def possibleTypes(expression: Expression): TypeGenerator =
    types(expression)(_).unwrapLists

  def types(expression: Expression): TypeGenerator = _.expressionType(expression).actual
}

object SemanticAnalysisTooling {
  private val pushStateScope: SemanticCheck = (state: SemanticState) => SemanticCheckResult.success(state.newChildScope)

  private def pushStateScopeWithVariablesFromRecordedScope(
    astNode: ASTNode,
    exclude: Set[String]
  ): SemanticCheck =
    (state: SemanticState) => {
      val scopeToImportFrom = state.recordedScopes(astNode).scope
      SemanticCheckResult.success(state.newChildScope.importValuesFromScope(scopeToImportFrom, exclude))
    }

  private val popStateScope: SemanticCheck = (state: SemanticState) => SemanticCheckResult.success(state.popScope)
}
