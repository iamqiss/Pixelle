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
package org.neo4j.cypher.internal.ast

import org.junit.jupiter.api.Assertions.assertEquals
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.nio.charset.StandardCharsets

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.ClassSymbol
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.termNames
import scala.reflect.runtime.universe.typeOf

class AdministrationCommandTest extends CypherFunSuite with AstConstructionTestSupport {
  private val p = InputPosition.withLength(0, 0, 0, 0)
  private val pos1 = InputPosition(0, 1, 0)
  private val pos2 = InputPosition(0, 2, 0)
  private val pos3 = InputPosition(0, 3, 0)
  private val pos4 = InputPosition(0, 4, 0)

  private val initialState =
    SemanticState.clean
      .withFeature(SemanticFeature.MultipleDatabases)

  // Privilege command tests

  object attrLoader {

    def loadAuthAttributes(): List[AuthAttribute] = {
      loadSubClassesRec(typeOf[AuthAttribute])
    }

    def loadSubClassesRec(tpe: Type): List[AuthAttribute] = {
      val clazz = tpe.typeSymbol.asClass
      clazz.knownDirectSubclasses

      val subclasses: List[AuthAttribute] = clazz.knownDirectSubclasses.collect {
        case subclass if subclass.isClass && subclass.asClass.isTrait => loadSubClassesRec(subclass.typeSignature)
        case subclass if subclass.isClass && !subclass.asClass.isTrait && !subclass.isAbstract =>
          val classMirror = currentMirror.reflectClass(subclass.asClass)

          val constructorSymbol = subclass.asClass.primaryConstructor.asMethod
          val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
          val mockedParams = mockParams(subclass.asClass)
          Set(constructorMirror(mockedParams: _*).asInstanceOf[AuthAttribute])
      }.toList.flatten
      subclasses
    }

    def mockParams(sub: ClassSymbol): List[Any] = {
      sub.typeSignature.decl(termNames.CONSTRUCTOR).asMethod.paramLists.flatten.map { param =>
        param.typeSignature match {
          case t if t =:= typeOf[Int]     => 0
          case t if t =:= typeOf[Boolean] => false
          case t if t =:= typeOf[Double]  => 0.0
          case t if t =:= typeOf[Long]    => 0L
          case t if t =:= typeOf[String]  => null
          case t if t <:< typeOf[AnyRef]  => null // For reference types, pass null
          case _                          => null // Fallback for any unexpected types
        }
      }
    }
  }

  def getGql42N97_missingMandatoryAuthClause(clause: String, authProvider: String): ErrorGqlStatusObject = {
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N97)
      .withParam(GqlParams.StringParam.clause, clause)
      .withParam(GqlParams.StringParam.auth, authProvider)
      .build()
  }

  def getGql42N19_duplicateClause(clause: String): ErrorGqlStatusObject = {
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N19)
      .withParam(GqlParams.StringParam.syntax, clause)
      .build()
  }

  test("allAuthAttributes should be up to date") {
    val allAuthAttributes = attrLoader.loadAuthAttributes().map(_.name).toSet
    assertEquals(
      allAuthAttributes,
      allAuthAttr.allAuthsAttributes.map(_.name).toSet
    ) // Note: if this fails allAuthAttributes in Administration is probably out of date
  }

  test("it should not be possible to administer privileges pertaining to an unassignable action") {

    val privilegeManagementActions =
      Table("PrivilegeManagementActions", AssignImmutablePrivilegeAction, RemoveImmutablePrivilegeAction)

    val grant = (pma: PrivilegeManagementAction) =>
      new GrantPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(literalString("role1"))
      )(p)

    val deny = (pma: PrivilegeManagementAction) =>
      new DenyPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(literalString("role1"))
      )(p)

    val revoke = (pma: PrivilegeManagementAction, rt: RevokeType) =>
      new RevokePrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(literalString("role1")),
        rt
      )(p)

    val revokeBoth = revoke(_, RevokeBothType()(p))
    val revokeGrant = revoke(_, RevokeGrantType()(p))
    val revokeDeny = revoke(_, RevokeDenyType()(p))
    val privilegeCommands = Table("PrivilegeCommand", grant, deny, revokeBoth, revokeGrant, revokeDeny)

    forAll(privilegeManagementActions) { pma =>
      forAll(privilegeCommands) { privilegeCommand =>
        val privilege = privilegeCommand(pma)
        privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
          .error(
            initialState,
            SemanticError(
              GqlHelper.getGql42001_42N14(
                "GRANT, DENY and REVOKE",
                pma.name,
                p.line,
                p.column,
                p.offset
              ),
              s"`GRANT`, `DENY` and `REVOKE` are not supported for `${pma.name}`",
              p
            )
          )
      }
    }
  }

  // Property Rules

  type QualifierFn = (Option[Variable], Expression) => List[PrivilegeQualifier]
  val allLabelPatternQualifier: QualifierFn = (v, e) => List(PatternQualifier(Seq(LabelAllQualifier()(p)), v, e))

  val singleLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p)), v, e))

  val multiLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p), LabelQualifier("B")(p)), v, e))

  val mixedList: ListLiteral =
    listOf(literalInt(1), literalString("s"), literalFloat(1.1), falseLiteral, parameter("value1", CTAny))

  Seq(
    (allLabelPatternQualifier, "all labels"),
    (singleLabelPatternQualifier, "single label"),
    (multiLabelPatternQualifier, "multiple labels")
  ).foreach {
    case (qualifierFn: QualifierFn, qualifierDescription) =>
      // e.g. FOR (n{prop1:val1, prop2:val2})
      test(s"property rules with more than one property should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            MapExpression(Seq(
              (PropertyKeyName("prop1")(p), StringLiteral("val1")(p)),
              (PropertyKeyName("prop2")(p), StringLiteral("val2")(p))
            ))(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Failed to administer property rule. The expression: `{prop1: \"val1\", prop2: \"val2\"}` is not supported. Property rules can only contain one property."
        }) shouldBe true
      }

      Seq(
        ("=", " Use `IS NULL` instead.", (lhs: Expression, rhs: Expression) => Equals(lhs, rhs)(p)),
        ("<>", " Use `IS NOT NULL` instead.", (lhs: Expression, rhs: Expression) => NotEquals(lhs, rhs)(p)),
        (">", "", (lhs: Expression, rhs: Expression) => GreaterThan(lhs, rhs)(p)),
        (">=", "", (lhs: Expression, rhs: Expression) => GreaterThanOrEqual(lhs, rhs)(p)),
        ("<", "", (lhs: Expression, rhs: Expression) => LessThan(lhs, rhs)(p)),
        ("<=", "", (lhs: Expression, rhs: Expression) => LessThanOrEqual(lhs, rhs)(p))
      ).foreach { case (operator, suggestionPartOfErrorMessage, op) =>
        // e.g. FOR (n) WHERE n.prop1 = 1 AND n.prop2 = 1
        test(
          s"property rules using WHERE syntax with multiple predicates via AND should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              And(
                op(
                  Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                  SignedDecimalIntegerLiteral("1")(p)
                ),
                op(
                  Property(varFor("n", p), PropertyKeyName("prop2")(p))(p),
                  SignedDecimalIntegerLiteral("1")(p)
                )
              )(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"Failed to administer property rule. The expression: `n.prop1 $operator 1 AND n.prop2 $operator 1` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = 1 OR n.prop2 = 1
        test(
          s"property rules using WHERE syntax with multiple predicates via OR should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Or(
                op(
                  Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                  SignedDecimalIntegerLiteral("1")(p)
                ),
                op(
                  Property(varFor("n", p), PropertyKeyName("prop2")(p))(p),
                  SignedDecimalIntegerLiteral("1")(p)
                )
              )(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"Failed to administer property rule. The expression: `n.prop1 $operator 1 OR n.prop2 $operator 1` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = NULL
        test(s"property rules using n.prop $operator NULL should fail semantic checking ($qualifierDescription)") {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Property(varFor("n", p), PropertyKeyName("prop1")(p))(p), Null.NULL)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(
            _.msg == s"Failed to administer property rule. The property value access rule pattern `prop1 $operator NULL` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
          ) shouldBe true
        }

        // e.g. FOR (n) WHERE NULL = n.prop1
        test(s"property rules using NULL $operator n.prop should fail semantic checking ($qualifierDescription)") {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Null.NULL, Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(
            _.msg == s"Failed to administer property rule. The property value access rule pattern `NULL $operator prop1` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
          ) shouldBe true
        }

        // e.g. FOR (n) WHERE NOT n.prop = NULL
        test(
          s"property rules using NOT n.prop $operator NULL should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(op(Property(varFor("n", p), PropertyKeyName("prop")(p))(p), Null.NULL))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(
            _.msg == s"Failed to administer property rule. The property value access rule pattern `prop $operator NULL` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
          ) shouldBe true
        }

        // e.g. FOR (n) WHERE NOT NULL = n.prop
        test(
          s"property rules using NOT NULL $operator n.prop should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(op(Null.NULL, Property(varFor("n", p), PropertyKeyName("prop")(p))(p)))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(
            _.msg == s"Failed to administer property rule. The property value access rule pattern `NULL $operator prop` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
          ) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = NaN
        test(
          s"property rules using n.prop $operator NaN should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Property(varFor("n", p), PropertyKeyName("prop1")(p))(p), NaN()(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == "Failed to administer property rule. `NaN` is not supported for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE NaN = n.prop1
        test(
          s"property rules using NaN $operator n.prop should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(NaN()(p), Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == "Failed to administer property rule. `NaN` is not supported for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = 1+2
        test(
          s"property rules using WHERE syntax with non-literal predicates should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                Add(SignedDecimalIntegerLiteral("1")(p), SignedDecimalIntegerLiteral("2")(p))(p)
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"Failed to administer property rule. The expression: `n.prop1 $operator 1 + 2` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = date.realtime()
        test(
          s"property rules using WHERE syntax with sub functions of temporal functions should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function("date.realtime")
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"Failed to administer property rule. The expression: `n.prop1 $operator `date.realtime`()` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = [1, 2]
        test(
          s"property rules using WHERE syntax with List of literals should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val expressionStringifier = ExpressionStringifier()
          Seq(
            // List of ints
            op(prop(varFor("n"), "prop1"), listOfInt(1, 2)), // n.prop = [1, 2]

            // List of strings
            op(prop(varFor("n"), "prop1"), listOfString("s1", "s2")), // n.prop = ['s1', 's2']

            // List of booleans
            op(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral)), // n.prop = [true, false]

            // List of floats
            op(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2))), // n.prop = [1.1, 2.2]

            // List of parameters
            op(
              prop(varFor("n"), "prop1"),
              listOf(parameter("value", CTAny), parameter("value2", CTAny))
            ), // n.prop = [$value, $value2]

            // List of temporal values
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("date", literalString("2024-10-09")),
                function("date", literalString("2024-10-11"))
              )
            ), // n.prop = [date("2024-10-09"), date("2024-10-11")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00")),
                function("datetime", literalString("2024-10-11T11:10:09:40+02:00"))
              )
            ), // n.prop = [datetime("2024-10-09T12:10:09:40+02:00"), datetime("2024-10-11T11:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5")),
                function("localdatetime", literalString("2024-10-09T12:55:35.5"))
              )
            ), // n.prop = [localdatetime("2024-10-09T12:50:35.5"), localdatetime("2024-10-09T12:55:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00")),
                function("time", literalString("11:10:09:40+02:00"))
              )
            ), // n.prop = [time("12:10:09:40+02:00"), time("11:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5")),
                function("localtime", literalString("12:55:35.5"))
              )
            ), // n.prop = [localtime("2024-10-09T12:50:35.5"), localtime("2024-10-09T12:55:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("duration", literalString("PT1S")),
                function("duration", literalString("PT2S"))
              )
            ), // n.prop = [duration("PT1S"), duration("PT2S")]

            // List of points
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2)),
                function("point", mapOfInt("x" -> 3, "y" -> 4))
              )
            ), // n.prop = [point({x: 1, y: 2}), point({x: 3, y: 4})]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
                function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
              )
            ), // n.prop = [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]

            // Mixed list
            op(prop(varFor("n"), "prop1"), mixedList) // n.prop = [1, 's', 1.1, false, $value1]
          ).foreach { expression =>
            withClue(expressionStringifier(expression)) {
              val privilege = new GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  expression
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
              result.errors.exists(s => {
                s.msg == "Failed to administer property rule. " +
                  s"The expression: `${expressionStringifier(expression)}` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
              }) shouldBe true
            }
          }
        }

        // e.g. FOR (n) WHERE n.prop1 = [1]
        test(
          s"property rules using WHERE syntax with single-item list of literals should fail semantic checking ($qualifierDescription)($operator)"
        ) {
          val expressionStringifier = ExpressionStringifier()
          Seq(
            // List of ints
            op(prop(varFor("n"), "prop1"), listOfInt(1)), // n.prop = [1]

            // List of strings
            op(prop(varFor("n"), "prop1"), listOfString("s1")), // n.prop = ['s1']

            // List of booleans
            op(prop(varFor("n"), "prop1"), listOf(trueLiteral)), // n.prop = [true]

            // List of floats
            op(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1))), // n.prop = [1.1]

            // List of temporal values
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("date", literalString("2024-10-09"))
              )
            ), // n.prop = [date("2024-10-09")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00"))
              )
            ), // n.prop = [datetime("2024-10-09T12:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5"))
              )
            ), // n.prop = [localdatetime("2024-10-09T12:50:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00"))
              )
            ), // n.prop = [time("12:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5"))
              )
            ), // n.prop = [localtime("2024-10-09T12:50:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("duration", literalString("PT1S"))
              )
            ), // n.prop = [duration("PT1S")]

            // List of points
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2))
              )
            ), // n.prop = [point({x: 1, y: 2})]

            // List of parameters
            op(
              prop(varFor("n"), "prop1"),
              listOf(parameter("value", CTAny))
            ) // n.prop = [$value]
          ).foreach { expression =>
            withClue(expressionStringifier(expression)) {
              val privilege = new GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  expression
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
              result.errors.exists(s => {
                s.msg == "Failed to administer property rule. " +
                  s"The expression: `${expressionStringifier(expression)}` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
              }) shouldBe true
            }
          }
        }

        // e.g. FOR (n) WHERE NOT NOT n.prop1 = 1
        test(
          s"using more than one NOT keyword combined with an '$operator' should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(Not(op(
                Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                SignedDecimalIntegerLiteral("1")(p)
              ))(p))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"Failed to administer property rule. The expression: `NOT (NOT n.prop1 $operator 1)` is not supported. " +
              s"Only single, literal-based predicate expressions are allowed for property-based access control."
          }) shouldBe true
        }

        // e.g. FOR (n) WHERE 1 = n.prop1
        test(
          s"property rules having n.prop on right hand side of operator $operator should fail semantic checking ($qualifierDescription)"
        ) {
          val privilege = new GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(SignedDecimalIntegerLiteral("1")(p), Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(
            _.msg == s"Failed to administer property rule. The property `prop1` must appear on the left hand side of the `$operator` operator."
          ) shouldBe true
        }
      }

      // e.g. FOR ({n:NULL})
      test(s"property rules NULL in map syntax should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(None, MapExpression(Seq((PropertyKeyName("prop1")(p), Null.NULL)))(p)),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Failed to administer property rule. The property value access rule pattern `{prop1:NULL}` always evaluates to `NULL`. Use `WHERE` syntax in combination with `IS NULL` instead."
        }) shouldBe true
      }

      // e.g. FOR ({prop1:1+2})
      test(
        s"property rules using map syntax with non-literal predicates should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            MapExpression(Seq((
              PropertyKeyName("prop1")(p),
              Add(SignedDecimalIntegerLiteral("1")(p), SignedDecimalIntegerLiteral("2")(p))(p)
            )))(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Failed to administer property rule. " +
            "The expression: `{prop1: 1 + 2}` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
        }) shouldBe true
      }

      // e.g. FOR (n {prop1: [1, 2]})
      test(
        s"property rules using map expression syntax with List of literals should fail semantic checking ($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          MapExpression(Seq((propName("prop1"), listOfInt(1, 2))))(p), // {prop1: [1, 2]}

          // List of strings
          MapExpression(Seq((propName("prop1"), listOfString("s1", "s2"))))(p), // {prop1: ['s1', 's2']}

          // List of booleans
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral, falseLiteral))))(p), // {prop1: [true, false]}

          // List of floats
          MapExpression(
            Seq((propName("prop1"), listOf(literalFloat(1.1), literalFloat(1.2))))
          )(p), // {prop1: [1.1, 2.2]}

          // List of temporal values
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("date", literalString("2024-10-09")),
                function("date", literalString("2024-10-11"))
              )
            ))
          )(p), // {prop1: [date("2024-10-09"), date("2024-10-11")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00")),
                function("datetime", literalString("2024-10-11T11:10:09:40+02:00"))
              )
            ))
          )(p), // {prop1: [datetime("2024-10-09T12:10:09:40+02:00"), datetime("2024-10-11T11:10:09:40+02:00")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5")),
                function("localdatetime", literalString("2024-10-09T12:55:35.5"))
              )
            ))
          )(p), // {prop1: [localdatetime("2024-10-09T12:50:35.5"), localdatetime("2024-10-09T12:55:35.5")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00")),
                function("time", literalString("11:10:09:40+02:00"))
              )
            ))
          )(p), // {prop1: [time("12:10:09:40+02:00"), time("11:10:09:40+02:00")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5")),
                function("localtime", literalString("12:55:35.5"))
              )
            ))
          )(p), // {prop1: [localtime("2024-10-09T12:50:35.5"), localtime("2024-10-09T12:55:35.5")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("duration", literalString("PT1S")),
                function("duration", literalString("PT2S"))
              )
            ))
          )(p), // {prop1: [duration("PT1S"), duration("PT2S")]}

          // List of points
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2)),
                function("point", mapOfInt("x" -> 3, "y" -> 4))
              )
            ))
          )(p), // {prop1: [point({x: 1, y: 2}), point({x: 3, y: 4})]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
                function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
              )
            ))
          )(p), // {prop1: [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]}

          // List of parameters
          MapExpression(
            Seq((propName("prop1"), listOf(parameter("value", CTAny), parameter("value2", CTAny))))
          )(p), // {prop1: [$value, $value2]}

          // Mixed list
          MapExpression(Seq((propName("prop1"), mixedList)))(p) // {prop1: [1, 's', 1.1, false, $value1]}

        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
            }) shouldBe true
          }
        }
      }

      // e.g. FOR (n {prop1: [1]})
      test(
        s"property rules using map expression syntax with single-item list of literals should fail semantic checking ($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          MapExpression(Seq((propName("prop1"), listOfInt(1))))(p), // {prop1: [1]}

          // List of strings
          MapExpression(Seq((propName("prop1"), listOfString("s1"))))(p), // {prop1: ['s1']}

          // List of booleans
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral))))(p), // {prop1: [true]}

          // List of floats
          MapExpression(Seq((propName("prop1"), listOf(literalFloat(1.1)))))(p), // {prop1: [1.1]}

          // List of temporal values
          MapExpression(Seq((propName("prop1"), listOf(function("date", literalString("2024-10-09"))))))(
            p
          ), // {prop1: [date("2024-10-09")]}
          MapExpression(Seq((
            propName("prop1"),
            listOf(function("datetime", literalString("2024-10-09T12:10:09:40+02:00")))
          )))(p), // {prop1: [datetime("2024-10-09T12:10:09:40+02:00")]}
          MapExpression(Seq((
            propName("prop1"),
            listOf(function("localdatetime", literalString("2024-10-09T12:50:35.5")))
          )))(p), // {prop1: [localdatetime("2024-10-09T12:50:35.5")]}
          MapExpression(Seq((propName("prop1"), listOf(function("time", literalString("12:10:09:40+02:00"))))))(
            p
          ), // {prop1: [time("12:10:09:40+02:00")]}
          MapExpression(Seq((propName("prop1"), listOf(function("localtime", literalString("12:50:35.5"))))))(
            p
          ), // {prop1: [localtime("2024-10-09T12:50:35.5")]}
          MapExpression(Seq((propName("prop1"), listOf(function("duration", literalString("PT1S"))))))(
            p
          ), // {prop1: [duration("PT1S")]}

          // List of points
          MapExpression(Seq((propName("prop1"), listOf(function("point", mapOfInt("x" -> 1, "y" -> 2))))))(
            p
          ), // {prop1: [point({x: 1, y: 2})]}
          MapExpression(Seq((propName("prop1"), listOf(function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1))))))(
            p
          ), // {prop1: [point({x: 1, y: 2, z: 1})]}

          // List of parameters
          MapExpression(Seq((propName("prop1"), listOf(parameter("value", CTAny)))))(p) // {prop1: [$value]}
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
            }) shouldBe true
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1]
      test(
        s"property rules using WHERE syntax with property IN List of one literal should pass semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1))(p), // n.prop IN [1]
          Not(In(prop(varFor("n"), "prop1"), listOfInt(1))(p))(p), // NOT n.prop IN [1]

          // List of strings
          In(prop(varFor("n"), "prop1"), listOfString("s1"))(p), // n.prop IN ['s1']
          Not(In(prop(varFor("n"), "prop1"), listOfString("s1"))(p))(p), // NOT n.prop IN ['s1']

          // List of booleans
          In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p), // n.prop IN [true]
          Not(In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p))(p), // NOT n.prop IN [true]

          // List of floats
          In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p), // n.prop IN [1.1]
          Not(In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p))(p), // NOT n.prop IN [1.1]

          // List of temporal values
          In(
            prop(varFor("n"), "prop1"),
            listOf(function("date", literalString("2024-10-09")))
          )(p), // n.prop IN [date("2024-10-09")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(function("datetime", literalString("2024-10-09T12:10:09:40+02:00")))
          )(p))(p), // NOT n.prop IN [datetime("2024-10-09T12:10:09:40+02:00")]
          In(
            prop(varFor("n"), "prop1"),
            listOf(function("localdatetime", literalString("2024-10-09T12:50:35.5")))
          )(p), // n.prop IN [localdatetime("2024-10-09T12:50:35.5")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(function("time", literalString("12:10:09:40+02:00")))
          )(p))(p), // NOT n.prop IN [time("12:10:09:40+02:00")]
          In(
            prop(varFor("n"), "prop1"),
            listOf(function("localtime", literalString("12:50:35.5")))
          )(p), // n.prop IN [localtime("2024-10-09T12:50:35.5")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(function("duration", literalString("PT1S")))
          )(p))(p), // NOT n.prop IN [duration("PT1S")]

          // List of points
          In(
            prop(varFor("n"), "prop1"),
            listOf(function("point", mapOfInt("x" -> 1, "y" -> 2)))
          )(p), // n.prop IN [point({x: 1, y: 2})]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)))
          )(p))(p), // NOT n.prop IN [point({x: 1, y: 2, z: 1})]

          // List of parameters
          In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p), // n.prop IN [$value]
          Not(In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p))(p), // NOT n.prop IN [$value]

          // Parameter list
          In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p), // n.prop IN $paramList
          Not(In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p))(p) // NOT n.prop IN $paramList
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
            }) shouldBe false
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1, 2]
      test(
        s"property rules using WHERE syntax with property IN List of more than one literal should pass semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p), // n.prop IN [1, 2]
          Not(In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p))(p), // NOT n.prop IN [1, 2]

          // List of strings
          In(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))(p), // n.prop IN ['s1', 's2']
          Not(In(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))(p))(p), // NOT n.prop IN ['s1', 's2']

          // List of booleans
          In(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))(p), // n.prop IN [true, false]
          Not(In(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))(p))(p), // NOT n.prop IN [true, false]

          // List of floats
          In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2)))(p), // n.prop IN [1.1, 2.2]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(literalFloat(1.1), literalFloat(2.2))
          )(p))(p), // NOT n.prop IN [1.1, 2.2]

          // List of temporal values
          In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("date", literalString("2024-10-09")),
              function("date", literalString("2024-10-11"))
            )
          )(p), // n.prop IN [date("2024-10-09"), date("2024-10-11")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("datetime", literalString("2024-10-09T12:10:09:40+02:00")),
              function("datetime", literalString("2024-10-11T11:10:09:40+02:00"))
            )
          )(p))(
            p
          ), // NOT n.prop IN [datetime("2024-10-09T12:10:09:40+02:00"), datetime("2024-10-11T11:10:09:40+02:00")]
          In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("localdatetime", literalString("2024-10-09T12:50:35.5")),
              function("localdatetime", literalString("2024-10-09T12:55:35.5"))
            )
          )(p), // n.prop IN [localdatetime("2024-10-09T12:50:35.5"), localdatetime("2024-10-09T12:55:35.5")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("time", literalString("12:10:09:40+02:00")),
              function("time", literalString("11:10:09:40+02:00"))
            )
          )(p))(p), // NOT n.prop IN [time("12:10:09:40+02:00"), time("11:10:09:40+02:00")]
          In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("localtime", literalString("12:50:35.5")),
              function("localtime", literalString("12:55:35.5"))
            )
          )(p), // n.prop IN [localtime("2024-10-09T12:50:35.5"), localtime("2024-10-09T12:55:35.5")]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("duration", literalString("PT1S")),
              function("duration", literalString("PT2S"))
            )
          )(p))(p), // NOT n.prop IN [duration("PT1S"), duration("PT2S")]

          // List of points
          In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("point", mapOfInt("x" -> 1, "y" -> 2)),
              function("point", mapOfInt("x" -> 3, "y" -> 4))
            )
          )(p), // n.prop IN [point({x: 1, y: 2}), point({x: 3, y: 4})]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
              function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
            )
          )(p))(p), // NOT n.prop IN [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]

          // List of parameters
          In(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          )(p), // n.prop IN [$value, $value2]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          )(p))(p), // NOT n.prop IN [$value, $value2]

          // Mixed list
          In(prop(varFor("n"), "prop1"), mixedList)(p), // n.prop IN [1, 's', 1.1, false, $value1]
          Not(In(prop(varFor("n"), "prop1"), mixedList)(p))(p) // NOT n.prop IN [1, 's', 1.1, false, $value1]

        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
            }) shouldBe false

            result.errors.exists(e => {
              e.msg == "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "All elements in a list must be literals of the same type for property-based access control."
            }) shouldBe false
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1, [2]]
      test(
        s"property rules using WHERE syntax with property IN List of literal value and non literal value should fail semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()
        val expression =
          In(
            prop(varFor("n"), "prop1"),
            listOf(literalInt(1), listOfString("stringValue"))
          )(p) // n.prop IN [1, ['stringValue']]

        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(Some(varFor("n", p)), expression),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(_.msg == "Failed to administer property rule. " +
          s"The expression: `${expressionStringifier(expression)}` is not supported. " +
          "All elements in a list must be literals of the same type for property-based access control.") shouldBe true
      }

      // e.g. FOR (node) WHERE n.prop1 = 1
      test(
        s"property rules using WHERE syntax using two different variable names should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("node", p)),
            Equals(
              Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Variable `n` not defined"
        }) shouldBe true
      }

      // e.g. FOR () WHERE n.prop1 = 1
      test(
        s"property rules using WHERE syntax with no variable should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            Equals(
              Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Variable `n` not defined"
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE 1 = n.prop1 (foo) TO role
      test(
        s"Valid property rule, extra (foo) gets parsed as a function and should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("n", p)),
            Equals(
              SignedDecimalIntegerLiteral("1")(p),
              FunctionInvocation(
                FunctionName(Namespace(List("n"))(p), "prop1")(p),
                distinct = false,
                Vector(varFor("foo", p))
              )(p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Failed to administer property rule. " +
            "The expression: `1 = n.prop1(foo)` is not supported. " +
            "Only single, literal-based predicate expressions are allowed for property-based access control."
        }) shouldBe true
      }

      // e.g. FOR (n:A WHERE EXISTS { MATCH (n) }) TO role1
      test(
        s"EXIST MATCH pattern in property rule should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("n", p)),
            ExistsExpression(
              SingleQuery(
                List(
                  Match(
                    optional = false,
                    MatchMode.DifferentRelationships(implicitlyCreated = true)(p),
                    ForMatch(List(PatternPartWithSelector(
                      AllPaths()(p),
                      PathPatternPart(NodePattern(Some(varFor("n", pos)), None, None, None)(p))
                    )))(p),
                    List(),
                    None
                  )(p)
                )
              )(p)
            )(p, None, None)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Failed to administer property rule. " +
            "The expression: `EXISTS { MATCH (n) }` is not supported. " +
            "Only single, literal-based predicate expressions are allowed for property-based access control."
        }) shouldBe true
      }

      Seq(
        AllGraphAction,
        MergeAdminAction,
        CreateElementAction,
        DeleteElementAction,
        SetLabelAction,
        RemoveLabelAction,
        SetPropertyAction,
        WriteAction
      ).foreach(invalidAction => {
        test(s"invalid actions: $invalidAction for property rules ($qualifierDescription)") {
          val privilege = new GrantPrivilege(
            GraphPrivilege(invalidAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              None,
              MapExpression(Seq(
                (PropertyKeyName("prop1")(p), StringLiteral("val1")(p.withInputLength(0)))
              ))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"${invalidAction.name} is not supported for property value access rules."
          }) shouldBe true
        }
      })
  }

  // Create/Alter user

  private def authId(id: String)(p: InputPosition): AuthId = authId(literalString(id))(p)
  private def authId(id: Expression)(p: InputPosition): AuthId = AuthId(id)(p)

  private def password(pass: Expression, isEncrypted: Boolean = false)(p: InputPosition): Password =
    Password(pass, isEncrypted)(p)

  private def passwordChange(requireChange: Boolean)(p: InputPosition): PasswordChange =
    PasswordChange(requireChange)(p)

  private val password: SensitiveStringLiteral =
    SensitiveStringLiteral("password".getBytes(StandardCharsets.UTF_8))(p)

  private val passwordEmpty: SensitiveStringLiteral =
    SensitiveStringLiteral("".getBytes(StandardCharsets.UTF_8))(p)
  private val paramPassword: Parameter = parameter("password", CTString)

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD 'password'") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(password)(pos2)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  test("CREATE USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword)(pos1), password(password)(pos2)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(),
      Some(Auth("native", List(password(password)(pos)))(pos))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("native", List(password(password)(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'foo' { SET ID 'bar' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("foo", List(authId("bar")(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE OR REPLACE USER $foo IF NOT EXISTS SET PASSWORD 'password'") {
    val createUser = CreateUser(
      parameter("foo", CTString),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(),
      Some(Auth("native", List(password(password)(pos)))(pos))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user '$foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE OR REPLACE USER $foo IF NOT EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      parameter("foo", CTString),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("native", List(password(password)(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user '$foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE OR REPLACE USER $foo IF NOT EXISTS SET AUTH 'foo' { SET ID 'bar' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("foo", List(authId("bar")(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.line, p.column, p.offset),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "No auth given for user.", p).errors
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsDoNothing,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "No auth given for user.", p).errors
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = false)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = false)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsReplace,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "No auth given for user.", p).errors
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(
          password(paramPassword)(pos1),
          passwordChange(requireChange = false)(pos2),
          passwordChange(requireChange = true)(pos3)
        )
      )(pos))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD CHANGE [NOT] REQUIRED"),
        initialState,
        "Duplicate `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos3
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = true)(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("foo")(pos2)))(pos1)),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val gql2 =
      GqlHelper.getGql42001_22N04(
        "SET ID",
        "auth provider 'native' attribute",
        java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
        2,
        0,
        0
      )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `native` does not allow `SET ID` clause.",
        pos2
      ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(password(password)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo"),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos1
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      2,
      0,
      0
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(
        Auth("native", List(password(password)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'native'"),
      initialState,
      "Duplicate `SET AUTH 'native'` clause.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(
        Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'native'"),
      initialState,
      "Duplicate `SET AUTH 'native'` clause.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val error3 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("CREATE USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1))
    )(p)

    val error = SemanticCheckResult.error(
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3))
    )(p)

    val error1 = SemanticCheckResult.error(
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test("CREATE USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1))
    )(p)

    val error1 = SemanticCheckResult.error(
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos1
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos2
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3))
    )(p)

    val error1 = SemanticCheckResult.error(
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val error3 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("bar")(pos4)))(pos3)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  test("CREATE USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD 'password'}") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2), password(password)(pos3)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  test("CREATE USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD $password}") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2), password(paramPassword)(pos3)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID $qux }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), authId(parameter("qux", CTString))(pos2)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID"),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None
    )(p)
    val gql =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider 'foo' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2)))(pos)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      2,
      0,
      0
    )
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        gql,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET PASSWORD CHANGE REQUIRED") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos2)))(pos2))
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos2
      ).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2)))(pos3)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native"),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos3
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `native` does not allow `SET ID` clause.",
        pos1
      ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED SET PASSWORD 'password' }"
  ) {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth(
        "native",
        List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2), password(password)(pos4))
      )(pos3)),
      None
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test(
    "CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED }"
  ) {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth(
        "foo",
        List(authId("bar")(pos1), password(password)(pos2), passwordChange(requireChange = true)(pos4))
      )(pos3)),
      None
    )(p)
    val gql =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider 'foo' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  test("CREATE USER foo SET AUTH '' { SET PASSWORD '' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(password(passwordEmpty)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", ""),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    val gql2 =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider '' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error3 = SemanticCheckResult.error(
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("CREATE USER foo SET AUTH PROVIDER '' { SET ID '' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(authId("")(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Invalid input. Auth provider is not allowed to be an empty string.", pos1).errors
  }

  test("CREATE USER foo SET AUTH '' { SET PASSWORD 'password' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(password(password)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", ""),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    val gql2 =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider '' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error3 = SemanticCheckResult.error(
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("CREATE USER foo SET AUTH PROVIDER '' { SET ID 'bar' }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(authId("bar")(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Invalid input. Auth provider is not allowed to be an empty string.", pos1).errors
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 42 }") {
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId(literalInt(42, pos3))(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "id must be a String, or a String parameter.", pos3).errors
  }

  test("ALTER USER foo") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "`ALTER USER` requires at least one clause.", p).errors
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD $password") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(paramPassword, isEncrypted = true)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword)(pos1), password(password)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD 'password'") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(password)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(passwordChange(requireChange = false)(pos1), passwordChange(requireChange = true)(pos2))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD CHANGE [NOT] REQUIRED"),
        initialState,
        "Duplicate `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("foo")(pos1)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql =
      GqlHelper.getGql42001_22N04(
        "SET ID",
        "auth provider 'native' attribute",
        java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
        1,
        0,
        0
      )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(password(password)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      2,
      0,
      0
    )
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo"),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos1
      ).errors
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2
  }

  test(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("native", List(password(password)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'native'"),
        initialState,
        "Duplicate `SET AUTH 'native'` clause.",
        pos3
      ).errors
  }

  test(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'native'"),
        initialState,
        "Duplicate `SET AUTH 'native'` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD 'password'") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(password(password)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Cannot combine old and new auth syntax for the same auth provider.", pos3).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Cannot combine old and new auth syntax for the same auth provider.", pos3).errors
  }

  test("ALTER USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Cannot combine old and new auth syntax for the same auth provider.", pos1).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } SET PASSWORD CHANGE REQUIRED") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Cannot combine old and new auth syntax for the same auth provider.", pos3).errors
  }

  test("ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("bar")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD 'password'}") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2), password(password)(pos3)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD $password}") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2), password(paramPassword)(pos3)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD"),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID 'qux' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), authId("qux")(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID"),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID $qux }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), authId(parameter("qux", CTString))(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID"),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider 'foo' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      2,
      0,
      0
    )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        gql,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD 'password' }"
  ) {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth(
        "native",
        List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2), password(password)(pos3))
      )(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      1,
      0,
      0
    )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  test(
    "ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD 'password' }"
  ) {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth(
        "foo",
        List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2), password(password)(pos3))
      )(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      2,
      0,
      0
    )
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.", pos2)
      .errors
  }

  test("ALTER USER foo SET AUTH '' { SET PASSWORD '' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(password(passwordEmpty)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider '' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 = SemanticCheckResult.error(
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", ""),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    alterUser.semanticCheck.run(
      initialState,
      SemanticCheckContext.default
    ).errors shouldBe error1 ++ error2 ++ error3 // shouldBe error1 ++ error2 ++ error3
  }

  test("ALTER USER foo SET AUTH PROVIDER '' { SET ID '' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(authId("")(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Invalid input. Auth provider is not allowed to be an empty string.", pos1).errors
  }

  test("ALTER USER foo SET AUTH '' { SET PASSWORD 'password' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(password(password)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 =
      GqlHelper.getGql42001_22N04("SET PASSWORD", "auth provider '' attribute", java.util.List.of("SET ID"), 2, 0, 0)
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 = SemanticCheckResult.error(
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", ""),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("ALTER USER foo SET AUTH PROVIDER '' { SET ID 'bar' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(authId("bar")(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "Invalid input. Auth provider is not allowed to be an empty string.", pos1).errors
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 42 }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId(literalInt(42, pos3))(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(initialState, "id must be a String, or a String parameter.", pos3).errors
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER 42") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(literalInt(42, pos1)))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER [42, 69]") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalInt(42, pos2), literalInt(69, pos3))))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER ['bar', 69]") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalString("bar"), literalInt(69, pos3))))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER [69, 'bar']") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalInt(69, pos3), literalString("bar"))))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER []") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1)))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  test("ALTER USER foo SET AUTH 'foo' {SET PASSWORD CHANGE NOT REQUIRED} SET AUTH 'foo' {SET ID 'bar'}") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("foo", List(PasswordChange(requireChange = false)(pos1)))(pos2),
        Auth("foo", List(AuthId(literalString("bar"))(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'foo'"),
      initialState,
      "Duplicate `SET AUTH 'foo'` clause.",
      pos3
    ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      1,
      0,
      0
    )
    val error2 = SemanticCheckResult.error(
      gql2,
      initialState,
      "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo"),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos2
      ).errors
    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe error1 ++ error2 ++ error3
  }

  test("ALTER USER foo REMOVE ALL AUTH SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = true, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  test("ALTER USER foo REMOVE AUTH 'foo' SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List(literalString("foo")))
    )(p)

    alterUser.semanticCheck.run(initialState, SemanticCheckContext.default).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'"),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }
}
