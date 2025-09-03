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

import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AdministrationCommand.checkIsStringLiteralOrParameter
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.optionSemanticChecking
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

sealed trait AdministrationCommand extends StatementWithGraph with SemanticAnalysisTooling {

  def name: String

  // We parse USE to give a nice error message, but it's not considered to be a part of the AST
  private var useGraphVar: Option[UseGraph] = None
  def useGraph: Option[UseGraph] = useGraphVar

  override def withGraph(useGraph: Option[UseGraph]): AdministrationCommand = {
    this.useGraphVar = useGraph
    this
  }

  def isReadOnly: Boolean

  override def containsUpdates: Boolean = !isReadOnly

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position) chain
      when(useGraphVar.isDefined)(error(
        s"The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically.",
        position
      ))

  override def dup(children: Seq[AnyRef]): this.type =
    super.dup(children).withGraph(useGraph).asInstanceOf[this.type]
}

object AdministrationCommand {
  val NATIVE_AUTH = "native"

  private[ast] def checkIsStringLiteralOrParameter(value: String, expression: Expression): SemanticCheck =
    expression match {
      case _: StringLiteral                            => success
      case p: Parameter if p.parameterType == CTString => success
      case exp => SemanticCheck.error(SemanticError(s"$value must be a String, or a String parameter.", exp.position))
    }
}

sealed trait ReadAdministrationCommand extends AdministrationCommand {

  val isReadOnly: Boolean = true

  private[ast] val defaultColumnSet: List[ShowColumn]

  def returnColumnNames: List[String] = (yields, returns) match {
    case (_, Some(r))                => r.returnItems.items.map(ri => ri.alias.get.name).toList
    case (Some(resultColumns), None) => resultColumns.returnItems.items.map(ri => ri.alias.get.name).toList
    case (None, None)                => defaultColumnNames
  }

  def defaultColumnNames: List[String] = defaultColumnSet.map(_.name)

  def yieldOrWhere: YieldOrWhere = None
  def yields: Option[Yield] = yieldOrWhere.flatMap(yw => yw.left.toOption.map { case (y, _) => y })
  def returns: Option[Return] = yieldOrWhere.flatMap(yw => yw.left.toOption.flatMap { case (_, r) => r })
  def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ReadAdministrationCommand

  override def returnColumns: List[LogicalVariable] =
    returnColumnNames.map(name => Variable(name)(position, Variable.isIsolatedDefault))

  override def semanticCheck: SemanticCheck = SemanticCheck.nestedCheck {

    def checkForSubquery(astNode: ASTNode): SemanticCheck = {
      val invalid: Option[Expression] = astNode.folder.treeFind[Expression] {
        case _: SubqueryExpression => true
      }
      invalid.map {
        case exp: ExistsExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "EXISTS expression on SHOW commands",
            "The EXISTS expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: CollectExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "COLLECT expression on SHOW commands",
            "The COLLECT expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: CountExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "COUNT expression on SHOW commands",
            "The COUNT expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: PatternExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Pattern expressions on SHOW commands",
            "Pattern expressions are not valid on SHOW commands.",
            exp.position
          )
        case exp: PatternComprehension =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Pattern comprehensions on SHOW commands",
            "Pattern comprehensions are not valid on SHOW commands.",
            exp.position
          )
        case exp =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Subquery expressions on SHOW commands",
            "Subquery expressions are not valid on SHOW commands.",
            exp.position
          )
      }.getOrElse(success)
    }

    def checkProjection(r: ProjectionClause): SemanticCheck = {
      val check = r.semanticCheck
      for {
        closingResult <- check
        continuationResult <- r.semanticCheckContinuation(closingResult.state.currentScope.scope)
      } yield {
        semantics.SemanticCheckResult(continuationResult.state, closingResult.errors ++ continuationResult.errors)
      }
    }

    val initialChecks: SemanticCheck = super.semanticCheck
      .chain((state: SemanticState) => SemanticCheckResult.success(state.newChildScope))
      .chain(
        // Create variables for the columns generated by the command
        semanticCheckFold(defaultColumnSet)(sc => declareVariable(sc.variable, sc.cypherType))
      )
      .chain(checkForSubquery(this))

    val projectionChecks = Seq(yields, returns).foldSemanticCheck {
      maybeClause =>
        maybeClause.foldSemanticCheck(r =>
          checkProjection(r).chain(recordCurrentScope(r))
        )
    }

    initialChecks chain projectionChecks
  }
}

sealed trait WriteAdministrationCommand extends AdministrationCommand {
  val isReadOnly: Boolean = false
  override def returnColumns: List[LogicalVariable] = List.empty

  protected def topologyCheck(topology: Option[Topology], command: String): SemanticCheck = {

    def numPrimaryGreaterThanZero(topology: Topology): SemanticCheck =
      if (topology.primaries.flatMap(_.left.toOption).exists(_ < 1)) {
        val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
          .atPosition(position.line, position.column, position.offset)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N52)
            .atPosition(position.line, position.column, position.offset)
            .withParam(GqlParams.NumberParam.count, topology.primaries.flatMap(_.left.toOption).get)
            .withParam(GqlParams.NumberParam.upper, 11)
            .build())
          .build()
        error(
          gql,
          s"Failed to $command with `${Prettifier.extractTopology(topology).trim}`, PRIMARY must be greater than 0.",
          position
        )
      } else {
        SemanticCheck.success
      }

    def numSecondaryPositive(topology: Topology): SemanticCheck =
      if (topology.secondaries.flatMap(_.left.toOption).exists(_ < 0)) {
        val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
          .atPosition(position.line, position.column, position.offset)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N53)
            .atPosition(position.line, position.column, position.offset)
            .withParam(GqlParams.NumberParam.count, topology.primaries.flatMap(_.left.toOption).get)
            .withParam(GqlParams.NumberParam.upper, 20)
            .build())
          .build()
        error(
          gql,
          s"Failed to $command with `${Prettifier.extractTopology(topology).trim}`, SECONDARY must be a positive value.",
          position
        )
      } else {
        SemanticCheck.success
      }

    topology.map(topology => {
      numPrimaryGreaterThanZero(topology) chain
        numSecondaryPositive(topology)
    }).getOrElse(SemanticCheck.success)
  }
}

// User commands

final case class ShowUsers(
  override val yieldOrWhere: YieldOrWhere,
  withAuth: Boolean,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW USERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowUsers =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowUsers {

  def apply(yieldOrWhere: YieldOrWhere, withAuth: Boolean)(position: InputPosition): ShowUsers = {
    val baseColumns = List(
      ShowColumn("user")(position),
      ShowColumn("roles", CTList(CTString))(position),
      ShowColumn("passwordChangeRequired", CTBoolean)(position),
      ShowColumn("suspended", CTBoolean)(position),
      ShowColumn("home")(position)
    )
    val columns =
      if (withAuth) baseColumns ++ List(ShowColumn("provider")(position), ShowColumn("auth", CTMap)(position))
      else baseColumns
    ShowUsers(
      yieldOrWhere,
      withAuth,
      columns
    )(position)
  }
}

final case class ShowCurrentUser(
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW CURRENT USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowCurrentUser =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowCurrentUser {

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowCurrentUser =
    ShowCurrentUser(
      yieldOrWhere,
      List(
        ShowColumn("user")(position),
        ShowColumn("roles", CTList(CTString))(position),
        ShowColumn("passwordChangeRequired", CTBoolean)(position),
        ShowColumn("suspended", CTBoolean)(position),
        ShowColumn("home")(position)
      )
    )(position)
}

sealed trait UserAuth extends SemanticAnalysisTooling {
  protected def newStyleAuth: List[Auth]
  protected def oldStyleAuth: Option[Auth]

  protected val externalAuths: List[ExternalAuth] =
    newStyleAuth.filter(_.provider != NATIVE_AUTH).map(a => ExternalAuth(a.provider, a.authAttributes)(a.position))

  private val allNativeAuths: List[NativeAuth] =
    (newStyleAuth.filter(_.provider == NATIVE_AUTH) ++ oldStyleAuth).map(a => NativeAuth(a.authAttributes)(a.position))

  // semantic check makes sure at most one exists
  protected val nativeAuth: Option[NativeAuth] = allNativeAuths.headOption

  protected val allAuths: Seq[AuthImpl] = externalAuths ++ allNativeAuths

  protected def checkDuplicateAuth: SemanticCheck = newStyleAuth.groupBy(_.provider).collectFirst {
    case (_, List(_, duplicate, _*)) =>
      AdministrationCommandSemanticAnalysis.duplicateClauseError(
        s"SET AUTH '${duplicate.provider}'",
        s"Duplicate `SET AUTH '${duplicate.provider}'` clause.",
        duplicate.position
      )
  }.getOrElse(success)

  protected def checkOldAndNewStyleCombination: SemanticCheck = newStyleAuth.filter(_.provider == NATIVE_AUTH) match {
    case Seq(_, _*) if oldStyleAuth.nonEmpty =>
      error(
        "Cannot combine old and new auth syntax for the same auth provider.",
        oldStyleAuth.head.authAttributes.head.position
      )
    case _ => success
  }

  val useOldStyleNativeAuth: Boolean = oldStyleAuth.nonEmpty
}

final case class CreateUser(
  userName: Expression,
  userOptions: UserOptions,
  ifExistsDo: IfExistsDo,
  protected val newStyleAuth: List[Auth],
  protected val oldStyleAuth: Option[Auth]
)(val position: InputPosition) extends WriteAdministrationCommand with UserAuth {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE USER"
    case _                                       => "CREATE USER"
  }

  private def checkAtLeastOneAuth: SemanticCheck = if (allAuths.isEmpty) {
    error("No auth given for user.", position)
  } else success

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("user", userAsString, position))
    case _ =>
      checkAtLeastOneAuth chain
        checkDuplicateAuth chain
        checkOldAndNewStyleCombination chain
        allAuths.foldSemanticCheck(auth =>
          auth.checkDuplicates chain
            auth.checkRequiredAttributes chain // for external checks that ID exists and is string or parameter
            auth.checkNoUnsupportedAttributes chain
            auth.checkProviderName
        ) chain
        super.semanticCheck chain
        checkIsStringLiteralOrParameter("username", userName) chain
        SemanticState.recordCurrentScope(this)
  }

  private val userAsString: String = Prettifier.escapeName(userName)
}

object CreateUser {

  def unapply(c: CreateUser): Some[(Expression, UserOptions, IfExistsDo, List[ExternalAuth], Option[NativeAuth])] =
    Some((c.userName, c.userOptions, c.ifExistsDo, c.externalAuths, c.nativeAuth))
}

final case class DropUser(userName: Expression, ifExists: Boolean)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "DROP USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("username", userName) chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameUser(
  fromUserName: Expression,
  toUserName: Expression,
  ifExists: Boolean
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("from username", fromUserName) chain
      checkIsStringLiteralOrParameter("to username", toUserName) chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterUser(
  userName: Expression,
  userOptions: UserOptions,
  ifExists: Boolean,
  protected val newStyleAuth: List[Auth],
  protected val oldStyleAuth: Option[Auth],
  removeAuth: RemoveAuth
)(val position: InputPosition) extends WriteAdministrationCommand with UserAuth {

  override def name = "ALTER USER"

  private def checkAtLeastOneClause: SemanticCheck =
    if (userOptions.isEmpty && allAuths.isEmpty && removeAuth.isEmpty) {
      error("`ALTER USER` requires at least one clause.", position)
    } else {
      success
    }

  private def checkRemoveAuth: SemanticCheck =
    removeAuth.auths.foldSemanticCheck {
      case s: StringLiteral if s.value.nonEmpty => success
      case _: Parameter                         => success
      case list: ListLiteral
        if list.expressions.forall(e =>
          e.isInstanceOf[StringLiteral] && e.asInstanceOf[StringLiteral].value.nonEmpty
        ) && list.expressions.nonEmpty =>
        success
      case expr =>
        error("Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.", expr.position)
    }

  override def semanticCheck: SemanticCheck =
    checkAtLeastOneClause chain
      checkDuplicateAuth chain
      checkOldAndNewStyleCombination chain
      allAuths.foldSemanticCheck(auth =>
        auth.checkDuplicates chain
          auth.checkNoUnsupportedAttributes chain
          auth.checkProviderName
      ) chain
      externalAuths.foldSemanticCheck(_.checkIdIsStringLiteralOrParameter) chain
      checkRemoveAuth chain
      super.semanticCheck chain
      checkIsStringLiteralOrParameter("username", userName) chain
      SemanticState.recordCurrentScope(this)
}

object AlterUser {

  def unapply(a: AlterUser)
    : Some[(Expression, UserOptions, Boolean, List[ExternalAuth], Option[NativeAuth], RemoveAuth)] =
    Some((a.userName, a.userOptions, a.ifExists, a.externalAuths, a.nativeAuth, a.removeAuth))
}

final case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "ALTER CURRENT USER SET PASSWORD"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

case class RemoveAuth(all: Boolean, auths: List[Expression]) {
  def isEmpty: Boolean = auths.isEmpty && !all
  def nonEmpty: Boolean = !isEmpty
}

// Only used during parsing
case class Auth(provider: String, authAttributes: List[AuthAttribute])(val position: InputPosition) extends ASTNode

object allAuthAttr {

  val allAuthsAttributes: List[AuthAttribute] =
    List(Password(null, false)(null), PasswordChange(false)(null), AuthId(null)(null))
}

sealed trait AuthImpl extends ASTNode with SemanticAnalysisTooling {
  def authAttributes: List[AuthAttribute]
  def provider: String

  def checkRequiredAttributes: SemanticCheck
  def checkNoUnsupportedAttributes: SemanticCheck

  def checkDuplicates: SemanticCheck = authAttributes.groupBy(_.name).collectFirst {
    case (_, List(_, duplicate, _*)) =>
      AdministrationCommandSemanticAnalysis.duplicateClauseError(
        duplicate.name,
        s"Duplicate `${duplicate.name}` clause.",
        duplicate.position
      )
  }.getOrElse(success)

  def checkProviderName: SemanticCheck =
    if (provider.isEmpty) error("Invalid input. Auth provider is not allowed to be an empty string.", position)
    else success

  protected def requiredAttributes(func: AuthAttribute => Boolean, name: String): SemanticCheck =
    authAttributes.find(func) match {
      case Some(_) => success
      case None => AdministrationCommandSemanticAnalysis.missingMandatoryAuthClauseError(
          name,
          provider,
          missingRequiredClauseErrorMessage(name),
          position
        )
    }

  protected def noUnsupportedAttributes(func: AuthAttribute => Boolean): SemanticCheck = {
    authAttributes.find(func) match {
      case Some(unsupported) => {
        val supportedFunc: AuthAttribute => Boolean = x => !func(x)

        val supported =
          allAuthAttr.allAuthsAttributes.filter(supportedFunc).map(attr =>
            attr.name
          ) // TODO do you agree that this filtering should be inside the "case"? A little inefficient but only occurs when we have unsupported, better than doing outside. Also do we even want to give the allowed commands like this if they are passed?

        val pos = unsupported.position
        val expected = if (supported.isEmpty) java.util.List.of("no clause")
        else
          supported.map(
            String.valueOf(_)
          ).asJava
        SemanticCheck.error(SemanticError.authForbidsClauseError(provider, unsupported.name, expected, pos))
      }
      case None => success
    }
  }

  protected def missingRequiredClauseErrorMessage(name: String): String =
    s"Clause `$name` is mandatory for auth provider `$provider`."
}

final case class NativeAuth(authAttributes: List[AuthAttribute])(val position: InputPosition) extends AuthImpl {

  val provider: String = NATIVE_AUTH

  override def checkRequiredAttributes: SemanticCheck =
    requiredAttributes(attr => attr.isInstanceOf[Password], "SET PASSWORD")

  override def checkNoUnsupportedAttributes: SemanticCheck =
    noUnsupportedAttributes(attr => !attr.isInstanceOf[NativeAuthAttribute])

  def password: Option[Password] = authAttributes.collectFirst { case p: Password => p }

  def changeRequired: Option[Boolean] = authAttributes.collectFirst { case PasswordChange(change) => change }
}

final case class ExternalAuth(provider: String, authAttributes: List[AuthAttribute])(val position: InputPosition)
    extends AuthImpl {
  private val maybeId = authAttributes.collectFirst { case AuthId(id) => id }

  override def checkRequiredAttributes: SemanticCheck =
    requiredAttributes(attr => attr.isInstanceOf[AuthId], "SET ID") ifOkChain
      checkIdIsStringLiteralOrParameter

  override def checkNoUnsupportedAttributes: SemanticCheck =
    noUnsupportedAttributes(attr => !attr.isInstanceOf[ExternalAuthAttribute])

  def checkIdIsStringLiteralOrParameter: SemanticCheck =
    maybeId.map(id => checkIsStringLiteralOrParameter("id", id))
      .getOrElse(AdministrationCommandSemanticAnalysis.missingMandatoryAuthClauseError(
        "SET ID",
        provider,
        missingRequiredClauseErrorMessage("SET ID"),
        position
      ))

  // this is expected to only be called after checkRequiredAttributes has been called
  def id: Expression = maybeId.get
}

sealed trait AuthAttribute extends ASTNode {
  def position: InputPosition
  def name: String
}

sealed trait NativeAuthAttribute extends AuthAttribute
sealed trait ExternalAuthAttribute extends AuthAttribute

final case class Password(
  password: Expression,
  isEncrypted: Boolean
)(val position: InputPosition) extends NativeAuthAttribute {
  override val name: String = "SET PASSWORD"
}

final case class PasswordChange(
  requireChange: Boolean
)(val position: InputPosition) extends NativeAuthAttribute {
  override val name: String = "SET PASSWORD CHANGE [NOT] REQUIRED"
}

final case class AuthId(
  id: Expression
)(val position: InputPosition) extends ExternalAuthAttribute {
  override val name: String = "SET ID"
}

sealed trait HomeDatabaseAction
case object RemoveHomeDatabaseAction extends HomeDatabaseAction
final case class SetHomeDatabaseAction(name: DatabaseName) extends HomeDatabaseAction

final case class UserOptions(
  suspended: Option[Boolean],
  homeDatabase: Option[HomeDatabaseAction]
) {
  def isEmpty: Boolean = suspended.isEmpty && homeDatabase.isEmpty
}

// Role commands

final case class ShowRoles(
  withUsers: Boolean,
  showAll: Boolean,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowRoles =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowRoles {

  def apply(withUsers: Boolean, showAll: Boolean, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowRoles = {
    val allColumns =
      if (withUsers) List(
        (ShowColumn(Variable("role")(position, Variable.isIsolatedDefault), CTString, "role"), true),
        (ShowColumn(Variable("member")(position, Variable.isIsolatedDefault), CTString, "member"), true),
        (ShowColumn(Variable("immutable")(position, Variable.isIsolatedDefault), CTBoolean, "immutable"), false)
      )
      else List(
        (ShowColumn(Variable("role")(position, Variable.isIsolatedDefault), CTString, "role"), true),
        (ShowColumn(Variable("immutable")(position, Variable.isIsolatedDefault), CTBoolean, "immutable"), false)
      )
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowRoles(withUsers, showAll, yieldOrWhere, columns)(position)
  }
}

final case class CreateRole(
  roleName: Expression,
  immutable: Boolean,
  from: Option[Expression],
  ifExistsDo: IfExistsDo
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => s"CREATE OR REPLACE${Prettifier.maybeImmutable(immutable)} ROLE"
    case _                                       => s"CREATE${Prettifier.maybeImmutable(immutable)} ROLE"
  }

  override def semanticCheck: SemanticCheck =
    ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeName(roleName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("role", name, position))
      case _ =>
        super.semanticCheck chain
          checkIsStringLiteralOrParameter("rolename", roleName) chain
          semanticCheckFold(from)(roleName => checkIsStringLiteralOrParameter("from rolename", roleName)) chain
          SemanticState.recordCurrentScope(this)
    }
}

final case class DropRole(roleName: Expression, ifExists: Boolean)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "DROP ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("rolename", roleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameRole(
  fromRoleName: Expression,
  toRoleName: Expression,
  ifExists: Boolean
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("from rolename", fromRoleName) chain
      checkIsStringLiteralOrParameter("to rolename", toRoleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantRolesToUsers(
  roleNames: Seq[Expression],
  userNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "GRANT ROLE"

  override def semanticCheck: SemanticCheck = {
    super.semanticCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(userNames)(roleName => checkIsStringLiteralOrParameter("username", roleName)) chain
      SemanticState.recordCurrentScope(this)
  }
}

final case class RevokeRolesFromUsers(
  roleNames: Seq[Expression],
  userNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "REVOKE ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(userNames)(roleName => checkIsStringLiteralOrParameter("username", roleName)) chain
      SemanticState.recordCurrentScope(this)
}

// Privilege commands

final case class ShowPrivileges(
  scope: ShowPrivilegeScope,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivileges =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivileges {

  def apply(scope: ShowPrivilegeScope, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowPrivileges = {
    val columns = List(
      ShowColumn("access")(position),
      ShowColumn("action")(position),
      ShowColumn("resource")(position),
      ShowColumn("graph")(position),
      ShowColumn("segment")(position),
      ShowColumn("role")(position),
      ShowColumn("immutable", CTBoolean)(position)
    ) ++ (scope match {
      case _: ShowUserPrivileges | _: ShowUsersPrivileges => List(ShowColumn("user")(position))
      case _                                              => List.empty
    })
    ShowPrivileges(scope, yieldOrWhere, columns)(position)
  }
}

final case class ShowSupportedPrivilegeCommand(
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW SUPPORTED PRIVILEGES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowSupportedPrivilegeCommand =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowSupportedPrivilegeCommand {
  val ACTION: String = "action"
  val QUALIFIER: String = "qualifier"
  val TARGET: String = "target"
  val SCOPE: String = "scope"
  val DESCRIPTION: String = "description"

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowSupportedPrivilegeCommand = {
    val columns =
      List(
        ShowColumn(ACTION)(position),
        ShowColumn(QUALIFIER)(position),
        ShowColumn(TARGET)(position),
        ShowColumn(SCOPE, CTList(CTString))(position),
        ShowColumn(DESCRIPTION)(position)
      )
    ShowSupportedPrivilegeCommand(yieldOrWhere, columns)(position)
  }
}

final case class ShowPrivilegeCommands(
  scope: ShowPrivilegeScope,
  asRevoke: Boolean,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE COMMANDS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivilegeCommands =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivilegeCommands {

  def apply(
    scope: ShowPrivilegeScope,
    asRevoke: Boolean,
    yieldOrWhere: YieldOrWhere
  )(position: InputPosition): ShowPrivilegeCommands = {
    val allColumns =
      List((ShowColumn("command")(position), true), (ShowColumn("immutable", CTBoolean)(position), false))
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowPrivilegeCommands(scope, asRevoke, yieldOrWhere, columns)(position)
  }
}

//noinspection ScalaUnusedSymbol
sealed abstract class PrivilegeCommand(
  privilege: PrivilegeType,
  qualifier: List[PrivilegeQualifier],
  position: InputPosition
) extends WriteAdministrationCommand {

  private val FAILED_PROPERTY_RULE = "Failed to administer property rule."

  private def nanError(l: NaN) =
    error(s"$FAILED_PROPERTY_RULE `NaN` is not supported for property-based access control.", l.position)

  private def propertyAlwaysNullError(
    gqlBuilder: String => ErrorGqlStatusObject,
    predicate: String,
    pos: InputPosition,
    hint: String = ""
  ) = {
    error(
      gqlBuilder(predicate),
      s"$FAILED_PROPERTY_RULE The property value access rule pattern `$predicate` always evaluates to `NULL`.$hint",
      pos
    )
  }

  private def propertyPositionError(p: Property, operator: String) =
    error(
      s"$FAILED_PROPERTY_RULE The property `${p.propertyKey.name}` must appear on the left hand side of the `$operator` operator.",
      p.position
    )

  private def checkActionTypeForPropertyRules(privilegeType: PrivilegeType): SemanticCheck = {
    privilegeType match {
      case GraphPrivilege(action, _) => action match {
          case ReadAction | TraverseAction | MatchAction => SemanticCheck.success
          case _ =>
            SemanticCheck.error(SemanticError.unsupportedActionAccess(
              action.name,
              java.util.List.of(ReadAction.name, TraverseAction.name, MatchAction.name),
              position
            ))
        }
      case _ => SemanticCheck.error(SemanticError.notSupported(position)) // We should never end up here
    }
  }

  private def privilegeQualifierCheckForPropertyRules(qualifiers: List[PrivilegeQualifier]): SemanticCheck = {
    qualifiers.foldLeft(SemanticCheck.success)((acc, qualifier) => {
      acc.chain(qualifier match {
        case PatternQualifier(_, v, e) =>
          v.foldSemanticCheck(declareVariable(_, CTNode)) chain
            SemanticExpressionCheck.check(SemanticContext.Results, e) chain
            checkActionTypeForPropertyRules(privilege) chain
            checkExpression(e)
        case _ => SemanticCheck.success
      })
    })
  }

  private def checkExpression(expression: Expression) = {

    def stringifyExpression = {
      ExpressionStringifier.apply(_.asCanonicalStringVal).apply(expression)
    }

    def unsupportedExpression = s"$FAILED_PROPERTY_RULE The expression: `$stringifyExpression` is not supported. " +
      s"Only single, literal-based predicate expressions are allowed for property-based access control."

    def checkScalarExpression(value: Expression): SemanticCheck = {
      value match {
        case _: Literal | _: ExplicitParameter => SemanticCheck.success
        case f: FunctionInvocation
          if Seq("date", "datetime", "localdatetime", "localtime", "time", "duration", "point").contains(
            f.functionName.name
          ) =>
          SemanticCheck.success
        case _ => error(unsupportedExpression, expression.position)
      }
    }

    def checkListExpression(value: Expression): SemanticCheck = {
      value match {
        case ll: ListLiteral          => checkTypesInList(ll)
        case param: ExplicitParameter => SemanticCheck.success
        case _                        => error(unsupportedExpression, expression.position)
      }
    }

    def checkTypesInList(listLiteral: ListLiteral): SemanticCheck =
      if (
        listLiteral.expressions.forall {
          checkScalarExpression(_) == SemanticCheck.success
        }
      ) SemanticCheck.success
      else error(
        s"$FAILED_PROPERTY_RULE The expression: `$stringifyExpression` is not supported. " +
          s"All elements in a list must be literals of the same type for property-based access control.",
        expression.position
      )

    (expression match {
      case Not(e: BooleanExpression) => e
      case e                         => e
    }) match {
      case Equals(_: Property, l: NaN)             => nanError(l)
      case NotEquals(_: Property, l: NaN)          => nanError(l)
      case GreaterThan(_: Property, l: NaN)        => nanError(l)
      case GreaterThanOrEqual(_: Property, l: NaN) => nanError(l)
      case LessThan(_: Property, l: NaN)           => nanError(l)
      case LessThanOrEqual(_: Property, l: NaN)    => nanError(l)
      case Equals(l: NaN, _: Property)             => nanError(l)
      case NotEquals(l: NaN, _: Property)          => nanError(l)
      case GreaterThan(l: NaN, _: Property)        => nanError(l)
      case GreaterThanOrEqual(l: NaN, _: Property) => nanError(l)
      case LessThan(l: NaN, _: Property)           => nanError(l)
      case LessThanOrEqual(l: NaN, _: Property)    => nanError(l)
      case Equals(p: Property, l: Null) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA5,
          s"${p.propertyKey.name} = NULL",
          l.position,
          " Use `IS NULL` instead."
        )
      case NotEquals(p: Property, l: Null) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA6,
          s"${p.propertyKey.name} <> NULL",
          l.position,
          " Use `IS NOT NULL` instead."
        )
      case GreaterThan(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} > NULL", l.position)
      case GreaterThanOrEqual(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} >= NULL", l.position)
      case LessThan(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} < NULL", l.position)
      case LessThanOrEqual(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} <= NULL", l.position)
      case Equals(l: Null, p: Property) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA5,
          s"NULL = ${p.propertyKey.name}",
          l.position,
          " Use `IS NULL` instead."
        )
      case NotEquals(l: Null, p: Property) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA6,
          s"NULL <> ${p.propertyKey.name}",
          l.position,
          " Use `IS NOT NULL` instead."
        )
      case GreaterThan(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL > ${p.propertyKey.name}", l.position)
      case GreaterThanOrEqual(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL >= ${p.propertyKey.name}", l.position)
      case LessThan(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL < ${p.propertyKey.name}", l.position)
      case LessThanOrEqual(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL <= ${p.propertyKey.name}", l.position)
      case Equals(_, p: Property)             => propertyPositionError(p, "=")
      case NotEquals(_, p: Property)          => propertyPositionError(p, "<>")
      case GreaterThan(_, p: Property)        => propertyPositionError(p, ">")
      case GreaterThanOrEqual(_, p: Property) => propertyPositionError(p, ">=")
      case LessThan(_, p: Property)           => propertyPositionError(p, "<")
      case LessThanOrEqual(_, p: Property)    => propertyPositionError(p, "<=")
      case map @ MapExpression(items) if items.size > 1 =>
        error(
          s"$FAILED_PROPERTY_RULE The expression: `$stringifyExpression` is not supported. Property rules can only contain one property.",
          map.position
        )
      case MapExpression(Seq((pk: PropertyKeyName, l: Null))) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NB0,
          s"{${pk.name}:NULL}",
          l.position,
          " Use `WHERE` syntax in combination with `IS NULL` instead."
        )
      case Equals(_: Property, e: Expression)                      => checkScalarExpression(e)
      case NotEquals(_: Property, e: Expression)                   => checkScalarExpression(e)
      case In(_: Property, e: Expression)                          => checkListExpression(e)
      case Not(In(_: Property, e: Expression))                     => checkListExpression(e)
      case IsNull(_: Property) | IsNotNull(_: Property)            => SemanticCheck.success
      case MapExpression(Seq((_: PropertyKeyName, e: Expression))) => checkScalarExpression(e)
      case GreaterThan(_: Property, e: Expression)                 => checkScalarExpression(e)
      case GreaterThanOrEqual(_: Property, e: Expression)          => checkScalarExpression(e)
      case LessThan(_: Property, e: Expression)                    => checkScalarExpression(e)
      case LessThanOrEqual(_: Property, e: Expression)             => checkScalarExpression(e)
      case _                                                       => error(unsupportedExpression, expression.position)
    }
  }

  override def semanticCheck: SemanticCheck = {
    val showSettingFeatureCheck = privilege match {
      case DbmsPrivilege(ShowSettingAction) =>
        requireFeatureSupport(s"The `$name` clause", SemanticFeature.ShowSetting, position)
      case _ => SemanticCheck.success
    }

    (privilege match {
      case DbmsPrivilege(u: UnassignableAction) =>
        SemanticCheck.error(SemanticError.grantDenyRevokeUnsupported(u.name, position))
      case _: LoadPrivilege =>
        qualifier match {
          case LoadUrlQualifier(_) :: _ =>
            error("LOAD privileges with a URL pattern are not currently supported", position)
          case _ => super.semanticCheck chain SemanticState.recordCurrentScope(this)
        }
      case _ => showSettingFeatureCheck chain super.semanticCheck chain
          SemanticState.recordCurrentScope(this)
    }) chain privilegeQualifierCheckForPropertyRules(qualifier)
  }
}

final case class GrantPrivilege(
  privilege: PrivilegeType,
  immutable: Boolean,
  resource: Option[ActionResourceBase],
  qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression]
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {
  override def name = s"GRANT${Prettifier.maybeImmutable(immutable)} ${privilege.name}"

  override def semanticCheck: SemanticCheck = super.semanticCheck chain semanticCheckFold(roleNames)(roleName =>
    checkIsStringLiteralOrParameter("rolename", roleName)
  )
}

object GrantPrivilege {

  def dbmsAction(
    action: DbmsAction,
    immutable: Boolean,
    roleNames: Seq[Expression],
    qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
  ): InputPosition => GrantPrivilege =
    GrantPrivilege(DbmsPrivilege(action)(InputPosition.NONE), immutable, None, qualifier, roleNames)

  def databaseAction(
    action: DatabaseAction,
    immutable: Boolean,
    scope: DatabaseScope,
    roleNames: Seq[Expression],
    qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))
  ): InputPosition => GrantPrivilege =
    GrantPrivilege(DatabasePrivilege(action, scope)(InputPosition.NONE), immutable, None, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](
    action: GraphAction,
    immutable: Boolean,
    resource: Option[ActionResourceBase],
    scope: GraphScope,
    qualifier: List[T],
    roleNames: Seq[Expression]
  ): InputPosition => GrantPrivilege =
    GrantPrivilege(GraphPrivilege(action, scope)(InputPosition.NONE), immutable, resource, qualifier, roleNames)
}

final case class DenyPrivilege(
  privilege: PrivilegeType,
  immutable: Boolean,
  resource: Option[ActionResourceBase],
  qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression]
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY${Prettifier.maybeImmutable(immutable)} ${privilege.name}"

  override def semanticCheck: SemanticCheck = {
    privilege match {
      case GraphPrivilege(MergeAdminAction, _) =>
        SemanticCheck.error(SemanticError.denyMergeUnsupported(position))
      case _ => super.semanticCheck
    }
  }
}

object DenyPrivilege {

  def dbmsAction(
    action: DbmsAction,
    immutable: Boolean,
    roleNames: Seq[Expression],
    qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
  ): InputPosition => DenyPrivilege =
    DenyPrivilege(DbmsPrivilege(action)(InputPosition.NONE), immutable, None, qualifier, roleNames)

  def databaseAction(
    action: DatabaseAction,
    immutable: Boolean,
    scope: DatabaseScope,
    roleNames: Seq[Expression],
    qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))
  ): InputPosition => DenyPrivilege =
    DenyPrivilege(DatabasePrivilege(action, scope)(InputPosition.NONE), immutable, None, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](
    action: GraphAction,
    immutable: Boolean,
    resource: Option[ActionResourceBase],
    scope: GraphScope,
    qualifier: List[T],
    roleNames: Seq[Expression]
  ): InputPosition => DenyPrivilege =
    DenyPrivilege(GraphPrivilege(action, scope)(InputPosition.NONE), immutable, resource, qualifier, roleNames)
}

final case class RevokePrivilege(
  privilege: PrivilegeType,
  immutableOnly: Boolean,
  resource: Option[ActionResourceBase],
  qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression],
  revokeType: RevokeType
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name: String = {
    val revokeTypeOrEmptyString = if (revokeType.name.nonEmpty) s" ${revokeType.name}" else ""
    s"REVOKE$revokeTypeOrEmptyString${Prettifier.maybeImmutable(immutableOnly)} ${privilege.name}"
  }

  override def semanticCheck: SemanticCheck = {
    (privilege, revokeType) match {
      case (GraphPrivilege(MergeAdminAction, _), RevokeDenyType()) =>
        SemanticCheck.error(SemanticError.denyMergeUnsupported(position))
      case _ => super.semanticCheck chain semanticCheckFold(roleNames)(roleName =>
          checkIsStringLiteralOrParameter("rolename", roleName)
        )
    }
  }

}

object RevokePrivilege {

  def dbmsAction(
    action: DbmsAction,
    immutable: Boolean,
    roleNames: Seq[Expression],
    revokeType: RevokeType,
    qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
  ): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), immutable, None, qualifier, roleNames, revokeType)

  def databaseAction(
    action: DatabaseAction,
    immutable: Boolean,
    scope: DatabaseScope,
    roleNames: Seq[Expression],
    revokeType: RevokeType,
    qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))
  ): InputPosition => RevokePrivilege =
    RevokePrivilege(
      DatabasePrivilege(action, scope)(InputPosition.NONE),
      immutable,
      None,
      qualifier,
      roleNames,
      revokeType
    )

  def graphAction[T <: GraphPrivilegeQualifier](
    action: GraphAction,
    immutable: Boolean,
    resource: Option[ActionResourceBase],
    scope: GraphScope,
    qualifier: List[T],
    roleNames: Seq[Expression],
    revokeType: RevokeType
  ): InputPosition => RevokePrivilege =
    RevokePrivilege(
      GraphPrivilege(action, scope)(InputPosition.NONE),
      immutable,
      resource,
      qualifier,
      roleNames,
      revokeType
    )
}

// Server commands

final case class EnableServer(serverName: Either[String, Parameter], optionsMap: Options)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "ENABLE SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterServer(serverName: Either[String, Parameter], optionsMap: Options)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "ALTER SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameServer(serverName: Either[String, Parameter], newName: Either[String, Parameter])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "RENAME SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropServer(serverName: Either[String, Parameter])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "DROP SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowServers(override val yieldOrWhere: YieldOrWhere, defaultColumns: DefaultOrAllShowColumns)(
  val position: InputPosition
) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = "SHOW SERVERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowServers =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowServers {

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowServers = {
    val showColumns = List(
      (ShowColumn("serverId")(position), false),
      (ShowColumn("name")(position), true),
      (ShowColumn("address")(position), true),
      (ShowColumn("httpAddress")(position), false),
      (ShowColumn("httpsAddress")(position), false),
      (ShowColumn("state")(position), true),
      (ShowColumn("health")(position), true),
      (ShowColumn("hosting", CTList(CTString))(position), true),
      (ShowColumn("requestedHosting", CTList(CTString))(position), false),
      (ShowColumn("tags", CTList(CTString))(position), false),
      (ShowColumn("allowedDatabases", CTList(CTString))(position), false),
      (ShowColumn("deniedDatabases", CTList(CTString))(position), false),
      (ShowColumn("modeConstraint")(position), false),
      (ShowColumn("version")(position), false)
    )
    val briefShowColumns = showColumns.filter(_._2).map(_._1)
    val allShowColumns = showColumns.map(_._1)

    val allColumns = yieldOrWhere match {
      case Some(Left(_)) => true
      case _             => false
    }
    val columns = DefaultOrAllShowColumns(allColumns, briefShowColumns, allShowColumns)
    ShowServers(yieldOrWhere, columns)(position)
  }
}

final case class DeallocateServers(dryRun: Boolean, serverNames: Seq[Either[String, Parameter]])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "DEALLOCATE DATABASES FROM SERVER"

  override val isReadOnly: Boolean = dryRun

  override def returnColumns: List[LogicalVariable] =
    if (dryRun) {
      List(
        Variable("database")(position, Variable.isIsolatedDefault),
        Variable("fromServerName")(position, Variable.isIsolatedDefault),
        Variable("fromServerId")(position, Variable.isIsolatedDefault),
        Variable("toServerName")(position, Variable.isIsolatedDefault),
        Variable("toServerId")(position, Variable.isIsolatedDefault),
        Variable("mode")(position, Variable.isIsolatedDefault)
      )
    } else List.empty

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ReallocateDatabases(dryRun: Boolean)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "REALLOCATE DATABASES"

  override val isReadOnly: Boolean = dryRun

  override def returnColumns: List[LogicalVariable] =
    if (dryRun) {
      List(
        Variable("database")(position, Variable.isIsolatedDefault),
        Variable("fromServerName")(position, Variable.isIsolatedDefault),
        Variable("fromServerId")(position, Variable.isIsolatedDefault),
        Variable("toServerName")(position, Variable.isIsolatedDefault),
        Variable("toServerId")(position, Variable.isIsolatedDefault),
        Variable("mode")(position, Variable.isIsolatedDefault)
      )
    } else List.empty

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

// Database commands

final case class ShowDatabase(
  scope: DatabaseScope,
  override val yieldOrWhere: YieldOrWhere,
  defaultColumns: DefaultOrAllShowColumns
)(val position: InputPosition) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = scope match {
    case _: SingleNamedDatabaseScope                   => "SHOW DATABASE"
    case _: AllDatabasesScope | _: NamedDatabasesScope => "SHOW DATABASES"
    case _: DefaultDatabaseScope                       => "SHOW DEFAULT DATABASE"
    case _: HomeDatabaseScope                          => "SHOW HOME DATABASE"
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowDatabase =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowDatabase {

  // Provided by the cypher stack - must be the same for all rows of a database
  val ALIASES_COL = "aliases"
  val REQUESTED_STATUS_COL = "requestedStatus"
  val DEFAULT_COL = "default"
  val HOME_COL = "home"
  val REQUESTED_PRIMARIES_COUNT_COL = "requestedPrimariesCount"
  val REQUESTED_SECONDARIES_COUNT_COL = "requestedSecondariesCount"
  val CREATION_TIME_COL = "creationTime"
  val LAST_START_TIME_COL = "lastStartTime"
  val LAST_STOP_TIME_COL = "lastStopTime"
  val CONSTITUENTS_COL = "constituents"

  // Provided by TopologyInfoService - same for every row for a database
  val NAME_COL = "name"
  val TYPE_COL = "type"
  val CURRENT_PRIMARIES_COUNT_COL = "currentPrimariesCount"
  val CURRENT_SECONDARIES_COUNT_COL = "currentSecondariesCount"
  val OPTIONS_COL = "options"

  // Provided by TopologyInfoService - if present must be the same for every row for a database
  val DATABASE_ID_COL = "databaseID"
  val STORE_COL = "store"

  // Provided by TopologyInfoService - can/will/must be different for every row for a database
  val ACCESS_COL = "access"
  val ROLE_COL = "role"
  val WRITER_COL = "writer"
  val CURRENT_STATUS_COL = "currentStatus"
  val STATUS_MSG_COL = "statusMessage"
  val LAST_COMMITTED_TX_COL = "lastCommittedTxn"
  val REPLICATION_LAG_COL = "replicationLag"
  val SERVER_ID_COL = "serverID"
  val ADDRESS_COL = "address"

  def apply(scope: DatabaseScope, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowDatabase = {
    val showColumns = List(
      // (column, brief)
      (ShowColumn(NAME_COL)(position), true),
      (ShowColumn(TYPE_COL)(position), true),
      (ShowColumn(ALIASES_COL, CTList(CTString))(position), true),
      (ShowColumn(ACCESS_COL)(position), true),
      (ShowColumn(DATABASE_ID_COL)(position), false),
      (ShowColumn(SERVER_ID_COL)(position), false),
      (ShowColumn(ADDRESS_COL)(position), true),
      (ShowColumn(ROLE_COL)(position), true),
      (ShowColumn(WRITER_COL, CTBoolean)(position), true),
      (ShowColumn(REQUESTED_STATUS_COL)(position), true),
      (ShowColumn(CURRENT_STATUS_COL)(position), true),
      (ShowColumn(STATUS_MSG_COL)(position), true)
    ) ++ (scope match {
      case _: DefaultDatabaseScope => List.empty
      case _: HomeDatabaseScope    => List.empty
      case _ =>
        List((ShowColumn(DEFAULT_COL, CTBoolean)(position), true), (ShowColumn(HOME_COL, CTBoolean)(position), true))
    }) ++ List(
      (ShowColumn(CURRENT_PRIMARIES_COUNT_COL, CTInteger)(position), false),
      (ShowColumn(CURRENT_SECONDARIES_COUNT_COL, CTInteger)(position), false),
      (ShowColumn(REQUESTED_PRIMARIES_COUNT_COL, CTInteger)(position), false),
      (ShowColumn(REQUESTED_SECONDARIES_COUNT_COL, CTInteger)(position), false),
      (ShowColumn(CREATION_TIME_COL, CTDateTime)(position), false),
      (ShowColumn(LAST_START_TIME_COL, CTDateTime)(position), false),
      (ShowColumn(LAST_STOP_TIME_COL, CTDateTime)(position), false),
      (ShowColumn(STORE_COL)(position), false),
      (ShowColumn(LAST_COMMITTED_TX_COL, CTInteger)(position), false),
      (ShowColumn(REPLICATION_LAG_COL, CTInteger)(position), false),
      (ShowColumn(CONSTITUENTS_COL, CTList(CTString))(position), true),
      (ShowColumn(OPTIONS_COL, CTMap)(position), false)
    )

    ShowDatabase(scope, yieldOrWhere, DefaultOrAllShowColumns(showColumns, yieldOrWhere))(position)
  }
}

final case class CreateDatabase(
  dbName: DatabaseName,
  ifExistsDo: IfExistsDo,
  options: Options,
  waitUntilComplete: WaitUntilComplete,
  topology: Option[Topology]
)(val position: InputPosition)
    extends WaitableAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE DATABASE"
    case _                                       => "CREATE DATABASE"
  }

  override def semanticCheck: SemanticCheck = (ifExistsDo match {
    case IfExistsInvalidSyntax =>
      val name = Prettifier.escapeName(dbName)
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("database", name, position))
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  })
    .chain(topologyCheck(topology, name))
}

case class Topology(primaries: Option[Either[Int, Parameter]], secondaries: Option[Either[Int, Parameter]])

final case class CreateCompositeDatabase(
  databaseName: DatabaseName,
  ifExistsDo: IfExistsDo,
  options: Options,
  waitUntilComplete: WaitUntilComplete
)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE COMPOSITE DATABASE"
    case _                                       => "CREATE COMPOSITE DATABASE"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      val name = Prettifier.escapeName(databaseName)
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("composite database", name, position))
    case _ =>
      databaseName match {
        case nsn @ NamespacedName(_, Some(_)) =>
          AdministrationCommandSemanticAnalysis.inputContainsInvalidCharactersError(
            nsn.toString,
            "composite database name",
            s"Failed to create the specified composite database '${nsn.toString}': COMPOSITE DATABASE names cannot contain \".\". " +
              "COMPOSITE DATABASE names using '.' must be quoted with backticks e.g. `composite.database`.",
            nsn.position
          )
        case _ => super.semanticCheck
      }
  }
}

final case class DropDatabase(
  dbName: DatabaseName,
  ifExists: Boolean,
  composite: Boolean,
  aliasAction: DropDatabaseAliasAction,
  additionalAction: DropDatabaseAdditionalAction,
  waitUntilComplete: WaitUntilComplete
)(val position: InputPosition) extends WaitableAdministrationCommand {

  override def name: String = if (composite) "DROP COMPOSITE DATABASE" else "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterDatabase(
  dbName: DatabaseName,
  ifExists: Boolean,
  access: Option[Access],
  topology: Option[Topology],
  options: Options,
  optionsToRemove: Set[String],
  waitUntilComplete: WaitUntilComplete
)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name = "ALTER DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      topologyCheck(topology, name)
}

final case class StartDatabase(dbName: DatabaseName, waitUntilComplete: WaitUntilComplete)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: DatabaseName, waitUntilComplete: WaitUntilComplete)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

sealed trait WaitableAdministrationCommand extends WriteAdministrationCommand {
  val waitUntilComplete: WaitUntilComplete

  override def returnColumns: List[LogicalVariable] = waitUntilComplete match {
    case NoWait => List.empty
    case _      => List("address", "state", "message", "success").map(Variable(_)(position, Variable.isIsolatedDefault))
  }
}

sealed trait WaitUntilComplete {
  val DEFAULT_TIMEOUT = 300L
  val name: String
  def timeout: Long = DEFAULT_TIMEOUT
}

case object NoWait extends WaitUntilComplete {
  override val name: String = ""
}

case object IndefiniteWait extends WaitUntilComplete {
  override val name: String = " WAIT"
}

case class TimeoutAfter(timoutSeconds: Long) extends WaitUntilComplete {
  override val name: String = s" WAIT $timoutSeconds SECONDS"
  override def timeout: Long = timoutSeconds
}

sealed trait Access
case object ReadOnlyAccess extends Access
case object ReadWriteAccess extends Access

sealed abstract class DropDatabaseAdditionalAction(val name: String)
case object DumpData extends DropDatabaseAdditionalAction("DUMP DATA")
case object DestroyData extends DropDatabaseAdditionalAction("DESTROY DATA")

sealed abstract class DropDatabaseAliasAction(val name: String)
case object Restrict extends DropDatabaseAliasAction("RESTRICT")
case object CascadeAliases extends DropDatabaseAliasAction("CASCADE ALIASES")

// Alias commands

final case class ShowAliases(
  aliasName: Option[DatabaseName],
  override val yieldOrWhere: YieldOrWhere,
  defaultColumns: DefaultOrAllShowColumns
)(
  val position: InputPosition
) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = aliasName match {
    case None    => "SHOW ALIASES"
    case Some(_) => "SHOW ALIAS"
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowAliases =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowAliases {

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowAliases = apply(None, yieldOrWhere)(position)

  def apply(
    aliasName: Option[DatabaseName],
    yieldOrWhere: YieldOrWhere
  )(position: InputPosition): ShowAliases = {
    val showColumns = List(
      // (column, brief)
      (ShowColumn("name")(position), true),
      (ShowColumn("composite")(position), true),
      (ShowColumn("database")(position), true),
      (ShowColumn("location")(position), true),
      (ShowColumn("url")(position), true),
      (ShowColumn("user")(position), true),
      (ShowColumn("driver", CTMap)(position), false),
      (ShowColumn("properties", CTMap)(position), false)
    )

    ShowAliases(aliasName, yieldOrWhere, DefaultOrAllShowColumns(showColumns, yieldOrWhere))(position)
  }
}

object AliasDriverSettingsCheck {
  val existsErrorMessage = "The EXISTS expression is not valid in driver settings."
  val countErrorMessage = "The COUNT expression is not valid in driver settings."
  val collectErrorMessage = "The COLLECT expression is not valid in driver settings."
  val genericErrorMessage = "This expression is not valid in driver settings."

  def findInvalidDriverSettings(driverSettings: Option[Either[Map[String, Expression], Parameter]])
    : Option[Expression] = {
    driverSettings match {
      case Some(Left(settings)) =>
        settings.values.flatMap(s =>
          s.folder.treeFind[Expression] {
            case _: ExistsExpression  => true
            case _: CollectExpression => true
            case _: CountExpression   => true
          }
        ).headOption
      case _ => None
    }
  }
}

final case class CreateLocalDatabaseAlias(
  aliasName: DatabaseName,
  targetName: DatabaseName,
  ifExistsDo: IfExistsDo,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ALIAS"
    case _                                       => "CREATE ALIAS"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists(
        "alias",
        Prettifier.escapeName(aliasName),
        position
      ))
    case _ => super.semanticCheck chain
        namespacedNameHasNoDots chain
        SemanticState.recordCurrentScope(this)
  }

  private def namespacedNameHasNoDots: SemanticCheck = aliasName match {
    case nsn @ NamespacedName(nameComponents, Some(_)) =>
      if (nameComponents.length > 1) AdministrationCommandSemanticAnalysis.inputContainsInvalidCharactersError(
        nsn.toString,
        "local alias name",
        s"'.' is not a valid character in the local alias name '${nsn.toString}'. " +
          "Local alias names using '.' must be quoted with backticks when adding a local alias to a composite database e.g. `local.alias`.",
        nsn.position
      )
      else success
    case _ => success
  }
}

final case class CreateRemoteDatabaseAlias(
  aliasName: DatabaseName,
  targetName: DatabaseName,
  ifExistsDo: IfExistsDo,
  url: Either[String, Parameter],
  username: Expression,
  password: Expression,
  driverSettings: Option[Either[Map[String, Expression], Parameter]] = None,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ALIAS"
    case _                                       => "CREATE ALIAS"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists(
        "alias",
        Prettifier.escapeName(aliasName),
        position
      ))
    case _ => AliasDriverSettingsCheck.findInvalidDriverSettings(driverSettings) match {
        case Some(expr: ExistsExpression) =>
          SemanticCheck.error(SemanticError.existsInDriverSettings(expr.position))
        case Some(expr: CountExpression) =>
          SemanticCheck.error(SemanticError.countInDriverSettings(expr.position))
        case Some(expr: CollectExpression) =>
          SemanticCheck.error(SemanticError.collectInDriverSettings(expr.position))
        case Some(expr) =>
          SemanticCheck.error(SemanticError.genericDriverSettingsFail(expr.position))
        // Apparently this should not happen, but if you have a better message do tell
        case _ => super.semanticCheck chain checkIsStringLiteralOrParameter(
            "username",
            username
          ) chain SemanticState.recordCurrentScope(this)
      }
  }
}

final case class AlterLocalDatabaseAlias(
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  ifExists: Boolean = false,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER ALIAS"

  override def semanticCheck: SemanticCheck = super.semanticCheck chain SemanticState.recordCurrentScope(this)
}

final case class AlterRemoteDatabaseAlias(
  aliasName: DatabaseName,
  targetName: Option[DatabaseName] = None,
  ifExists: Boolean = false,
  url: Option[Either[String, Parameter]] = None,
  username: Option[Expression] = None,
  password: Option[Expression] = None,
  driverSettings: Option[Either[Map[String, Expression], Parameter]] = None,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER ALIAS"

  override def semanticCheck: SemanticCheck =
    AliasDriverSettingsCheck.findInvalidDriverSettings(driverSettings) match {
      case Some(expr) =>
        expr match {
          case _: ExistsExpression =>
            SemanticCheck.error(SemanticError.existsInDriverSettings(expr.position))
          case _: CountExpression =>
            SemanticCheck.error(SemanticError.countInDriverSettings(expr.position))
          case _: CollectExpression =>
            SemanticCheck.error(SemanticError.collectInDriverSettings(expr.position))
          case _ =>
            SemanticCheck.error(SemanticError.genericDriverSettingsFail(expr.position))
        }
      case _ =>
        val isLocalAlias = targetName.isDefined && url.isEmpty
        val isRemoteAlias = url.isDefined || username.isDefined || password.isDefined || driverSettings.isDefined
        if (isLocalAlias && isRemoteAlias) {
          AdministrationCommandSemanticAnalysis.invalidInputError(
            Prettifier.escapeName(aliasName),
            "database alias",
            List("url of a remote alias target"),
            s"Failed to alter the specified database alias '${Prettifier.escapeName(aliasName)}': url needs to be defined to alter a remote alias target.",
            position
          )
        } else {
          super.semanticCheck chain semanticCheckFold(username)(un =>
            checkIsStringLiteralOrParameter("username", un)
          ) chain SemanticState.recordCurrentScope(this)
        }
    }
}

final case class DropDatabaseAlias(aliasName: DatabaseName, ifExists: Boolean)(
  val position: InputPosition
) extends WriteAdministrationCommand {

  override def name = "DROP ALIAS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
