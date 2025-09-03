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

package org.neo4j.cypher.internal.parser.v25.ast.factory

import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.AuthAttribute
import org.neo4j.cypher.internal.ast.AuthId
import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropDatabaseAliasAction
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astOptFromList
import org.neo4j.cypher.internal.parser.ast.util.Util.astPairs
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.ast.util.Util.rangePos
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.Cypher25ParserListener
import org.neo4j.cypher.internal.util.symbols.CTString

import java.nio.charset.StandardCharsets

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.ListHasAsScala

trait DdlBuilder extends Cypher25ParserListener {

  override def exitCommandOptions(ctx: Cypher25Parser.CommandOptionsContext): Unit = {
    val map = ctx.mapOrParameter().ast[Either[Map[String, Expression], Parameter]]()
    ctx.ast = map match {
      case Left(m)  => OptionsMap(m)
      case Right(p) => OptionsParam(p)
    }
  }

  final override def exitMapOrParameter(
    ctx: Cypher25Parser.MapOrParameterContext
  ): Unit = {
    val map = ctx.map()
    ctx.ast = if (map != null) {
      Left[Map[String, Expression], Parameter](map.ast[MapExpression]().items.map(x => (x._1.name, x._2)).toMap)
    } else {
      Right[Map[String, Expression], Parameter](ctx.parameter().ast())
    }
  }

  final override def exitCommand(
    ctx: Cypher25Parser.CommandContext
  ): Unit = {
    val useCtx = ctx.useClause()
    ctx.ast = lastChild[AstRuleCtx](ctx) match {
      case c: Cypher25Parser.ShowCommandContext => c.ast match {
          case sQ: SingleQuery if useCtx != null => SingleQuery(useCtx.ast[UseGraph]() +: sQ.clauses)(pos(ctx))
          case command: StatementWithGraph if useCtx != null => command.withGraph(Some(useCtx.ast()))
          case a                                             => a
        }
      case c: Cypher25Parser.TerminateCommandContext =>
        if (useCtx != null) SingleQuery(useCtx.ast[UseGraph]() +: c.ast[Seq[Clause]]())(pos(ctx))
        else SingleQuery(c.ast[Seq[Clause]]())(pos(ctx))
      case c => c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](useCtx))
    }
  }

  final override def exitDropCommand(
    ctx: Cypher25Parser.DropCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  // Constraint and index command contexts

  final override def exitCommandNodePattern(
    ctx: Cypher25Parser.CommandNodePatternContext
  ): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      ctx.labelType.ast[LabelName]()
    )
  }

  final override def exitCommandRelPattern(
    ctx: Cypher25Parser.CommandRelPatternContext
  ): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      ctx.relType().ast[RelTypeName]()
    )
  }

  final override def exitDropConstraint(
    ctx: Cypher25Parser.DropConstraintContext
  ): Unit = {
    val p = pos(ctx.getParent)
    val constraintName = ctx.symbolicNameOrStringParameter()
    ctx.ast = DropConstraintOnName(constraintName.ast(), ctx.EXISTS() != null)(p)
  }

  final override def exitDropIndex(
    ctx: Cypher25Parser.DropIndexContext
  ): Unit = {
    val indexName = ctx.symbolicNameOrStringParameter()
    ctx.ast = DropIndexOnName(indexName.ast[Either[String, Parameter]](), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitPropertyList(
    ctx: Cypher25Parser.PropertyListContext
  ): Unit = {
    val enclosed = ctx.enclosedPropertyList()
    ctx.ast =
      if (enclosed != null) enclosed.ast[Seq[Property]]()
      else ArraySeq(Property(ctx.variable().ast[Expression], ctx.property().ast[PropertyKeyName])(pos(ctx)))
  }

  final override def exitEnclosedPropertyList(
    ctx: Cypher25Parser.EnclosedPropertyListContext
  ): Unit = {
    ctx.ast = astPairs[Expression, PropertyKeyName](ctx.variable(), ctx.property())
      .map { case (e, p) => Property(e, p)(e.position) }
  }

  // Admin command contexts (ordered as in parser file)

  final override def exitAlterCommand(
    ctx: Cypher25Parser.AlterCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitRenameCommand(
    ctx: Cypher25Parser.RenameCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  // Server command contexts

  final override def exitEnableServerCommand(
    ctx: Cypher25Parser.EnableServerCommandContext
  ): Unit = {
    ctx.ast = EnableServer(ctx.stringOrParameter().ast(), astOpt[Options](ctx.commandOptions(), NoOptions))(pos(ctx))
  }

  final override def exitAlterServer(
    ctx: Cypher25Parser.AlterServerContext
  ): Unit = {
    ctx.ast = AlterServer(
      ctx.stringOrParameter().ast[Either[String, Parameter]](),
      ctx.commandOptions().ast()
    )(pos(ctx.getParent))
  }

  final override def exitRenameServer(
    ctx: Cypher25Parser.RenameServerContext
  ): Unit = {
    val names = ctx.stringOrParameter()
    ctx.ast = RenameServer(
      names.get(0).ast[Either[String, Parameter]](),
      names.get(1).ast[Either[String, Parameter]]()
    )(pos(ctx.getParent))
  }

  final override def exitDropServer(
    ctx: Cypher25Parser.DropServerContext
  ): Unit = {
    ctx.ast = DropServer(ctx.stringOrParameter().ast[Either[String, Parameter]])(pos(ctx.getParent))
  }

  final override def exitAllocationCommand(
    ctx: Cypher25Parser.AllocationCommandContext
  ): Unit = {
    val dryRun = ctx.DRYRUN() != null
    ctx.ast = if (ctx.reallocateDatabases() != null) {
      ReallocateDatabases(dryRun)(pos(ctx.reallocateDatabases()))
    } else {
      DeallocateServers(
        dryRun,
        ctx.deallocateDatabaseFromServers().ast()
      )(pos(ctx.deallocateDatabaseFromServers()))
    }
  }

  final override def exitDeallocateDatabaseFromServers(
    ctx: Cypher25Parser.DeallocateDatabaseFromServersContext
  ): Unit = {
    ctx.ast = astSeq[Either[String, Parameter]](ctx.stringOrParameter())
  }

  final override def exitReallocateDatabases(
    ctx: Cypher25Parser.ReallocateDatabasesContext
  ): Unit = {}

  // Role command contexts

  final override def exitDropRole(
    ctx: Cypher25Parser.DropRoleContext
  ): Unit = {
    ctx.ast = DropRole(ctx.commandNameExpression().ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitRenameRole(
    ctx: Cypher25Parser.RenameRoleContext
  ): Unit = {
    val names = ctx.commandNameExpression()
    ctx.ast = RenameRole(names.get(0).ast(), names.get(1).ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  // User command contexts

  final override def exitDropUser(
    ctx: Cypher25Parser.DropUserContext
  ): Unit = {
    ctx.ast = DropUser(ctx.commandNameExpression().ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitRenameUser(
    ctx: Cypher25Parser.RenameUserContext
  ): Unit = {
    val names = ctx.commandNameExpression()
    ctx.ast = RenameUser(names.get(0).ast(), names.get(1).ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitAlterCurrentUser(
    ctx: Cypher25Parser.AlterCurrentUserContext
  ): Unit = {
    ctx.ast = SetOwnPassword(
      ctx.passwordExpression(1).ast[Expression](),
      ctx.passwordExpression(0).ast[Expression]()
    )(pos(ctx.getParent))
  }

  final override def exitAlterUser(
    ctx: Cypher25Parser.AlterUserContext
  ): Unit = {
    val username = ctx.commandNameExpression().ast[Expression]()
    val nativePassAttributes = ctx.password().asScala.toList
      .map(_.ast[(Password, Option[PasswordChange])]())
      .foldLeft(List.empty[AuthAttribute]) { case (acc, (password, change)) => (acc :+ password) ++ change }
    val nativeAuthAttr = ctx.passwordChangeRequired().asScala.toList
      .map(c => PasswordChange(c.ast[Boolean]())(pos(c)))
      .foldLeft(nativePassAttributes) { case (acc, changeReq) => acc :+ changeReq }
      .sortBy(_.position)
    val suspended = astOptFromList[Boolean](ctx.userStatus(), None)
    val removeHome = if (!ctx.HOME().isEmpty) Some(RemoveHomeDatabaseAction) else None
    val homeDatabaseAction = astOptFromList[HomeDatabaseAction](ctx.homeDatabase(), removeHome)
    val userOptions = UserOptions(suspended, homeDatabaseAction)
    val nativeAuth =
      if (nativeAuthAttr.nonEmpty) Some(Auth(NATIVE_AUTH, nativeAuthAttr)(nativeAuthAttr.head.position)) else None
    val removeAuth = RemoveAuth(!ctx.ALL().isEmpty, ctx.removeNamedProvider().asScala.toList.map(_.ast[Expression]()))
    val setAuth = ctx.setAuthClause().asScala.toList.map(_.ast[Auth]())
    ctx.ast =
      AlterUser(username, userOptions, ctx.EXISTS() != null, setAuth, nativeAuth, removeAuth)(pos(ctx.getParent))
  }

  override def exitRemoveNamedProvider(ctx: Cypher25Parser.RemoveNamedProviderContext): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) ctx.stringLiteral().ast[StringLiteral]()
    else if (ctx.stringListLiteral() != null) ctx.stringListLiteral().ast[ListLiteral]()
    else ctx.parameter().ast[Parameter]()
  }

  override def exitSetAuthClause(ctx: Cypher25Parser.SetAuthClauseContext): Unit = {
    val provider = ctx.stringLiteral().ast[StringLiteral]()
    val attributes = astSeq[AuthAttribute](ctx.userAuthAttribute()).toList
    ctx.ast = Auth(provider.value, attributes)(pos(ctx))
  }

  override def exitUserAuthAttribute(ctx: Cypher25Parser.UserAuthAttributeContext): Unit = {
    ctx.ast = if (ctx.ID() != null) {
      AuthId(ctx.stringOrParameterExpression().ast())(pos(ctx.ID()))
    } else if (ctx.passwordOnly() != null) {
      ctx.passwordOnly().ast()
    } else {
      PasswordChange(ctx.passwordChangeRequired().ast[Boolean]())(pos(ctx.passwordChangeRequired()))
    }
  }

  override def exitPasswordOnly(ctx: Cypher25Parser.PasswordOnlyContext): Unit = {
    ctx.ast = Password(ctx.passwordExpression().ast[Expression](), ctx.ENCRYPTED() != null)(pos(ctx))
  }

  override def exitPassword(ctx: Cypher25Parser.PasswordContext): Unit = {
    val password = Password(ctx.passwordExpression().ast[Expression](), ctx.ENCRYPTED() != null)(pos(ctx))
    val passwordReq =
      if (ctx.passwordChangeRequired() != null)
        Some(PasswordChange(ctx.passwordChangeRequired().ast[Boolean]())(pos(ctx.passwordChangeRequired())))
      else None
    ctx.ast = (password, passwordReq)
  }

  final override def exitPasswordExpression(
    ctx: Cypher25Parser.PasswordExpressionContext
  ): Unit = {
    val str = ctx.stringLiteral()
    ctx.ast = if (str != null) {
      val pass = str.ast[StringLiteral]()
      SensitiveStringLiteral(pass.value.getBytes(StandardCharsets.UTF_8))(pass.position)
    } else {
      val pass = ctx.parameter().ast[Parameter]()
      new ExplicitParameter(pass.name, CTString)(pass.position) with SensitiveParameter
    }
  }

  final override def exitPasswordChangeRequired(
    ctx: Cypher25Parser.PasswordChangeRequiredContext
  ): Unit = {
    ctx.ast = ctx.NOT() == null
  }

  final override def exitUserStatus(
    ctx: Cypher25Parser.UserStatusContext
  ): Unit = {
    ctx.ast = ctx.SUSPENDED() != null
  }

  final override def exitHomeDatabase(
    ctx: Cypher25Parser.HomeDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    ctx.ast = SetHomeDatabaseAction(dbName)
  }

  // Database command contexts

  final override def exitDropDatabase(
    ctx: Cypher25Parser.DropDatabaseContext
  ): Unit = {
    val additionalAction = if (ctx.DUMP() != null) DumpData else DestroyData
    val aliasAction = astOpt[DropDatabaseAliasAction](ctx.aliasAction(), Restrict)
    ctx.ast = DropDatabase(
      ctx.symbolicAliasNameOrParameter().ast[DatabaseName](),
      ctx.EXISTS() != null,
      ctx.COMPOSITE() != null,
      aliasAction,
      additionalAction,
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx.getParent))
  }

  final override def exitAliasAction(
    ctx: Cypher25Parser.AliasActionContext
  ): Unit = {
    ctx.ast =
      if (ctx.CASCADE() != null) CascadeAliases
      else Restrict
  }

  final override def exitAlterDatabase(
    ctx: Cypher25Parser.AlterDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val waitUntilComplete = astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    ctx.ast = if (!ctx.REMOVE().isEmpty) {
      val optionsToRemove = Set.from(astSeq[String](ctx.symbolicNameString()))
      AlterDatabase(dbName, ctx.EXISTS() != null, None, None, NoOptions, optionsToRemove, waitUntilComplete)(
        pos(ctx.getParent)
      )
    } else {
      val access = astOptFromList(ctx.alterDatabaseAccess(), None)
      val topology = astOptFromList(ctx.alterDatabaseTopology(), None)
      val options =
        if (ctx.alterDatabaseOption().isEmpty) NoOptions
        else OptionsMap(astSeq[Map[String, Expression]](ctx.alterDatabaseOption()).reduce(_ ++ _))
      AlterDatabase(dbName, ctx.EXISTS() != null, access, topology, options, Set.empty, waitUntilComplete)(
        pos(ctx.getParent)
      )
    }
  }

  final override def exitAlterDatabaseAccess(ctx: Cypher25Parser.AlterDatabaseAccessContext): Unit = {
    ctx.ast = if (ctx.ONLY() != null) {
      ReadOnlyAccess
    } else {
      ReadWriteAccess
    }
  }

  final override def exitAlterDatabaseTopology(ctx: Cypher25Parser.AlterDatabaseTopologyContext): Unit = {
    ctx.ast =
      if (ctx.TOPOLOGY() != null) {
        val pT = astOptFromList[Either[Int, Parameter]](ctx.primaryTopology(), None)
        val sT = astOptFromList[Either[Int, Parameter]](ctx.secondaryTopology(), None)
        Topology(pT, sT)
      } else None
  }

  final override def exitPrimaryTopology(ctx: Cypher25Parser.PrimaryTopologyContext): Unit = {
    ctx.ast = ctx.uIntOrIntParameter().ast()
  }

  final override def exitSecondaryTopology(ctx: Cypher25Parser.SecondaryTopologyContext): Unit = {
    ctx.ast = ctx.uIntOrIntParameter().ast()
  }

  final override def exitAlterDatabaseOption(ctx: Cypher25Parser.AlterDatabaseOptionContext): Unit = {
    ctx.ast = Map((ctx.symbolicNameString().ast[String], ctx.expression().ast[Expression]))
  }

  final override def exitStartDatabase(
    ctx: Cypher25Parser.StartDatabaseContext
  ): Unit = {
    ctx.ast = StartDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

  final override def exitStopDatabase(
    ctx: Cypher25Parser.StopDatabaseContext
  ): Unit = {
    ctx.ast = StopDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

  final override def exitWaitClause(
    ctx: Cypher25Parser.WaitClauseContext
  ): Unit = {
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case Cypher25Parser.NOWAIT => NoWait
      case Cypher25Parser.WAIT => ctx.UNSIGNED_DECIMAL_INTEGER() match {
          case null    => IndefiniteWait
          case seconds => TimeoutAfter(seconds.getText.toLong)
        }
    }
  }

  // Alias command contexts

  final override def exitDropAlias(
    ctx: Cypher25Parser.DropAliasContext
  ): Unit = {
    ctx.ast =
      DropDatabaseAlias(ctx.aliasName().symbolicAliasNameOrParameter().ast[DatabaseName](), ctx.EXISTS() != null)(pos(
        ctx.getParent
      ))
  }

  final override def exitAlterAlias(
    ctx: Cypher25Parser.AlterAliasContext
  ): Unit = {
    val aliasName = ctx.aliasName().symbolicAliasNameOrParameter().ast[DatabaseName]()
    val aliasTargetCtx = ctx.alterAliasTarget()
    val (targetName, url) = {
      if (aliasTargetCtx.isEmpty) (None, None)
      else
        (
          Some(aliasTargetCtx.get(0).databaseName().symbolicAliasNameOrParameter().ast[DatabaseName]()),
          astOpt[Either[String, Parameter]](aliasTargetCtx.get(0).stringOrParameter())
        )
    }
    val username = astOptFromList[Expression](ctx.alterAliasUser(), None)
    val password = astOptFromList[Expression](ctx.alterAliasPassword(), None)
    val driverSettings = astOptFromList[Either[Map[String, Expression], Parameter]](ctx.alterAliasDriver(), None)
    val properties = astOptFromList[Either[Map[String, Expression], Parameter]](ctx.alterAliasProperties(), None)
    ctx.ast = if (url.isEmpty && username.isEmpty && password.isEmpty && driverSettings.isEmpty) {
      AlterLocalDatabaseAlias(aliasName, targetName, ctx.EXISTS() != null, properties)(pos(ctx.getParent))
    } else {
      AlterRemoteDatabaseAlias(
        aliasName,
        targetName,
        ctx.EXISTS() != null,
        url,
        username,
        password,
        driverSettings,
        properties
      )(pos(ctx.getParent))
    }
  }

  override def exitAlterAliasTarget(ctx: Cypher25Parser.AlterAliasTargetContext): Unit = {
    val target = ctx.databaseName().symbolicAliasNameOrParameter().ast[DatabaseName]()
    val url = astOpt[Either[String, Parameter]](ctx.stringOrParameter())
    ctx.ast = (target, url)
  }

  override def exitAlterAliasUser(ctx: Cypher25Parser.AlterAliasUserContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasPassword(ctx: Cypher25Parser.AlterAliasPasswordContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasDriver(ctx: Cypher25Parser.AlterAliasDriverContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasProperties(ctx: Cypher25Parser.AlterAliasPropertiesContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  // General symbolic names/string contexts

  final override def exitSymbolicNameOrStringParameter(
    ctx: Cypher25Parser.SymbolicNameOrStringParameterContext
  ): Unit = {
    ctx.ast = if (ctx.symbolicNameString() != null) {
      Left(ctx.symbolicNameString().ast[String]())
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

  final override def exitCommandNameExpression(
    ctx: Cypher25Parser.CommandNameExpressionContext
  ): Unit = {
    val name = ctx.symbolicNameString()
    ctx.ast = if (name != null) {
      StringLiteral(ctx.symbolicNameString().ast[String]())(rangePos(name))
    } else {
      ctx.parameter().ast[Parameter]()
    }
  }

  final override def exitSymbolicNameOrStringParameterList(
    ctx: Cypher25Parser.SymbolicNameOrStringParameterListContext
  ): Unit = {
    ctx.ast = astSeq[Expression](ctx.commandNameExpression())
  }

  final override def exitSymbolicAliasNameList(
    ctx: Cypher25Parser.SymbolicAliasNameListContext
  ): Unit = {
    ctx.ast = astSeq[DatabaseName](ctx.symbolicAliasNameOrParameter())
  }

  final override def exitSymbolicAliasNameOrParameter(
    ctx: Cypher25Parser.SymbolicAliasNameOrParameterContext
  ): Unit = {
    val symbAliasName = ctx.symbolicAliasName()
    ctx.ast =
      if (symbAliasName != null) {
        val s = symbAliasName.ast[ArraySeq[String]]().toList
        NamespacedName(s)(pos(ctx))
      } else
        ParameterName(ctx.parameter().ast())(pos(ctx))
  }

  final override def exitAliasName(ctx: Cypher25Parser.AliasNameContext): Unit = {
    ctx.ast = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]
  }

  final override def exitDatabaseName(ctx: Cypher25Parser.DatabaseNameContext): Unit = {
    ctx.ast = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]
  }

  final override def exitStringOrParameterExpression(
    ctx: Cypher25Parser.StringOrParameterExpressionContext
  ): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) {
      ctx.stringLiteral().ast[StringLiteral]()
    } else {
      ctx.parameter().ast[Parameter]()
    }
  }

  final override def exitStringOrParameter(
    ctx: Cypher25Parser.StringOrParameterContext
  ): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) {
      Left(ctx.stringLiteral().ast[StringLiteral]().value)
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

  override def exitUIntOrIntParameter(ctx: Cypher25Parser.UIntOrIntParameterContext): Unit = {
    ctx.ast = if (ctx.UNSIGNED_DECIMAL_INTEGER() != null) {
      Left(ctx.UNSIGNED_DECIMAL_INTEGER().getText.toInt)
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

}
