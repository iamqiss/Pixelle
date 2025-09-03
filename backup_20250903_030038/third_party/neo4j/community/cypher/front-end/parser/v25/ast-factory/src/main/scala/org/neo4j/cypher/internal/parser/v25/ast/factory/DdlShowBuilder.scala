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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllExistsConstraints
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NodeAllExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropExistsConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PropExistsConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelAllExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropExistsConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.inputText
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser
import org.neo4j.cypher.internal.parser.v25.Cypher25ParserListener
import org.neo4j.cypher.internal.parser.v25.ast.factory.DdlShowBuilder.ShowWrapper
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.immutable.ArraySeq

trait DdlShowBuilder extends Cypher25ParserListener {

  final override def exitShowCommand(
    ctx: Cypher25Parser.ShowCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast match {
      case ast: Seq[Clause @unchecked] => SingleQuery(ast)(pos(ctx))
      case ast                         => ast
    }
  }

  final override def exitTerminateCommand(
    ctx: Cypher25Parser.TerminateCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  // YIELD context and helpers

  private def decomposeYield(
    yieldOrWhere: YieldOrWhere
  ): ShowWrapper = {
    if (yieldOrWhere.isDefined) {
      yieldOrWhere.get match {
        case Left((y, optR)) =>
          val (yieldAll, yieldedItems, optY) = getYieldAllAndYieldItems(y)
          ShowWrapper(yieldedItems = yieldedItems, yieldAll = yieldAll, yieldClause = optY, returnClause = optR)
        case Right(where) =>
          ShowWrapper(where = Some(where))
      }
    } else ShowWrapper()
  }

  private def getYieldAllAndYieldItems(yieldClause: Yield): (Boolean, List[CommandResultItem], Option[Yield]) = {
    val yieldAll = Option(yieldClause).exists(_.returnItems.includeExisting)
    val yieldedItems = Option(yieldClause)
      .map(_.returnItems.items.map(item => {
        // yield is always parsed as `variable` with potentially `AS variable` after
        val variable = item.expression.asInstanceOf[LogicalVariable]
        val aliasedVariable: LogicalVariable = item.alias.getOrElse(variable)
        CommandResultItem(variable.name, aliasedVariable)(item.position)
      }).toList)
      .getOrElse(List.empty)
    (yieldAll, yieldedItems, Some(yieldClause))
  }

  final override def exitYieldItem(
    ctx: Cypher25Parser.YieldItemContext
  ): Unit = {
    val variables = ctx.variable()
    val returnItem = variables.get(0).ast[Variable]()
    ctx.ast = if (variables.size == 1) {
      UnaliasedReturnItem(returnItem, inputText(variables.get(0)))(pos(ctx))
    } else {
      AliasedReturnItem(returnItem, variables.get(1).ast[Variable]())(pos(ctx))
    }
  }

  final override def exitYieldSkip(ctx: Cypher25Parser.YieldSkipContext): Unit = {
    ctx.ast = Skip(ctx.signedIntegerLiteral().ast[Expression]())(pos(ctx))
  }

  final override def exitYieldLimit(ctx: Cypher25Parser.YieldLimitContext): Unit = {
    ctx.ast = Limit(ctx.signedIntegerLiteral().ast[Expression]())(pos(ctx))
  }

  final override def exitOrderBy(ctx: Cypher25Parser.OrderByContext): Unit = {
    ctx.ast = OrderBy(astSeq[SortItem](ctx.orderItem()))(pos(ctx.ORDER().getSymbol))
  }

  final override def exitYieldClause(
    ctx: Cypher25Parser.YieldClauseContext
  ): Unit = {
    val returnItems =
      if (ctx.TIMES() != null)
        ReturnItems(includeExisting = true, Seq.empty)(pos(ctx.YIELD().getSymbol))
      else {
        ReturnItems(includeExisting = false, astSeq[ReturnItem](ctx.yieldItem()))(pos(ctx.yieldItem().get(0)))
      }
    ctx.ast = Yield(
      returnItems,
      astOpt[OrderBy](ctx.orderBy()),
      astOpt[Skip](ctx.yieldSkip()),
      astOpt[Limit](ctx.yieldLimit()),
      astOpt[Where](ctx.whereClause())
    )(pos(ctx))
  }

  final override def exitShowCommandYield(
    ctx: Cypher25Parser.ShowCommandYieldContext
  ): Unit = {
    val yieldClause = ctx.yieldClause()
    val whereClause = ctx.whereClause()
    ctx.ast = if (yieldClause != null) {
      Left[(Yield, Option[Return]), Where]((
        yieldClause.ast[Yield](),
        astOpt[Return](ctx.returnClause())
      ))
    } else
      Right[(Yield, Option[Return]), Where](whereClause.ast[Where]())
  }

  // Non-admin show and terminate command contexts (ordered as in parser file)

  final override def exitComposableCommandClauses(
    ctx: Cypher25Parser.ComposableCommandClausesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  override def exitComposableShowCommandClauses(
    ctx: Cypher25Parser.ComposableShowCommandClausesContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitShowIndexCommand(
    ctx: Cypher25Parser.ShowIndexCommandContext
  ): Unit = {
    val parentPos = pos(ctx.getParent)
    ctx.ast = {
      val indexType = astOpt[ShowIndexType](ctx.showIndexType()).getOrElse(AllIndexes)
      ctx.showIndexesEnd().ast[ShowWrapper].buildIndexClauses(indexType, parentPos)
    }
  }

  override def exitShowIndexType(
    ctx: Cypher25Parser.ShowIndexTypeContext
  ): Unit = {
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case Cypher25Parser.ALL      => AllIndexes
      case Cypher25Parser.FULLTEXT => FulltextIndexes
      case Cypher25Parser.LOOKUP   => LookupIndexes
      case Cypher25Parser.POINT    => PointIndexes
      case Cypher25Parser.RANGE    => RangeIndexes
      case Cypher25Parser.TEXT     => TextIndexes
      case Cypher25Parser.VECTOR   => VectorIndexes
      case _                       => throw new IllegalStateException("Unexpected index type")
    }
  }

  final override def exitShowIndexesEnd(
    ctx: Cypher25Parser.ShowIndexesEndContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()))
  }

  override def exitShowConstraintCommand(ctx: Cypher25Parser.ShowConstraintCommandContext): Unit = {
    val parentPos = pos(ctx.getParent)
    ctx.ast = ctx match {
      case c: Cypher25Parser.ShowConstraintAllContext =>
        val constraintType = AllConstraints
        c.showConstraintsEnd().ast[ShowWrapper]().buildConstraintClauses(constraintType, parentPos)
      case c: Cypher25Parser.ShowConstraintExistContext =>
        val constraintType = if (c.constraintExistType().PROPERTY() != null) {
          pickShowConstraintType(
            c.showConstraintEntity(),
            NodePropExistsConstraints.cypher25,
            RelPropExistsConstraints.cypher25,
            PropExistsConstraints.cypher25
          )
        } else {
          pickShowConstraintType(
            c.showConstraintEntity(),
            NodeAllExistsConstraints,
            RelAllExistsConstraints,
            AllExistsConstraints
          )
        }
        c.showConstraintsEnd().ast[ShowWrapper]().buildConstraintClauses(constraintType, parentPos)
      case c: Cypher25Parser.ShowConstraintKeyContext =>
        val constraintType = pickShowConstraintType(
          c.showConstraintEntity(),
          NodeKeyConstraints,
          RelKeyConstraints,
          KeyConstraints
        )
        c.showConstraintsEnd().ast[ShowWrapper]().buildConstraintClauses(constraintType, parentPos)
      case c: Cypher25Parser.ShowConstraintPropTypeContext =>
        val constraintType = pickShowConstraintType(
          c.showConstraintEntity(),
          NodePropTypeConstraints,
          RelPropTypeConstraints,
          PropTypeConstraints
        )
        c.showConstraintsEnd().ast[ShowWrapper]().buildConstraintClauses(constraintType, parentPos)
      case c: Cypher25Parser.ShowConstraintUniqueContext =>
        val constraintType = pickShowConstraintType(
          c.showConstraintEntity(),
          NodeUniqueConstraints.cypher25,
          RelUniqueConstraints.cypher25,
          UniqueConstraints.cypher25
        )
        c.showConstraintsEnd().ast[ShowWrapper]().buildConstraintClauses(constraintType, parentPos)
      case _ => throw new IllegalStateException("Invalid Constraint Type")
    }
  }

  private def pickShowConstraintType(
    enitityTypeContext: Cypher25Parser.ShowConstraintEntityContext,
    nodeType: ShowConstraintType,
    relType: ShowConstraintType,
    allType: ShowConstraintType
  ): ShowConstraintType = {
    astOpt[ConstraintEntity](enitityTypeContext).getOrElse(NoEntity) match {
      case Node => nodeType
      case Rel  => relType
      case _    => allType
    }
  }

  override def exitConstraintExistType(
    ctx: Cypher25Parser.ConstraintExistTypeContext
  ): Unit = {}

  override def exitShowConstraintEntity(
    ctx: Cypher25Parser.ShowConstraintEntityContext
  ): Unit = {
    ctx.ast = ctx match {
      case _: Cypher25Parser.NodeEntityContext => Node
      case _: Cypher25Parser.RelEntityContext  => Rel
      case _                                   => NoEntity
    }
  }

  sealed private trait ConstraintEntity
  private case object Node extends ConstraintEntity
  private case object Rel extends ConstraintEntity
  private case object NoEntity extends ConstraintEntity

  final override def exitShowConstraintsEnd(
    ctx: Cypher25Parser.ShowConstraintsEndContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()))
  }

  final override def exitShowProcedures(
    ctx: Cypher25Parser.ShowProceduresContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()))
      .buildProcedureClauses(astOpt[ExecutableBy](ctx.executableBy), pos(ctx.getParent))
  }

  final override def exitShowFunctions(
    ctx: Cypher25Parser.ShowFunctionsContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()))
      .buildFunctionClauses(
        astOpt[ShowFunctionType](ctx.showFunctionsType, AllFunctions),
        astOpt[ExecutableBy](ctx.executableBy),
        pos(ctx.getParent)
      )
  }

  final override def exitShowFunctionsType(
    ctx: Cypher25Parser.ShowFunctionsTypeContext
  ): Unit = {
    ctx.ast = if (ctx.BUILT() != null) {
      BuiltInFunctions
    } else if (ctx.USER() != null) {
      UserDefinedFunctions
    } else AllFunctions
  }

  final override def exitExecutableBy(ctx: Cypher25Parser.ExecutableByContext): Unit = {
    val name = ctx.symbolicNameString()
    ctx.ast =
      if (name != null) {
        User(ctx.symbolicNameString().ast())
      } else CurrentUser
  }

  final override def exitShowTransactions(
    ctx: Cypher25Parser.ShowTransactionsContext
  ): Unit = {
    ctx.ast = ctx.namesAndClauses().ast[ShowWrapper]().buildShowTransactions(pos(ctx.getParent))
  }

  final override def exitTerminateTransactions(
    ctx: Cypher25Parser.TerminateTransactionsContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(
        composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()),
        names = ctx.stringsOrExpression().ast[Either[List[String], Expression]]
      )
      .buildTerminateTransaction(pos(ctx.getParent))
  }

  final override def exitShowSettings(
    ctx: Cypher25Parser.ShowSettingsContext
  ): Unit = {
    ctx.ast = ctx.namesAndClauses().ast[ShowWrapper]().buildSettingsClauses(pos(ctx.getParent))
  }

  override def exitNamesAndClauses(
    ctx: Cypher25Parser.NamesAndClausesContext
  ): Unit = {
    ctx.ast = decomposeYield(astOpt(ctx.showCommandYield()))
      .copy(
        composableClauses = astOpt[Seq[Clause]](ctx.composableCommandClauses()),
        names = astOpt[Either[List[String], Expression]](ctx.stringsOrExpression(), Left(List.empty))
      )
  }

  final override def exitStringsOrExpression(
    ctx: Cypher25Parser.StringsOrExpressionContext
  ): Unit = {
    val stringList = ctx.stringList()
    ctx.ast = if (stringList != null) {
      Left[List[String], Expression](
        stringList.ast[Seq[StringLiteral]]().map(_.value).toList
      )
    } else {
      Right[List[String], Expression](ctx.expression.ast())
    }
  }

  final override def exitStringList(
    ctx: Cypher25Parser.StringListContext
  ): Unit = {
    ctx.ast = astSeq[StringLiteral](ctx.stringLiteral())
  }

  // Admin show command contexts (ordered as in parser file)

  final override def exitShowServers(
    ctx: Cypher25Parser.ShowServersContext
  ): Unit = {
    ctx.ast = ShowServers(
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx))
  }

  final override def exitShowRoles(
    ctx: Cypher25Parser.ShowRolesContext
  ): Unit = {
    ctx.ast = ShowRoles(
      ctx.WITH() != null,
      ctx.POPULATED() == null,
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx))
  }

  final override def exitShowUsers(
    ctx: Cypher25Parser.ShowUsersContext
  ): Unit = {
    ctx.ast = ShowUsers(
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield()),
      withAuth = ctx.AUTH() != null
    )(pos(ctx))
  }

  final override def exitShowCurrentUser(
    ctx: Cypher25Parser.ShowCurrentUserContext
  ): Unit = {
    ctx.ast = ShowCurrentUser(
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx))
  }

  final override def exitShowSupportedPrivileges(
    ctx: Cypher25Parser.ShowSupportedPrivilegesContext
  ): Unit = {
    ctx.ast = ShowSupportedPrivilegeCommand(
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx))
  }

  final override def exitShowPrivileges(
    ctx: Cypher25Parser.ShowPrivilegesContext
  ): Unit = {
    val (asCommand, asRevoke) = astOpt[(Boolean, Boolean)](ctx.privilegeAsCommand(), (false, false))
    val cmdYield = astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    ctx.ast = if (asCommand)
      ShowPrivilegeCommands(ShowAllPrivileges()(pos(ctx)), asRevoke, cmdYield)(pos(ctx))
    else {
      ShowPrivileges(ShowAllPrivileges()(pos(ctx)), cmdYield)(pos(ctx))
    }
  }

  final override def exitShowRolePrivileges(
    ctx: Cypher25Parser.ShowRolePrivilegesContext
  ): Unit = {
    val (asCommand, asRevoke) = astOpt[(Boolean, Boolean)](ctx.privilegeAsCommand(), (false, false))
    val cmdYield = astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    val scope = ShowRolesPrivileges(
      ctx.roleNames.symbolicNameOrStringParameterList().ast[Seq[Expression]]().toList
    )(pos(ctx))
    ctx.ast = if (asCommand) {
      ShowPrivilegeCommands(scope, asRevoke, cmdYield)(pos(ctx))
    } else {
      ShowPrivileges(scope, cmdYield)(pos(ctx))
    }
  }

  final override def exitShowUserPrivileges(
    ctx: Cypher25Parser.ShowUserPrivilegesContext
  ): Unit = {
    val (asCommand, asRevoke) = astOpt[(Boolean, Boolean)](ctx.privilegeAsCommand(), (false, false))
    val namesList = ctx.userNames
    val cmdYield = astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    val scope = if (namesList != null)
      ShowUsersPrivileges(namesList.ast[ArraySeq[Expression]]().toList)(pos(ctx))
    else ShowUserPrivileges(None)(pos(ctx))
    ctx.ast = if (asCommand) {
      ShowPrivilegeCommands(scope, asRevoke, cmdYield)(pos(ctx))
    } else {
      ShowPrivileges(scope, cmdYield)(pos(ctx))
    }
  }

  override def exitPrivilegeAsCommand(ctx: Cypher25Parser.PrivilegeAsCommandContext): Unit = {
    ctx.ast = (ctx.AS() != null, ctx.REVOKE() != null)
  }

  final override def exitShowDatabase(
    ctx: Cypher25Parser.ShowDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter()
    val dbScope =
      if (dbName != null) SingleNamedDatabaseScope(dbName.ast[DatabaseName]())(pos(ctx))
      else if (ctx.HOME() != null) HomeDatabaseScope()(pos(ctx))
      else if (ctx.DEFAULT() != null) DefaultDatabaseScope()(pos(ctx))
      else AllDatabasesScope()(pos(ctx))
    ctx.ast = ShowDatabase(
      dbScope,
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx.getParent))
  }

  final override def exitShowAliases(
    ctx: Cypher25Parser.ShowAliasesContext
  ): Unit = {
    ctx.ast = ShowAliases(
      astOpt[DatabaseName](ctx.aliasName()),
      astOpt[Either[(Yield, Option[Return]), Where]](ctx.showCommandYield())
    )(pos(ctx))
  }

}

object DdlShowBuilder {

  case class ShowWrapper(
    where: Option[Where] = None,
    yieldedItems: List[CommandResultItem] = List.empty,
    yieldAll: Boolean = false,
    yieldClause: Option[Yield] = None,
    returnClause: Option[Return] = None,
    composableClauses: Option[Seq[Clause]] = None,
    names: Either[List[String], Expression] = Left(List.empty)
  ) {

    def buildConstraintClauses(constraintType: ShowConstraintType, position: InputPosition): Seq[Clause] = {
      buildClauses(
        ShowConstraintsClause(
          constraintType,
          where,
          yieldedItems,
          yieldAll
        )(position)
      )
    }

    def buildIndexClauses(indexType: ShowIndexType, position: InputPosition): Seq[Clause] = {
      buildClauses(
        ShowIndexesClause(
          indexType,
          where,
          yieldedItems,
          yieldAll = yieldAll
        )(position)
      )
    }

    def buildFunctionClauses(
      functionType: ShowFunctionType,
      executableBy: Option[ExecutableBy],
      position: InputPosition
    ): Seq[Clause] = {
      buildClauses(
        ShowFunctionsClause(
          functionType,
          executableBy,
          where,
          yieldedItems,
          yieldAll
        )(position)
      )
    }

    def buildProcedureClauses(executableBy: Option[ExecutableBy], position: InputPosition): Seq[Clause] = {
      buildClauses(
        ShowProceduresClause(executableBy, where, yieldedItems, yieldAll)(position)
      )
    }

    def buildSettingsClauses(position: InputPosition): Seq[Clause] = {
      buildClauses(
        ShowSettingsClause(names, where, yieldedItems, yieldAll)(position)
      )
    }

    def buildShowTransactions(position: InputPosition): Seq[Clause] = {
      buildClauses(
        ShowTransactionsClause(names, where, yieldedItems, yieldAll, returnCypher5Types = false)(position)
      )
    }

    def buildTerminateTransaction(position: InputPosition): Seq[Clause] = {
      buildClauses(
        TerminateTransactionsClause(names, yieldedItems, yieldAll, where.map(_.position))(position)
      )
    }

    private def buildClauses(cmdClause: Clause): Seq[Clause] = {
      ArraySeq.from(
        Seq(cmdClause) ++ yieldClause.map(turnYieldToWith) ++ returnClause ++ composableClauses.getOrElse(Seq.empty)
      )
    }

    private def turnYieldToWith(yieldClause: Yield): Clause = {
      val returnItems = yieldClause.returnItems
      val itemOrder = Option.when(returnItems.items.nonEmpty)(returnItems.items.map(_.name).toList)
      val (orderBy, where) = CommandClause.updateAliasedVariablesFromYieldInOrderByAndWhere(yieldClause)
      With(
        distinct = false,
        ReturnItems(includeExisting = true, Seq(), itemOrder)(returnItems.position),
        orderBy,
        yieldClause.skip,
        yieldClause.limit,
        where,
        withType = ParsedAsYield
      )(yieldClause.position)
    }
  }
}
