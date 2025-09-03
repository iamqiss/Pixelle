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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.InputDataStream
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.eval.UseEvaluation
import org.neo4j.fabric.pipeline.FabricFrontEnd
import org.neo4j.fabric.planning.Ast.aliasedReturn
import org.neo4j.fabric.planning.Fragment.Apply
import org.neo4j.fabric.planning.Fragment.Exec
import org.neo4j.fabric.planning.Fragment.Init
import org.neo4j.fabric.util.Rewritten.RewritingOps

/**
 * @param queryString     For error reporting
 * @param compositeContext When false, throws on multi-graph queries
 */
case class FabricStitcher(
  queryString: String,
  compositeContext: Boolean,
  cypherVersion: CypherVersion,
  pipeline: FabricFrontEnd#Pipeline,
  useHelper: UseHelper
) {

  /**
   * Convert to executable fragments
   * Exec fragments formed by recursively stitching same-graph Leaf:s together
   * and adding appropriate glue clauses
   */
  def convert(fragment: Fragment): Fragment = fragment match {
    case chain: Fragment.Chain =>
      val convertedChain = convertChain(chain)
      if (compositeContext) processCompositeCallInTx(convertedChain) else convertedChain
    case union: Fragment.Union => convertUnion(union)
    case command: Fragment.Command =>
      if (!compositeContext && !UseEvaluation.isStatic(command.use.graphSelection)) {
        failDynamicGraph(command.use)
      }
      asExec(
        Fragment.Init(command.use),
        command.command,
        command.outputColumns
      )
  }

  private def processCompositeCallInTx(fragment: Fragment): Fragment.Chain = fragment match {
    // Go over the fragment chain and process CALL IN TX Apply if present
    case apply: Apply if apply.inTransactionsParameters.isDefined => {
      val newExec = apply.inner match {
        case exec: Exec => constructCallInTransactionExec(exec, apply.inTransactionsParameters.get)
        // At the end of stitching an Apply can have only Exec as the inner fragment
        case f => throw new IllegalArgumentException("Unexpected fragment: " + f);
      }
      apply.copy(input = processCompositeCallInTx(apply.input), inner = newExec)(apply.pos)
    }
    case apply: Apply => apply.copy(input = processCompositeCallInTx(apply.input))(apply.pos)
    case init: Init   => init
    case exec: Exec   => exec.copy(input = processCompositeCallInTx(exec.input))
    case f            => throw new IllegalArgumentException("Unexpected fragment: " + f);
  }

  private def constructCallInTransactionExec(
    originalExec: Exec,
    inTransactionsParameters: SubqueryCall.InTransactionsParameters
  ): Fragment = {
    val pos = originalExec.pos
    val clauses = originalExec.query match {
      case singleQuery: SingleQuery => singleQuery.clauses
      case q                        => throw new IllegalArgumentException("Unexpected query type: " + q)
    }
    val clausesWithoutInsertedWith = if (originalExec.importColumns.isEmpty) clauses else clauses.tail
    val unwind =
      ast.Unwind(
        ExplicitParameter(Apply.CALL_IN_TX_ROWS, CTAny)(pos),
        Variable(Apply.CALL_IN_TX_ROW)(pos, Variable.isIsolatedDefault)
      )(pos)
    val postUnwindWith = With(ReturnItems(
      includeExisting = false,
      items =
        for {
          varName <- originalExec.importColumns :+ Apply.CALL_IN_TX_ROW_ID
        } yield AliasedReturnItem(
          expression = Property(
            Variable(Apply.CALL_IN_TX_ROW)(pos, Variable.isIsolatedDefault),
            (org.neo4j.cypher.internal.expressions PropertyKeyName varName)(pos)
          )(pos),
          variable = Variable(varName)(pos, Variable.isIsolatedDefault)
        )(pos)
    )(pos))(pos)

    val adjustedParameters = adjustInTransactionsParameters(inTransactionsParameters)
    val call =
      ScopeClauseSubqueryCall(
        SingleQuery(clausesWithoutInsertedWith)(pos),
        isImportingAll = false,
        originalExec.importColumns.map(x => Variable(x)(pos, Variable.isIsolatedDefault)),
        Some(adjustedParameters),
        optional = false
      )(pos)
    val outputColumns = callInTxOutputColumns(originalExec, adjustedParameters)
    val returnClause = aliasedReturn(outputColumns, pos)

    val resultClauses = Seq(unwind, postUnwindWith, call, returnClause)
    asExec(originalExec.input, SingleQuery(resultClauses)(pos), outputColumns)
  }

  private def callInTxOutputColumns(
    originalExec: Exec,
    inTransactionsParameters: SubqueryCall.InTransactionsParameters
  ): Seq[String] = {
    // These are the columns that were actually specified in the query ...
    val userColumns = inTransactionsParameters.reportParams
      .map(reportParams => reportParams.reportAs.name)
      .map(reportVariable => originalExec.outputColumns :+ reportVariable)
      .getOrElse(originalExec.outputColumns)
    // ... and we add one extra column that will enable the runtime to pair
    // output rows to the input ones.
    userColumns :+ Apply.CALL_IN_TX_ROW_ID
  }

  // When using ON ERROR break, we need the status regardless if the user
  // wanted the status report or not. The reason is that success or failure
  // of the batch cannot be determined without the status report in the case of ON ERROR break.
  private def adjustInTransactionsParameters(inTransactionsParameters: SubqueryCall.InTransactionsParameters)
    : SubqueryCall.InTransactionsParameters = {
    if (
      inTransactionsParameters.errorParams.isEmpty
      || !inTransactionsParameters.errorParams.get.behaviour.equals(OnErrorBreak)
    ) {
      return inTransactionsParameters
    }

    if (inTransactionsParameters.reportParams.isDefined) {
      return inTransactionsParameters
    }

    inTransactionsParameters.copy(reportParams =
      Some(SubqueryCall.InTransactionsReportParameters(
        Variable(Apply.REPORT_VARIABLE)(inTransactionsParameters.position, Variable.isIsolatedDefault)
      )(inTransactionsParameters.position))
    )(inTransactionsParameters.position)
  }

  def convertUnion(union: Fragment.Union): Fragment =
    stitched(union)
      .getOrElse(union.copy(
        lhs = convert(union.lhs),
        rhs = convertChain(union.rhs)
      )(union.pos))

  def convertChain(chain: Fragment.Chain): Fragment.Chain =
    stitched(chain)
      .getOrElse(convertSeparate(chain))

  def convertSeparate(chain: Fragment.Chain, lastInChain: Boolean = true): Fragment.Chain = chain match {
    case init: Fragment.Init     => init
    case stitched: Fragment.Exec => stitched

    case leaf: Fragment.Leaf =>
      val input = convertSeparate(leaf.input, lastInChain = false)
      if (leaf.executable)
        single(leaf.copy(input = input)(leaf.pos), lastInChain)
      else
        input

    case apply: Fragment.Apply =>
      apply.copy(input = convertSeparate(apply.input, lastInChain = false), inner = convert(apply.inner))(apply.pos)
  }

  def validateNoTransactionalSubquery(fragment: Fragment): Unit = {
    fragment.flatten.foreach {
      case apply: Fragment.Apply if apply.inTransactionsParameters.isDefined =>
        failFabricTransactionalSubquery(apply.pos)
      case exec: Fragment.Exec => SubqueryCall.findTransactionalSubquery(exec.query).foreach(subquery =>
          failFabricTransactionalSubquery(subquery.position)
        )
      case leaf: Fragment.Leaf => leaf.clauses.foreach(c =>
          SubqueryCall.findTransactionalSubquery(c).foreach(subquery =>
            failFabricTransactionalSubquery(subquery.position)
          )
        )
      case _ => ()
    }
  }

  /**
   * Transform a single leaf into exec
   */
  def single(leaf: Fragment.Leaf, lastInChain: Boolean): Fragment.Exec = {
    val pos = leaf.clauses.head.position

    val clauses = Seq(
      Ast.inputDataStream(leaf.input.outputColumns, pos).toSeq,
      Ast.paramBindings(leaf.importColumns, pos).toSeq,
      Ast.withoutGraphSelection(leaf.clauses),
      if (lastInChain)
        Seq.empty
      else
        Ast.aliasedReturn(leaf.clauses.last, leaf.outputColumns, leaf.clauses.last.position).toSeq
    ).flatten

    asExec(
      input = leaf.input,
      statement = SingleQuery(clauses)(pos),
      outputColumns = leaf.outputColumns
    )
  }

  /**
   * Transform the entire fragment tree into exec, by stitching it back together.
   * Returns a value when the entire query targets the same graph, statically
   */
  def stitched(fragment: Fragment): Option[Fragment.Exec] = {
    val noPos = InputPosition.NONE
    val stitched = stitcher(
      fragment,
      clauseExpansion = {
        case Outer(init: Fragment.Init) => Ast.paramBindings(init.importColumns, noPos).toSeq
        case Outer(leaf: Fragment.Leaf) => Ast.withoutGraphSelection(leaf.clauses)
        case Inner(leaf: Fragment.Leaf) => Ast.withoutGraphSelection(leaf.clauses)
        case _                          => Seq()
      }
    )

    val nonStatic = stitched.useAppearances.flatMap(_.nonStatic).headOption
    val nonEqual = stitched.useAppearances.flatMap(_.nonEqual).headOption
    val invalidOverride = stitched.useAppearances.flatMap(_.isInvalidOverride).headOption

    (compositeContext, nonStatic, nonEqual, invalidOverride) match {
      case (false, Some(use), _, _)                 => failDynamicGraph(use)
      case (false, _, Some(use), _)                 => failMultipleGraphs(use)
      case (true, _, _, Some((useOuter, useInner))) => failInvalidOverride(useOuter, useInner)

      case (_, _, None, None) =>
        val init = Fragment.Init(stitched.lastUse, fragment.argumentColumns, fragment.importColumns)
        Some(asExec(init, stitched.query, fragment.outputColumns))

      case (_, _, _, _) => None
    }
  }

  private def asExec(
    input: Fragment.Chain,
    statement: Statement,
    outputColumns: Seq[String]
  ): Fragment.Exec = {

    val sensitive = statement.folder.treeExists {
      // these are used for auto-parameterization when query-obfuscation
      // is enabled, we should still cache these.
      case _: SensitiveAutoParameter => false
      // these two are used for password fields
      case _: SensitiveParameter     => true
      case _: SensitiveStringLiteral => true
    }

    val local = pipeline.checkAndFinalize.process(statement, useFullQueryText = !compositeContext)

    val (rewriter, extracted) = sensitiveLiteralReplacement(statement)
    val toRender = statement.endoRewrite(rewriter)
    val remote = Fragment.RemoteQuery(QueryRenderer.render(toRender), extracted)

    Fragment.Exec(input, statement, local, remote, sensitive, outputColumns)
  }

  private def failDynamicGraph(use: Use): Nothing =
    throw new SyntaxException(
      MessageUtilProvider.createDynamicGraphReferenceUnsupportedError(Use.show(use)).stripMargin,
      queryString,
      use.position.offset
    )

  private def failMultipleGraphs(use: Use): Nothing =
    throw SyntaxException.accessingMultipleGraphsOnlySupportedOnCompositeDatabases(
      MessageUtilProvider.createMultipleGraphReferencesError(Use.show(use)),
      queryString,
      use.position.offset
    )

  private def failInvalidOverride(useOuter: Use, useInner: Use): Nothing =
    throw SyntaxException.invalidNestedUseClause(
      Use.show(useOuter),
      Use.show(useInner),
      s"""Nested subqueries must use the same graph as their parent query.
         |Attempted to access graph ${Use.show(useInner)}""".stripMargin,
      queryString,
      useInner.position.offset
    )

  private def failFabricTransactionalSubquery(pos: InputPosition): Nothing =
    throw new SyntaxException(
      "Transactional subquery is not allowed here. This feature is not supported on composite databases.",
      queryString,
      pos.offset
    )

  private case class StitchResult(
    query: Query,
    lastUse: Use,
    useAppearances: Seq[UseAppearance]
  )

  private case class StitchChainResult(
    clauses: Seq[Clause],
    lastUse: Use,
    useAppearances: Seq[UseAppearance]
  )

  sealed private trait NestedFragment
  final private case class Outer(fragment: Fragment) extends NestedFragment
  final private case class Inner(fragment: Fragment) extends NestedFragment

  sealed private trait UseAppearance {
    def nonStatic: Option[Use] = uses.find(use => !UseEvaluation.isStatic(use.graphSelection))
    def nonEqual: Option[Use] = uses.find(use => use.graphSelection != uses.head.graphSelection)
    def isInvalidOverride: Option[(Use, Use)] = None
    def uses: Seq[Use]
  }

  final private case class UnionUse(lhs: Use, rhs: Use) extends UseAppearance {
    def uses: Seq[Use] = Seq(lhs, rhs)
  }

  final private case class ChainUse(outer: Option[Use], inner: Use) extends UseAppearance {
    def uses: Seq[Use] = outer.toSeq :+ inner

    override def isInvalidOverride: Option[(Use, Use)] = outer match {
      case None => None
      case Some(outer) =>
        def outerIsComposite = useHelper.useTargetsCompositeContext(outer)
        def same = outer.graphSelection == inner.graphSelection
        if (!outerIsComposite && !same) Some(outer, inner) else None
    }
  }

  private def stitcher(
    fragment: Fragment,
    clauseExpansion: NestedFragment => Seq[Clause]
  ): StitchResult = {

    def stitch(fragment: Fragment, outermost: Boolean, outerUse: Option[Use]): StitchResult = {

      fragment match {
        case chain: Fragment.Chain =>
          val stitched = stitchChain(chain, outermost, outerUse)
          StitchResult(SingleQuery(stitched.clauses)(chain.pos), stitched.lastUse, stitched.useAppearances)

        case union: Fragment.Union =>
          val lhs = stitch(union.lhs, outermost, outerUse)
          val rhs = stitchChain(union.rhs, outermost, outerUse)
          val uses = lhs.useAppearances ++ rhs.useAppearances :+ UnionUse(lhs.lastUse, rhs.lastUse)
          val rhsQuery = SingleQuery(rhs.clauses)(union.rhs.pos)
          val result =
            if (union.distinct) {
              UnionDistinct(lhs.query, rhsQuery)(union.pos)
            } else {
              UnionAll(lhs.query, rhsQuery)(union.pos)
            }
          StitchResult(result, rhs.lastUse, uses)
      }
    }

    def stitchChain(chain: Fragment.Chain, outermost: Boolean, outerUse: Option[Use]): StitchChainResult = {

      def wrapped: NestedFragment = if (outermost) Outer(chain) else Inner(chain)

      chain match {

        case init: Fragment.Init =>
          val clauses = clauseExpansion(wrapped)
          StitchChainResult(clauses, init.use, Seq(ChainUse(outerUse, init.use)))

        case leaf: Fragment.Leaf =>
          val before = stitchChain(leaf.input, outermost, outerUse)
          val clauses = clauseExpansion(wrapped)
          before.copy(clauses = before.clauses ++ clauses)

        case apply: Fragment.Apply =>
          val before = stitchChain(apply.input, outermost, outerUse)
          val inner = stitch(apply.inner, outermost = false, Some(before.lastUse))
          val innerImports = inner.query.importColumns
          val scopeImports = apply.inner.importColumns
          val imports = (innerImports ++ scopeImports).distinct.map(Variable(_)(apply.pos, Variable.isIsolatedDefault))
          before.copy(
            clauses = before.clauses :+ ScopeClauseSubqueryCall(
              inner.query,
              isImportingAll = false,
              imports,
              apply.inTransactionsParameters,
              optional = false
            )(apply.pos),
            useAppearances = before.useAppearances ++ inner.useAppearances
          )

      }
    }

    stitch(fragment, outermost = true, outerUse = None)
  }
}

private object Ast {

  private def conditionally[T](cond: Boolean, prod: => T) =
    if (cond) Some(prod) else None

  private def variable(name: String, pos: InputPosition) =
    internal.expressions.Variable(name)(pos, Variable.isIsolatedDefault)

  def paramBindings(columns: Seq[String], pos: InputPosition): Option[With] =
    conditionally(
      columns.nonEmpty,
      With(ReturnItems(
        includeExisting = false,
        items =
          for {
            varName <- columns
            parName = Columns.paramName(varName)
          } yield AliasedReturnItem(
            expression = ExplicitParameter(parName, CTAny)(pos),
            variable = variable(varName, pos)
          )(pos)
      )(pos))(pos)
    )

  def inputDataStream(names: Seq[String], pos: InputPosition): Option[InputDataStream] =
    conditionally(
      names.nonEmpty,
      InputDataStream(
        variables = for {
          name <- names
        } yield variable(name, pos)
      )(pos)
    )

  def aliasedReturn(lastClause: Clause, names: Seq[String], pos: InputPosition): Option[Return] =
    lastClause match {
      case _: Return => None
      case _         => Some(aliasedReturn(names, pos))
    }

  def aliasedReturn(names: Seq[String], pos: InputPosition): Return =
    Return(ReturnItems(
      includeExisting = false,
      items =
        for {
          name <- names
        } yield AliasedReturnItem(
          expression = variable(name, pos),
          variable = variable(name, pos)
        )(pos)
    )(pos))(pos)

  def withoutGraphSelection(clauses: Seq[Clause]): Seq[Clause] =
    clauses.filter {
      case _: GraphSelection => false
      case _                 => true
    }

  def withoutGraphSelection(query: Query): Query = {
    query.rewritten.bottomUp {
      case sq: SingleQuery =>
        SingleQuery(clauses = withoutGraphSelection(sq.clauses))(sq.position)
    }
  }

  def chain[T <: AnyRef](rewrites: (T => T)*): T => T =
    rewrites.foldLeft(identity[T] _)(_ andThen _)

}
