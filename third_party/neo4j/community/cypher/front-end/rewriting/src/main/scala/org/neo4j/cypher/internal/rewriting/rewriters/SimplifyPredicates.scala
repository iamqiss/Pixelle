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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ListType

/**
 * Rewrites [[org.neo4j.cypher.internal.expressions.IterablePredicateExpression]]s to IN expressions when possible.
 *
 * For example:
 * any(x IN list WHERE x = 1) ==> 1 IN x
 * none(x IN list WHERE x = 2) ==> not(2 IN x)
 */
case object SimplifyPredicates extends Rewriter with BottomUpMergeableRewriter {

  override def apply(plan: AnyRef): AnyRef = {
    instance(plan)
  }

  val instance: Rewriter = bottomUp(innerRewriter)

  override def innerRewriter: Rewriter = Rewriter.lift {
    case any @ AnyIterablePredicate(SimpleEqualsFilterScope(inLhs), list) => In(inLhs, list)(any.position)
    case none @ NoneIterablePredicate(SimpleEqualsFilterScope(inLhs), list) =>
      Not(In(inLhs, list)(none.position))(none.position)
  }
}

object SimpleEqualsFilterScope {

  def unapply(scope: FilterScope): Option[Expression] = scope match {
    case FilterScope(scope, Some(EqualEquivalent(lhs, rhs))) if scope == lhs && !rhs.dependencies.contains(scope) =>
      Some(rhs)
    case FilterScope(scope, Some(EqualEquivalent(lhs, rhs))) if scope == rhs && !lhs.dependencies.contains(scope) =>
      Some(lhs)
    case _ => None
  }
}

object EqualEquivalent {

  def unapply(expression: Expression): Option[(Expression, Expression)] = expression match {
    case Equals(lhs, rhs)                      => Some((lhs, rhs))
    case In(lhs, ListLiteral(Seq(singleItem))) => Some((lhs, singleItem))
    case In(lhs, p @ Parameter(_, _: ListType, ExactSize(1))) =>
      Some((lhs, ContainerIndex(p, SignedDecimalIntegerLiteral("0")(p.position))(p.position)))
    case _ => None
  }
}
