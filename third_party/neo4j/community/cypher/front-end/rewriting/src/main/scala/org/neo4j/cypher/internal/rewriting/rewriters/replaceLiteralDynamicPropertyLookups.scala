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

import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object replaceLiteralDynamicPropertyLookups extends Step with DefaultPostCondition with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val instance: Rewriter = topDown(Rewriter.lift {
    case dP @ SetDynamicPropertyItem(
        dynamicPropertyLookup @ ContainerIndex(expr, lit: StringLiteral),
        expression: Expression
      ) =>
      SetPropertyItem(
        Property(expr, PropertyKeyName(lit.value)(lit.position))(dynamicPropertyLookup.position),
        expression
      )(dP.position)
    case _ @RemoveDynamicPropertyItem(
        dynamicPropertyLookup @ ContainerIndex(expr, lit: StringLiteral)
      ) =>
      RemovePropertyItem(
        Property(expr, PropertyKeyName(lit.value)(lit.position))(dynamicPropertyLookup.position)
      )
    case index @ ContainerIndex(expr, lit: StringLiteral) =>
      Property(expr, PropertyKeyName(lit.value)(lit.position))(index.position)
  })

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance
}
