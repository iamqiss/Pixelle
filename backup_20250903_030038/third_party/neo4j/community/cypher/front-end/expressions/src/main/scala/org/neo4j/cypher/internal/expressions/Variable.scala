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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

/* Note that isIsolated may be false, even if the variable was isolated (escaped or parenthesized).
 * Currently, isIsolated is only set to true for isolated variables in the Cypher5 parser.
 */
case class Variable(name: String)(val position: InputPosition, val isIsolated: Boolean) extends LogicalVariable {
  override def copyId: Variable = copy()(position, isIsolated)

  override def withPosition(position: InputPosition): LogicalVariable =
    copy()(position = position, Variable.isIsolatedDefault)

  override def renameId(newName: String): Variable = copy(name = newName)(position, isIsolated)

  override def asCanonicalStringVal: String = name

  override def dup(children: Seq[AnyRef]): this.type = {
    children.size match {
      case 1 => Variable(children.head.asInstanceOf[String])(position, isIsolated).asInstanceOf[this.type]
      case 2 => Variable(children.head.asInstanceOf[String])(
          children(1).asInstanceOf[InputPosition],
          isIsolated
        ).asInstanceOf[this.type]
      case 3 => Variable(children.head.asInstanceOf[String])(
          children(1).asInstanceOf[InputPosition],
          children(2).asInstanceOf[Boolean]
        ).asInstanceOf[this.type]
      case _ => throw new IllegalStateException("Variable has at least 1 and at most 3 children.")
    }
  }
}

object Variable {

  val isIsolatedDefault: Boolean = false

  implicit val byName: Ordering[Variable] =
    Ordering.by { (variable: Variable) =>
      (variable.name, variable.position)
    }(Ordering.Tuple2(implicitly[Ordering[String]], InputPosition.byOffset))
}

object UnPositionedVariable {
  def varFor(name: String): LogicalVariable = Variable(name)(InputPosition.NONE, Variable.isIsolatedDefault)
}
