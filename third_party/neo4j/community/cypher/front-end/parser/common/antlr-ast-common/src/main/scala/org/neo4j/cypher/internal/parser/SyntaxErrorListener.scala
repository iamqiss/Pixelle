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
package org.neo4j.cypher.internal.parser

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.neo4j.cypher.internal.parser.lexer.CypherToken
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

class SyntaxErrorListener(exceptionFactory: CypherExceptionFactory) extends BaseErrorListener {
  private[this] var _syntaxErrors = Seq.empty[Exception]

  def syntaxErrors: Seq[Exception] = _syntaxErrors

  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException
  ): Unit = {
    val position = offendingSymbol match {
      case cypherToken: CypherToken => cypherToken.position()
      case _                        => InputPosition(recognizer.getInputStream.index(), line, charPositionInLine)
    }
    _syntaxErrors = _syntaxErrors.appended(exceptionFactory.syntaxException(msg, position))
  }

  def reset(): Unit = _syntaxErrors = Seq.empty
}
