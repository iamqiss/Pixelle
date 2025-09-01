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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.symbols.CTAny

class CreateMatchMergeParserTest extends AstParsingTestBase {

  private val nodeFillers: Seq[(String, NodePattern)] = Seq(
    // No labels
    ("", nodePat()),
    ("n", nodePat(Some("n"))),
    ("{prop: 1}", nodePat(None, None, Some(mapOf(("prop", literalInt(1)))))),
    (
      "x {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        None,
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // IS A
    ("IS A", nodePat(None, Some(labelLeaf("A", containsIs = true)))),
    ("n IS A", nodePat(Some("n"), Some(labelLeaf("A", containsIs = true)))),
    (
      "IS A {prop: 1}",
      nodePat(
        None,
        Some(labelLeaf("A", containsIs = true)),
        Some(mapOf(("prop", literalInt(1))))
      )
    ),
    (
      "x IS A {}",
      nodePat(
        Some("x"),
        Some(labelLeaf("A", containsIs = true)),
        Some(mapOf())
      )
    ),

    // IS $(a)
    ("IS $(a)", nodePat(None, Some(dynamicLabelLeaf(varFor("a"), containsIs = true)))),
    ("IS $all(a)", nodePat(None, Some(dynamicLabelLeaf(varFor("a"), containsIs = true)))),
    ("IS $any(a)", nodePat(None, Some(dynamicLabelLeaf(varFor("a"), all = false, containsIs = true)))),
    ("n IS $(a)", nodePat(Some("n"), Some(dynamicLabelLeaf(varFor("a"), containsIs = true)))),
    (
      "IS $(a) {prop: 1}",
      nodePat(
        None,
        Some(dynamicLabelLeaf(varFor("a"), containsIs = true)),
        Some(mapOf(("prop", literalInt(1))))
      )
    ),
    (
      "x IS $(a) {}",
      nodePat(
        Some("x"),
        Some(dynamicLabelLeaf(varFor("a"), containsIs = true)),
        Some(mapOf())
      )
    ),

    // IS A&B
    (
      "IS A&B",
      nodePat(
        None,
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "n IS A&B",
      nodePat(
        Some("n"),
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "IS A&B {prop: $value}",
      nodePat(
        None,
        Some(
          labelConjunction(labelLeaf("A", containsIs = true), labelLeaf("B", containsIs = true), containsIs = true)
        ),
        Some(mapOf(("prop", parameter("value", CTAny))))
      )
    ),
    (
      "x IS A&B {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        )),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // IS $("A")&$("B")
    (
      "IS $(\"A\")&$(\"B\")",
      nodePat(
        None,
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A"), containsIs = true),
          dynamicLabelLeaf(literalString("B"), containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "IS $all(\"A\")&$any(\"B\")",
      nodePat(
        None,
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A"), containsIs = true),
          dynamicLabelLeaf(literalString("B"), all = false, containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "n IS $(\"A\")&$(\"B\")",
      nodePat(
        Some("n"),
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A"), containsIs = true),
          dynamicLabelLeaf(literalString("B"), containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "IS $(\"A\")&$(\"B\") {prop: $value}",
      nodePat(
        None,
        Some(
          labelConjunction(
            dynamicLabelLeaf(literalString("A"), containsIs = true),
            dynamicLabelLeaf(literalString("B"), containsIs = true),
            containsIs = true
          )
        ),
        Some(mapOf(("prop", parameter("value", CTAny))))
      )
    ),
    (
      "x IS $(\"A\")&$(\"B\") {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A"), containsIs = true),
          dynamicLabelLeaf(literalString("B"), containsIs = true),
          containsIs = true
        )),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // :A
    (":A", nodePat(None, Some(labelLeaf("A")))),
    ("n :A", nodePat(Some("n"), Some(labelLeaf("A")))),
    (
      ":A $map",
      nodePat(
        None,
        Some(labelLeaf("A")),
        Some(parameter("map", CTAny))
      )
    ),
    (
      "x :A {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(labelLeaf("A")),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // :$(n.name)
    (":$(n.name)", nodePat(None, Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"))))),
    ("n :$(n.name)", nodePat(Some("n"), Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"))))),
    (
      ":$(n.name) $map",
      nodePat(
        None,
        Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"))),
        Some(parameter("map", CTAny))
      )
    ),
    (
      ":$all(n.name) $map",
      nodePat(
        None,
        Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"))),
        Some(parameter("map", CTAny))
      )
    ),
    (
      ":$any(n.name) $map",
      nodePat(
        None,
        Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"), all = false)),
        Some(parameter("map", CTAny))
      )
    ),
    (
      "x :$(n.name) {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(dynamicLabelLeaf(propExpression(varFor("n"), "name"))),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // :A&B
    (
      ":A&B",
      nodePat(
        None,
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B")))
      )
    ),
    (
      "n :A&B",
      nodePat(
        Some("n"),
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B")))
      )
    ),
    (
      ":A&B {prop: duration('P1Y')}",
      nodePat(
        None,
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B"))),
        Some(mapOf(("prop", function("duration", literalString("P1Y")))))
      )
    ),
    (
      "x :A&B {prop1: 'a', prop2: false}",
      nodePat(
        Some("x"),
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B"))),
        Some(mapOf(("prop1", literalString("a")), ("prop2", falseLiteral)))
      )
    ),

    // :$("A")&$("B")
    (
      ":$(\"A\")&$(\"B\")",
      nodePat(
        None,
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"))
        ))
      )
    ),
    (
      ":($(\"A\")&$(\"B\"))|$(\"C\")",
      nodePat(
        None,
        Some(labelDisjunctions(Seq(
          labelConjunction(
            dynamicLabelLeaf(literalString("A")),
            dynamicLabelLeaf(literalString("B"))
          ),
          dynamicLabelLeaf(literalString("C"))
        )))
      )
    ),
    (
      "n :$(\"A\")&$(\"B\")",
      nodePat(
        Some("n"),
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"))
        ))
      )
    ),
    (
      "n :$all(\"A\")&$any(\"B\")",
      nodePat(
        Some("n"),
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"), all = false)
        ))
      )
    ),
    (
      ":$(\"A\")&$(\"B\") {prop: duration('P1Y')}",
      nodePat(
        None,
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"))
        )),
        Some(mapOf(("prop", function("duration", literalString("P1Y")))))
      )
    ),
    (
      ":$(\"A\")&$(\"B\") $map",
      nodePat(
        None,
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"))
        )),
        Some(parameter("map", CTAny))
      )
    ),
    (
      "x :$(\"A\")&$(\"B\") {prop1: 'a', prop2: false}",
      nodePat(
        Some("x"),
        Some(labelConjunction(
          dynamicLabelLeaf(literalString("A")),
          dynamicLabelLeaf(literalString("B"))
        )),
        Some(mapOf(("prop1", literalString("a")), ("prop2", falseLiteral)))
      )
    ),
    (":Foo", nodePat(None, Some(labelLeaf("Foo")))),
    (":$(\"Foo\")", nodePat(None, Some(dynamicLabelLeaf(literalString("Foo"))))),
    (":$(toUpper(\"foo\"))", nodePat(None, Some(dynamicLabelLeaf(function("toUpper", literalString("foo")))))),
    (
      ":$(\"Foo\"):$(\"Bar\"):Baz",
      nodePat(
        None,
        Some(labelColonConjunction(
          labelColonConjunction(
            dynamicLabelLeaf(literalString("Foo")),
            dynamicLabelLeaf(literalString("Bar"))
          ),
          labelLeaf("Baz")
        ))
      )
    ),
    (
      ":$(\"Foo\"):Bar",
      nodePat(
        None,
        Some(labelColonConjunction(dynamicLabelLeaf(literalString("Foo")), labelLeaf("Bar")))
      )
    ),
    (
      ":$all(\"Foo\"):Bar",
      nodePat(
        None,
        Some(labelColonConjunction(dynamicLabelLeaf(literalString("Foo")), labelLeaf("Bar")))
      )
    ),
    (
      ":$any(\"Foo\"):Bar",
      nodePat(
        None,
        Some(labelColonConjunction(dynamicLabelLeaf(literalString("Foo"), all = false), labelLeaf("Bar")))
      )
    ),
    (
      ":Foo:$(\"Bar\")",
      nodePat(
        None,
        Some(labelColonConjunction(labelLeaf("Foo"), dynamicLabelLeaf(literalString("Bar"))))
      )
    ),
    ("IS Foo", nodePat(None, Some(labelLeaf("Foo", containsIs = true)))),
    ("IS $(\"Foo\")", nodePat(None, Some(dynamicLabelLeaf(literalString("Foo"), containsIs = true)))),
    (
      ":$(label) $map",
      nodePat(
        None,
        Some(dynamicLabelLeaf(varFor("label"))),
        Some(parameter("map", CTAny))
      )
    )
  )

  private def relFillers(direction: SemanticDirection): Seq[(String, RelationshipPattern)] = Seq(
    // No types
    ("", relPat(None, None, None, None, None, direction)),
    ("n", relPat(Some("n"), None, None, None, None, direction)),
    ("{prop: 1}", relPat(None, None, None, Some(mapOf(("prop", literalInt(1)))), None, direction)),
    (
      "x {prop1: 'a', prop2: 42}",
      relPat(
        Some("x"),
        None,
        None,
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42)))),
        None,
        direction
      )
    ),

    // IS A
    ("IS A", relPat(None, Some(labelRelTypeLeaf("A", containsIs = true)), direction = direction)),
    ("n IS A", relPat(Some("n"), Some(labelRelTypeLeaf("A", containsIs = true)), direction = direction)),
    (
      "IS A {prop: 1}",
      relPat(
        None,
        Some(labelRelTypeLeaf("A", containsIs = true)),
        None,
        Some(mapOf(("prop", literalInt(1)))),
        direction = direction
      )
    ),
    (
      "x IS A {}",
      relPat(
        Some("x"),
        Some(labelRelTypeLeaf("A", containsIs = true)),
        None,
        Some(mapOf()),
        direction = direction
      )
    ),

    // IS $(a)
    ("IS $(a)", relPat(None, Some(dynamicRelTypeLeaf(varFor("a"), containsIs = true)), direction = direction)),
    ("IS $all(a)", relPat(None, Some(dynamicRelTypeLeaf(varFor("a"), containsIs = true)), direction = direction)),
    (
      "IS $any(a)",
      relPat(None, Some(dynamicRelTypeLeaf(varFor("a"), all = false, containsIs = true)), direction = direction)
    ),
    (
      "n IS $(a)",
      relPat(Some("n"), Some(dynamicRelTypeLeaf(varFor("a"), containsIs = true)), direction = direction)
    ),
    (
      "IS $(a) {prop: 1}",
      relPat(
        None,
        Some(dynamicRelTypeLeaf(varFor("a"), containsIs = true)),
        None,
        Some(mapOf(("prop", literalInt(1)))),
        direction = direction
      )
    ),
    (
      "x IS $(a) {}",
      relPat(
        Some("x"),
        Some(dynamicRelTypeLeaf(varFor("a"), containsIs = true)),
        None,
        Some(mapOf()),
        direction = direction
      )
    ),

    // :A
    (":A", relPat(None, Some(labelRelTypeLeaf("A")), direction = direction)),
    ("n :A", relPat(Some("n"), Some(labelRelTypeLeaf("A")), direction = direction)),
    (
      ":A $map",
      relPat(
        None,
        Some(labelRelTypeLeaf("A")),
        None,
        Some(parameter("map", CTAny)),
        direction = direction
      )
    ),
    (
      "x :A {prop1: 'a', prop2: 42}",
      relPat(
        Some("x"),
        Some(labelRelTypeLeaf("A")),
        None,
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42)))),
        direction = direction
      )
    ),

    // :$(n.name)
    (
      ":$(n.name)",
      relPat(None, Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"))), direction = direction)
    ),
    (
      ":$all(n.name)",
      relPat(None, Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"))), direction = direction)
    ),
    (
      ":$any(n.name)",
      relPat(
        None,
        Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"), all = false)),
        direction = direction
      )
    ),
    (
      "n :$(n.name)",
      relPat(Some("n"), Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"))), direction = direction)
    ),
    (
      ":$(n.name) $map",
      relPat(
        None,
        Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"))),
        None,
        Some(parameter("map", CTAny)),
        direction = direction
      )
    ),
    (
      "x :$(n.name) {prop1: 'a', prop2: 42}",
      relPat(
        Some("x"),
        Some(dynamicRelTypeLeaf(propExpression(varFor("n"), "name"))),
        None,
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42)))),
        direction = direction
      )
    )
  )

  for {
    (nodeText1, nodePattern1) <- nodeFillers
  } yield {
    test(s"CREATE ($nodeText1)") {
      parsesTo[Clause](
        create(
          nodePattern1
        )
      )
    }

    test(s"MATCH ($nodeText1)") {
      parsesTo[Clause](
        match_(
          nodePattern1
        )
      )
    }
    test(s"MERGE ($nodeText1)") {
      parsesTo[Clause](
        merge(
          nodePattern1
        )
      )
    }
  }

  for {
    (relText, relPattern) <- relFillers(SemanticDirection.OUTGOING)
  } yield {
    test(s"CREATE ()-[$relText]->()") {
      parsesTo[Clause](
        create(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
    test(s"MATCH ()-[$relText]->()") {
      parsesTo[Clause](
        match_(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
    test(s"MERGE ()-[$relText]->()") {
      parsesTo[Clause](
        merge(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
  }

  for {
    (relText, relPattern) <- relFillers(SemanticDirection.INCOMING)
  } yield {
    test(s"CREATE ()<-[$relText]-()") {
      parsesTo[Clause](
        create(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
    test(s"MATCH ()<-[$relText]-()") {
      parsesTo[Clause](
        match_(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
    test(s"MERGE ()<-[$relText]-()") {
      parsesTo[Clause](
        merge(
          relationshipChain(
            nodePat(),
            relPattern,
            nodePat()
          )
        )
      )
    }
  }

  test("CREATE (n:$())") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ')'")
      case _ => _.withSyntaxError(
          """Invalid input ')': expected an expression (line 1, column 13 (offset: 12))
            |"CREATE (n:$())"
            |             ^""".stripMargin
        )
    }
  }

}
