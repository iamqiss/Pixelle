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
package org.neo4j.cypher.internal.util.attribution

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink

class ImmutableAttributeTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  case class Deduplicable(value: String)(val id: Int) {
    override def toString: String = s"$value $id"
  }

  test("random key values") {
    forAll(genKeyValues(), minSuccessful(20)) { values =>
      val uniqueValues = values.toMap.toSeq
      val attributes = ImmutableAttribute(uniqueValues)
      val dedupAttributes = ImmutableAttribute.deduplicated(uniqueValues.iterator)

      dedupAttributes shouldBe attributes
      dedupAttributes.hashCode() shouldBe attributes.hashCode()

      uniqueValues.foreach { case (id, value) =>
        attributes.isDefinedAt(id) shouldBe true
        attributes.apply(id) shouldBe value
        attributes.get(id) shouldBe value
        dedupAttributes.isDefinedAt(id) shouldBe true
        dedupAttributes.apply(id) shouldBe value
        dedupAttributes.get(id) shouldBe value
      }
      attributes.orderedIterator.toSeq shouldBe uniqueValues.sortBy(_._1.x)
      dedupAttributes.orderedIterator.toSeq shouldBe uniqueValues.sortBy(_._1.x)
    }
  }

  test("no deduplication") {
    val attributes = ImmutableAttribute(Seq(
      Id(0) -> Deduplicable("a")(0),
      Id(1) -> Deduplicable("a")(1),
      Id(2) -> Deduplicable("b")(2),
      Id(3) -> Deduplicable("a")(3),
      Id(4) -> Deduplicable("a")(4)
    ))
    attributes.get(Id(0)).id shouldBe 0
    attributes.get(Id(1)).id shouldBe 1
    attributes.get(Id(2)).id shouldBe 2
    attributes.get(Id(3)).id shouldBe 3
    attributes.get(Id(4)).id shouldBe 4
  }

  test("deduplication") {
    val attributes = ImmutableAttribute.deduplicated(Iterator(
      Id(0) -> Deduplicable("a")(0),
      Id(1) -> Deduplicable("a")(1),
      Id(2) -> Deduplicable("b")(2),
      Id(3) -> Deduplicable("a")(3),
      Id(4) -> Deduplicable("a")(4)
    ))
    val choosenId = attributes.get(Id(0)).id
    attributes.get(Id(1)).id shouldBe choosenId
    attributes.get(Id(3)).id shouldBe choosenId
    attributes.get(Id(4)).id shouldBe choosenId
    attributes.orderedIterator.size shouldBe 5
    attributes.orderedIterator.map(_._2.id).toSet shouldBe Set(choosenId, 2)
  }

  private def genKeyValues(): Gen[IndexedSeq[(Id, String)]] = {
    val sizeHint = 128
    for {
      size <- Gen.choose(0, sizeHint)
      values <- Gen.oneOf(
        Gen.containerOfN[IndexedSeq, Option[(Id, String)]](size, randomValue(size)),
        Gen.const(Range(0, size).map(i => Option(Id(i) -> i.toString))),
        Gen.const(Range(0, size, 5).map(i => Option(Id(i) -> i.toString))),
        Gen.const(Range(0, size).map(i => Option(Id(i) -> (i % 5).toString))),
        Gen.const(Range(0, size, 5).map(i => Option(Id(i) -> (i % 5).toString))),
        Gen.const(Range(0, size).map(i => Option.empty))
      )
    } yield values.flatten
  }

  private def randomValue(size: Int): Gen[Option[(Id, String)]] = {
    for {
      defined <- Arbitrary.arbitrary[Boolean]
      id <- Gen.choose(0, size)
      value <- Gen.choose(0, size)
    } yield Option.when(defined)(Id(id) -> value.toString)
  }
}
