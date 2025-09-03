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

sealed trait NormalForm {
  def formName: String
  def description: String = s"$formName"
  override def toString: String = description
}

object NormalForm {

  def unapply(name: String): Option[NormalForm] = name match {
    case NFCNormalForm.formName  => Some(NFCNormalForm)
    case NFDNormalForm.formName  => Some(NFDNormalForm)
    case NFKCNormalForm.formName => Some(NFKCNormalForm)
    case NFKDNormalForm.formName => Some(NFKDNormalForm)
    case _                       => None
  }
}

case object NFCNormalForm extends NormalForm {
  val formName: String = "NFC"
}

case object NFDNormalForm extends NormalForm {
  val formName: String = "NFD"
}

case object NFKCNormalForm extends NormalForm {
  val formName: String = "NFKC"
}

case object NFKDNormalForm extends NormalForm {
  val formName: String = "NFKD"
}
