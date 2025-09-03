#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#encoding: utf-8
Feature: DynamicLabelsAcceptance

  Scenario Outline: Set dynamic labels
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH <label_definitions>
      MATCH (n)
      SET <query>
      RETURN labels(n) as newLabels
      """
    Then the result should be (ignoring element order for lists):
      | newLabels |
      | <labels>  |
    And the side effects should be:
      | +labels | <new_labels_count> |
    Examples:
      | existing_node | label_definitions           | query                            | labels     | new_labels_count |
      | ()            | "A" AS a                    | n:$(a)                           | ['A']      | 1                |
      | ()            | "A" AS a                    | n IS $(a)                        | ['A']      | 1                |
      | ()            | "A" AS a, "B" as b          | n:$(a):$(b)                      | ['B', 'A'] | 2                |
      | ()            | "A" AS a, "B" as b          | n IS $(a), n IS $(b)             | ['A', 'B'] | 2                |
      | ()            | 1 as numericLabel, "A" as a | n:$(a):$(toString(numericLabel)) | ['A','1']  | 2                |
      | (:A)          | "B" as b                    | n:$(b)                           | ['A', 'B'] | 1                |

  Scenario: Support parameters in dynamic labels
    Given an empty graph
    And parameters are:
      | a | 'A' |

    When executing query:
      """
       CREATE (n)
       SET n:$($a)
       RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels |
      | ['A']     |
    And the side effects should be:
      | +labels | 1 |
      | +nodes  | 1 |

  Scenario: Use property value as dynamic label
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {genre:"Horror"})
      """
    When executing query:
      """
      MATCH (n:Movie)
      SET n:$(n.genre)
      REMOVE n.genre
      RETURN labels(n) as newLabels
      """
    Then the result should be (ignoring element order for lists):
      | newLabels          |
      | ['Movie','Horror'] |
    And the side effects should be:
      | +labels     | 1 |
      | -properties | 1 |

  Scenario Outline: Set labels in a merge
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH "NewLabel" AS label, "OldLabel" AS label2
      MERGE (david:Person {name: "David"})
      ON CREATE
        SET david:$(label)
      ON MATCH
        SET david:$(label2)
      RETURN labels(david) AS newLabels
      """
    Then the result should be, in any order:
      | newLabels                   |
      | ['Person',<expected_label>] |
    And the side effects should be:
      | +labels     | <new_labels_count> |
      | +nodes      | <new_nodes_count>  |
      | +properties | <new_nodes_count>  |
    Examples:
      | existing_node             | expected_label | new_labels_count | new_nodes_count |
      | ()                        | 'NewLabel'     | 2                | 1               |
      | (:Person {name: "David"}) | 'OldLabel'     | 1                | 0               |

  Scenario: Take labels from the CSV file
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name       | role        |
      | 'David'    | 'ADMIN'     |
      | 'Tim'      | 'READ_ONLY' |
      | 'Gareth'   | 'READ_ONLY' |
      | 'Dawn'     | 'READ_ONLY' |
      | 'Jennifer' | 'ADMIN'     |

    When  executing query:
      """
      LOAD CSV WITH HEADERS FROM $param AS line
      CREATE (n {name: line.name})
      SET n:$(line.role)
      RETURN n
      """
    Then the result should be, in order:
      | n                             |
      | (:ADMIN {name: 'David'})      |
      | (:READ_ONLY {name: 'Tim'})    |
      | (:READ_ONLY {name: 'Gareth'}) |
      | (:READ_ONLY {name: 'Dawn'})   |
      | (:ADMIN {name: 'Jennifer'})   |
    And the side effects should be:
      | +nodes      | 5 |
      | +properties | 5 |
      | +labels     | 2 |

  Scenario Outline: Should throw syntax errors for setting labels using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      SET n:$(<invalid_expr>)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr                      |
      | 1 + 2                             |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |

  Scenario: Should throw type errors when setting labels with variables with null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH NULL AS a
      MATCH (n)
      SET n:$(a)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *

  @allowCustomErrors
  Scenario: Should throw token errors when setting labels with variables with empty strings
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH '' AS a
      MATCH (n)
      SET n:$(a)
      RETURN labels(n) AS labels
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors when setting labels with invalid values
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      SET n:$(a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value                     |
      | 1 + 2                             |
      | true                              |
      | {x : 1}                           |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |

  Scenario: Should throw type error if labels missing in the CSV file
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name    | role |
      | 'David' | ''   |

    When  executing query:
    """
    LOAD CSV WITH HEADERS FROM $param AS line
    CREATE (n {name: line.name})
    SET n:$(line.role)
    RETURN n
    """
    Then a TypeError should be raised at runtime: *

  Scenario Outline: Should throw syntax errors for setting labels where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      SET n:$($a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param |
      | 1             |
      | true          |
      | {x: 1}        |

  Scenario: Should throw type errors for setting labels where parameters evaluate to null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | null |
    When executing query:
      """
      MATCH (n)
      SET n:$($a)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *

  @allowCustomErrors
  Scenario: Should throw token name errors for setting labels where parameters evaluate to empty strings
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | '' |
    When executing query:
      """
      MATCH (n)
      SET n:$($a)
      RETURN labels(n) AS labels
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario Outline: Should throw type errors when a node property being set as a dynamic label is invalid
    Given an empty graph
    And having executed:
      """
      CREATE (:A{prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      SET n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *
    Examples:
      | invalid_value                     |
      | 1                                 |
      | null                              |
      | false                             |
      | localdatetime("2024185T19:32:24") |

  Scenario Outline: Should remove dynamic labels
    Given an empty graph
    And having executed:
      """
      CREATE <existing_node>
      """
    When executing query:
      """
      WITH <label_definitions>
      MATCH (n)
      REMOVE <query>
      RETURN labels(n) as newLabels
      """
    Then the result should be, in any order:
      | newLabels |
      | <labels>  |
    And the side effects should be:
      | -labels | <removed_labels_count> |
    Examples:
      | existing_node | label_definitions  | query                | labels | removed_labels_count |
      | (:A)          | "A" AS a           | n:$(a)               | []     | 1                    |
      | (:A)          | "A" AS a           | n IS $(a)            | []     | 1                    |
      | (:A:B)        | "A" AS a, "B" as b | n:$(a):$(b)          | []     | 2                    |
      | (:A:B)        | "A" AS a, "B" as b | n IS $(a), n IS $(b) | []     | 2                    |
      | (:A)          | "B" as b           | n:$(b)               | ['A']  | 0                    |

  Scenario: Set a list of labels
    Given an empty graph
    And having executed:
      """
      CREATE ({name:'Dave'})
      """
    When executing query:
      """
      WITH ["Person", "READ_ONLY"] AS labels
      MATCH (p)
      SET p:$(labels)
      RETURN p;
      """
    Then the result should be, in any order:
              | p                      |
              | (:Person:READ_ONLY{name:'Dave'}) |
            And the side effects should be:
              | +labels | 2 |

  Scenario: Remove a list of labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Person:ADMIN{name:'Dave'})
      CREATE (:Person:READ_ONLY:EXTERNAL{name:'John'})
      """
    When executing query:
      """
      WITH ["ADMIN", "READ_ONLY", "EXTERNAL"] AS labels
      MATCH (p:Person)
      REMOVE p:$(labels)
      RETURN p;
      """
    Then the result should be, in any order:
          | p                      |
          | (:Person{name:'Dave'}) |
          | (:Person{name:'John'}) |
        And the side effects should be:
          | -labels | 3 |

  Scenario: Update labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Person{name:'Dave'})
      """
    When executing query:
      """
      MATCH (n)
      WITH n, labels(n)[0] AS label
      REMOVE n:$(label)
      SET n:$(upper(label))
      RETURN n;
      """
    Then the result should be, in any order:
      | n                      |
      | (:PERSON{name:'Dave'}) |
    And the side effects should be:
      | -labels | 1 |
      | +labels | 1 |

  Scenario Outline: Should throw syntax errors for removing labels using invalid constant expressions
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      REMOVE n:$(<invalid_expr>)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_expr                      |
      | 1 + 2                             |
      | localdatetime("2024185T19:32:24") |
      | point({x:3,y:0})                  |

  Scenario Outline: Should throw syntax errors when removing labels using variables with invalid values
    Given an empty graph
    When executing query:
      """
      WITH <invalid_value> AS a
      MATCH (n)
      REMOVE n:$(a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_value                     |
      | 1 + 2                             |
      | true                              |
      | {x : 1}                           |
      | point({x:3,y:0})                  |
      | localdatetime("2024185T19:32:24") |

  Scenario Outline: Should throw syntax errors for removing labels where parameters evaluate to invalid values
    Given an empty graph
    And parameters are:
      | a | <invalid_param> |
    When executing query:
      """
      MATCH (n)
      REMOVE n:$($a)
      RETURN labels(n) AS labels
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | invalid_param    |
      | 1                |
      | {x: 2.3, y: 4.5} |
      | true             |

  Scenario: Should throw type errors for removing labels where parameters evaluates to null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    And parameters are:
      | a | null |
    When executing query:
      """
      MATCH (n)
      REMOVE n:$($a)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *

  Scenario Outline: Should throw type errors when a node property being removed as a dynamic label is invalid
    Given an empty graph
    And having executed:
      """
      CREATE (:A{prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *
    Examples:
      | invalid_value |
      | 1             |
      | null          |
      | true          |

  @allowCustomErrors
  Scenario: Should throw token error for settings labels where parameters evaluate to invalid token values
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop:''})
      """
    When executing query:
      """
      MATCH (n)
      SET n:$(n.prop)
      RETURN labels(n) AS labels
      """
    Then a TokenNameError should be raised at runtime: *

  Scenario Outline: Dynamic Labels in MATCH clause: all and default case
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Foo:Bar:Baz), (:Baz)
      """

    When executing query:
      """
       MATCH (n:$<all>(["Foo", "Bar"]))
       RETURN labels(n) AS labels;
      """
    Then the result should be (ignoring element order for lists):
      | labels                 |
      | ['Foo', 'Bar']         |
      | ['Foo', 'Bar', 'Baz']  |
    Examples:
      | all |
      | all |
      |     |

  Scenario: Dynamic Labels in MATCH clause: any
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Foo:Bar:Baz), (:Baz)
      """

    When executing query:
      """
       MATCH (n:$any(["Foo", "Bar"]))
       RETURN labels(n) AS labels;
      """
    Then the result should be (ignoring element order for lists):
      | labels                 |
      | ['Foo']                |
      | ['Foo', 'Bar']         |
      | ['Foo', 'Bar', 'Baz']  |

  Scenario Outline: Dynamic Labels in MATCH clause with empty list: all and default case
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Foo:Bar:Baz), (:Baz)
      """

    When executing query:
      """
       MATCH (n:$<all>([]))
       RETURN labels(n) AS labels;
      """
    Then the result should be (ignoring element order for lists):
      | labels                 |
      | ['Foo']                |
      | ['Baz']                |
      | ['Foo', 'Bar']         |
      | ['Foo', 'Bar', 'Baz']  |
    Examples:
      | all |
      | all |
      |     |

  Scenario: Dynamic Labels in MATCH clause with empty list: any
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Foo:Bar:Baz), (:Baz)
      """

    When executing query:
      """
       MATCH (n:$any([]))
       RETURN labels(n) AS labels;
      """
    Then the result should be, in any order:
      | labels                 |

  Scenario Outline: Dynamic Labels in MATCH should error if a null or empty string is present
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Foo:Bar:Baz), (:Baz)
      """

    When executing query:
      """
       MATCH (n:$<allOrAny>(<invalidValue>))
       RETURN labels(n) AS labels;
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | allOrAny | invalidValue |
      | all      | null         |
      | any      | null         |
      |          | null         |
      | all      | ''           |
      | any      | ''           |
      |          | ''           |
      | all      | [null]       |
      | any      | [null]       |
      |          | [null]       |
      | all      | ['']         |
      | any      | ['']         |
      |          | ['']         |
      | all      | 1 + 2        |
      | any      | 1 + 2        |
      |          | 1 + 2        |

  Scenario Outline: Dynamic Types in MATCH clause: all and default case
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo)-[:REL1]->(:Foo:Bar), (:Foo:Bar:Baz)-[:REL2]->(:Baz)
      """

    When executing query:
      """
       MATCH ()-[r:$<all>(["REL1", "REL2"])]->()
       RETURN r;
      """
    Then the result should be, in any order:
      | r |
    Examples:
      | all |
      | all |
      |     |

  Scenario: Dynamic Types in MATCH clause: any
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo)-[:REL1]->(:Foo:Bar), (:Foo:Bar:Baz)-[:REL2]->(:Baz)
      """

    When executing query:
      """
       MATCH ()-[r:$any(["REL1", "REL2"])]->()
       RETURN type(r) as types
      """
    Then the result should be, in any order:
      | types |
      | 'REL1' |
      | 'REL2' |

  Scenario Outline: Dynamic Types in CREATE/MERGE clause
    Given an empty graph

    When executing query:
      """
       <clause> ()-[r:$(<input>)]->()
       RETURN type(r) AS types
      """
    Then the result should be, in any order:
      | types  |
      | 'FOO'  |
    And the side effects should be:
      | +relationships | 1 |
      | +nodes         | 2 |
    Examples:
      | clause | input   |
      | CREATE | 'FOO'   |
      | MERGE  | 'FOO'   |
      | CREATE | ['FOO'] |
      | MERGE  | ['FOO'] |

  Scenario Outline: CREATE or MERGE with invalid input for Dynamic Types
    Given an empty graph
    When executing query:
      """
       <clause> ()-[:$(<invalidInput>)]->()
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | clause | invalidInput   |
      | CREATE | null           |
      | MERGE  | null           |
      | CREATE | ''             |
      | MERGE  | ''             |
      | CREATE | []             |
      | MERGE  | []             |
      | CREATE | [null]         |
      | MERGE  | [null]         |
      | CREATE | ['']           |
      | MERGE  | ['']           |
      | CREATE | ['Foo', 'Bar'] |
      | MERGE  | ['Foo', 'Bar'] |
      | CREATE | 1 + 2          |
      | MERGE  | 1 + 2          |

  Scenario: Dynamic Labels/Types in with parameters in CREATE clause
    Given an empty graph
    And parameters are:
      | a | 'label1'   |
      | b | 'label2'   |
      | c | {prop: 1}  |
      | d | 'TYPE'     |

    When executing query:
      """
       CREATE (n :$($a):$($b) $c)-[r:$($d) $c]->()
       RETURN labels(n) AS labels, type(r) AS type  
      """
    Then the result should be (ignoring element order for lists):
      | labels               | type   |
      | ['label1', 'label2'] | 'TYPE' |
    And the side effects should be:
      | +labels        | 2 |
      | +relationships | 1 |
      | +nodes         | 2 |
      | +properties    | 2 |

  Scenario: Dynamic Labels/Types in with variables in CREATE clause
    Given an empty graph

    When executing query:
      """
       WITH "label1" AS a, "label2" AS b, "TYPE" AS c
       CREATE (n :$(a):$(b))-[r:$(c)]->()
       RETURN labels(n) AS labels, type(r) AS type  
      """
    Then the result should be (ignoring element order for lists):
      | labels               | type   |
      | ['label1', 'label2'] | 'TYPE' |
    And the side effects should be:
      | +labels        | 2 |
      | +relationships | 1 |
      | +nodes         | 2 |

  Scenario: Dynamic Labels from a Collect in CREATE clause
    Given an empty graph

    When executing query:
      """
       CREATE (n :$(COLLECT { UNWIND range(0, 3) AS id RETURN "Node_" + id }))
       RETURN labels(n) AS labels
      """
    Then the result should be (ignoring element order for lists):
      | labels                                   |
      | ['Node_0', 'Node_1', 'Node_2', 'Node_3'] |
    And the side effects should be:
      | +labels        | 4 |
      | +nodes         | 1 |

  Scenario: Dynamic Labels with a negation label expression in MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Bar)
      """

    When executing query:
      """
       MATCH (n:$("Foo")&!$("Bar")) 
       RETURN labels(n) AS labels
      """
    Then the result should be, in any order:
      | labels  |
      | ['Foo'] |

  Scenario: Dynamic Labels empty list in MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Bar)
      """

    When executing query:
      """
       MATCH (n:$([])) 
       RETURN count(n) AS count
      """
    Then the result should be, in any order:
      | count  |
      | 3      |

  Scenario: Dynamic Labels in MATCH with negation on any - !$any(labels) == !(label1|label2)
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Bar)
      """

    When executing query:
      """
       WITH ["Foo", "Bar"] AS labels
       MATCH (n :!$any(labels))
       RETURN count(n) as count
      """
    Then the result should be, in any order:
      | count  |
      | 0      |

  Scenario: Dynamic Labels in MATCH with negation on all - !$all(labels) == !(label1&label2)
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Bar)
      """

    When executing query:
      """
       WITH ["Foo", "Bar"] AS labels
       MATCH (n :!$all(labels))
       RETURN count(n) as count
      """
    Then the result should be, in any order:
      | count  |
      | 2      |

  Scenario: Dynamic Labels in MATCH with all and any - !$all(labels)&$any(labels)) == !(label1&label2)&(label1|label2)
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo), (:Foo:Bar), (:Bar), (:Baz)
      """

    When executing query:
      """
       WITH ["Foo", "Bar"] AS labels
       MATCH (n :!$all(labels)&$any(labels)) 
       RETURN labels(n) AS returnedLabels
      """
    Then the result should be, in any order:
      | returnedLabels  |
      | ['Foo']         |
      | ['Bar']         |

  Scenario Outline: Should throw type errors when a node property being used as a dynamic label is invalid
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      <clause> (m:$(n.prop))
      RETURN labels(m) AS labels
      """
    Then a TypeError should be raised at runtime: *
    Examples:
      | clause | invalid_value |
      | CREATE | 1             |
      | MERGE  | 1             |
      | CREATE | null          |
      | MERGE  | null          |

  @allowCustomErrors
  Scenario Outline: Should throw token type errors when a node property being used as a dynamic label is invalid token
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop:<invalid_value>})
      """
    When executing query:
      """
      MATCH (n)
      <clause> (m:$(n.prop))
      RETURN labels(m) AS labels
      """
    Then a TokenNameError should be raised at runtime: *
    Examples:
      | clause | invalid_value |
      | CREATE | ''            |
      | MERGE  | ''            |


  Scenario: Should throw type errors when removing dynamic labels that resolve to null
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      WITH NULL AS a
      MATCH (n)
      REMOVE n:$(a)
      RETURN labels(n) AS labels
      """
    Then a TypeError should be raised at runtime: *
    
  Scenario: Dynamic Labels from a Collect in CREATE clause acting on graph data
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(:B) 
      CREATE (:A)-[:T2]->(:B) 
      CREATE (:A)-[:T3]->(:B)
      """
    When executing query:
      """
       CREATE (n :$(COLLECT { MATCH (:A)-[r]-(:B) RETURN type(r) }))
       RETURN labels(n) AS labels
      """
    Then the result should be (ignoring element order for lists):
      | labels             |
      | ['T1', 'T2', 'T3'] |
    And the side effects should be:
      | +labels        | 3 |
      | +nodes         | 1 |

  @allowCustomErrors
  Scenario: Node self reference in CREATE inside dynamic labels is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (n:$(labels(n)))
    """
    Then a SyntaxError should be raised at compile time: The Node variable 'n' is referencing a Node that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.
    
  @allowCustomErrors
  Scenario: Node self reference in CREATE of properties inside dynamic labels is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (n:$(n.prop) {prop:5})
    """
    Then a SyntaxError should be raised at compile time: The Node variable 'n' is referencing a Node that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.
    
  @allowCustomErrors
  Scenario: Relationship self reference in CREATE inside dynamic labels is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE ()-[r:$(type(r))]->()
    """
    Then a SyntaxError should be raised at compile time: The Relationship variable 'r' is referencing a Relationship that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.
    
  @allowCustomErrors
  Scenario: Relationship self reference in CREATE of properties inside dynamic labels is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE ()-[r:$(r.prop) {prop:5}]->()
    """
    Then a SyntaxError should be raised at compile time: The Relationship variable 'r' is referencing a Relationship that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.
    
  @allowCustomErrors
  Scenario: Node self reference inside subqueries inside dynamic labels is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (:_1)
    CREATE (n:$(COLLECT { 
      MATCH (n) WITH COUNT(n) AS cnt RETURN "_"+cnt 
    })) 
    RETURN substring(labels(n)[0], 1)
    """
    Then a SyntaxError should be raised at compile time: The Node variable 'n' is referencing a Node that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.
