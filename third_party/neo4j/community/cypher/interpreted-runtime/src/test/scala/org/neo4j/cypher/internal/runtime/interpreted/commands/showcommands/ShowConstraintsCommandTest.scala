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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.mockito.Mockito.when
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllExistsConstraints
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.NodeAllExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropExistsConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.PropExistsConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RelAllExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropExistsConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexStatus
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class ShowConstraintsCommandTest extends ShowCommandTestBase {

  private val defaultColumns =
    ShowConstraintsClause(
      AllConstraints,
      None,
      List.empty,
      yieldAll = false
    )(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val allColumns =
    ShowConstraintsClause(
      AllConstraints,
      None,
      List.empty,
      yieldAll = true
    )(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val optionsMap = VirtualValues.map(
    Array("indexProvider", "indexConfig"),
    Array(Values.stringValue(AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name()), VirtualValues.EMPTY_MAP)
  )

  private val nodeUniquenessIndexDescriptor =
    IndexPrototype.uniqueForSchema(labelDescriptor, AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
      .withName("constraint0")
      .materialise(0)
      .withOwningConstraintId(1)

  private val relUniquenessIndexDescriptor =
    IndexPrototype.uniqueForSchema(relTypeDescriptor, AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
      .withName("constraint2")
      .materialise(3)
      .withOwningConstraintId(4)

  private val nodeKeyIndexDescriptor =
    IndexPrototype.uniqueForSchema(labelDescriptor, AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
      .withName("constraint3")
      .materialise(5)
      .withOwningConstraintId(6)

  private val relKeyIndexDescriptor =
    IndexPrototype.uniqueForSchema(relTypeDescriptor, AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
      .withName("constraint4")
      .materialise(7)
      .withOwningConstraintId(8)

  private val nodeUniquenessConstraintDescriptor =
    ConstraintDescriptorFactory.uniqueForSchema(nodeUniquenessIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint0")
      .withOwnedIndexId(0)
      .withId(1)

  private val relUniquenessConstraintDescriptor =
    ConstraintDescriptorFactory.uniqueForSchema(relUniquenessIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint2")
      .withOwnedIndexId(3)
      .withId(4)

  private val nodeKeyConstraintDescriptor =
    ConstraintDescriptorFactory.keyForSchema(nodeKeyIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint3")
      .withOwnedIndexId(5)
      .withId(6)

  private val relKeyConstraintDescriptor =
    ConstraintDescriptorFactory.keyForSchema(relKeyIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint4")
      .withOwnedIndexId(7)
      .withId(8)

  private val nodeExistConstraintDescriptor =
    ConstraintDescriptorFactory.existsForSchema(labelDescriptor, false)
      .withName("constraint5")
      .withId(9)

  private val relExistConstraintDescriptor =
    ConstraintDescriptorFactory.existsForSchema(relTypeDescriptor, false)
      .withName("constraint1")
      .withId(2)

  private val nodePropTypeConstraintDescriptor =
    ConstraintDescriptorFactory.typeForSchema(labelDescriptor, PropertyTypeSet.of(SchemaValueType.BOOLEAN), false)
      .withName("constraint10")
      .withId(10)

  private val relPropTypeConstraintDescriptor =
    ConstraintDescriptorFactory.typeForSchema(relTypeDescriptor, PropertyTypeSet.of(SchemaValueType.STRING), false)
      .withName("constraint11")
      .withId(11)

  private val nodeLabelExistenceConstraintDescriptor =
    ConstraintDescriptorFactory.nodeLabelExistenceForSchema(
      SchemaDescriptors.forNodeLabelExistence(0),
      1
    )
      .withName("constraint12")
      .withId(12)

  private val relSourceConstraintDescriptor =
    ConstraintDescriptorFactory.relationshipEndpointLabelForSchema(
      relEndpointLabelDescriptor,
      labelDescriptor.getLabelId,
      EndpointType.START
    )
      .withName("constraint13")
      .withId(13)

  private val relTargetConstraintDescriptor =
    ConstraintDescriptorFactory.relationshipEndpointLabelForSchema(
      relEndpointLabelDescriptor,
      labelDescriptor.getLabelId,
      EndpointType.END
    )
      .withName("constraint14")
      .withId(14)

  private val nodeUniquenessConstraintInfo =
    ConstraintInfo(List(label), List(prop), Some(nodeUniquenessIndexDescriptor), None, None)

  private val relUniquenessConstraintInfo =
    ConstraintInfo(List(relType), List(prop), Some(relUniquenessIndexDescriptor), None, None)

  private val nodeKeyConstraintInfo = ConstraintInfo(List(label), List(prop), Some(nodeKeyIndexDescriptor), None, None)
  private val relKeyConstraintInfo = ConstraintInfo(List(relType), List(prop), Some(relKeyIndexDescriptor), None, None)
  private val nodeExistConstraintInfo = ConstraintInfo(List(label), List(prop), None, None, None)
  private val relExistConstraintInfo = ConstraintInfo(List(relType), List(prop), None, None, None)
  private val nodePropTypeConstraintInfo = ConstraintInfo(List(label), List(prop), None, None, None)
  private val relPropTypeConstraintInfo = ConstraintInfo(List(relType), List(prop), None, None, None)
  private val constraintInfo = ConstraintInfo(List(label), List(), None, Some(label2), None)

  private val relSourceConstraintInfo =
    ConstraintInfo(List(relType), List(), None, Some(label), Some(EndpointType.START))

  private val relTargetConstraintInfo =
    ConstraintInfo(List(relType), List(), None, Some(label), Some(EndpointType.END))

  protected lazy val kernelQueryContext: org.neo4j.internal.kernel.api.QueryContext =
    mock[org.neo4j.internal.kernel.api.QueryContext]

  override def beforeEach(): Unit = {
    super.beforeEach()
    when(ctx.transactionalContext).thenReturn(txContext)
    when(txContext.kernelQueryContext).thenReturn(kernelQueryContext)

    // Defaults:
    when(ctx.getConfig).thenReturn(Config.defaults())
    when(ctx.getAllIndexes()).thenReturn(Map(
      nodeUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeUniquenessConstraintDescriptor)),
        List(label),
        List(prop)
      )
    ))
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    name: Option[String],
    id: Option[Long] = None,
    constraintType: Option[String] = None,
    entityType: Option[String] = None,
    labelsOrTypes: Option[List[String]] = None,
    properties: Option[List[String]] = None,
    index: Option[String] = None,
    propType: Option[String] = None,
    options: Option[AnyValue] = None,
    createStatement: Option[String] = None
  ): Unit = {
    id.foreach(expected => resultMap(ShowConstraintsClause.idColumn) should be(Values.longValue(expected)))
    name.foreach(expected => resultMap(ShowConstraintsClause.nameColumn) should be(Values.stringValue(expected)))
    constraintType.foreach(expected =>
      resultMap(ShowConstraintsClause.typeColumn) should be(Values.stringValue(expected))
    )
    entityType.foreach(expected =>
      resultMap(ShowConstraintsClause.entityTypeColumn) should be(Values.stringValue(expected))
    )
    labelsOrTypes.foreach(expected =>
      resultMap(ShowConstraintsClause.labelsOrTypesColumn) should be(
        VirtualValues.list(expected.map(Values.stringValue): _*)
      )
    )
    properties.foreach { maybeExpected =>
      val expected =
        if (maybeExpected == null) Values.NO_VALUE
        else VirtualValues.list(maybeExpected.map(Values.stringValue): _*)
      resultMap(ShowConstraintsClause.propertiesColumn) should be(expected)
    }
    index.foreach(expected =>
      resultMap(ShowConstraintsClause.ownedIndexColumn) should be(Values.stringOrNoValue(expected))
    )
    propType.foreach(expected =>
      resultMap(ShowConstraintsClause.propertyTypeColumn) should be(Values.stringOrNoValue(expected))
    )
    options.foreach(expected => resultMap(ShowConstraintsClause.optionsColumn) should be(expected))
    createStatement.foreach(expected =>
      resultMap(ShowConstraintsClause.createStatementColumn) should be(Values.stringOrNoValue(expected))
    )
  }

  private def setupAllConstraints(): Unit = {
    // Override returned indexes:
    when(ctx.getAllIndexes()).thenReturn(Map(
      nodeUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeUniquenessConstraintDescriptor)),
        List(label),
        List(prop)
      ),
      relUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(relUniquenessConstraintDescriptor)),
        List(relType),
        List(prop)
      ),
      nodeKeyIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeKeyConstraintDescriptor)),
        List(label),
        List(prop)
      ),
      relKeyIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(relKeyConstraintDescriptor)),
        List(relType),
        List(prop)
      )
    ))

    // Set-up which constraints the context returns:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relUniquenessConstraintDescriptor -> relUniquenessConstraintInfo,
      nodeKeyConstraintDescriptor -> nodeKeyConstraintInfo,
      relKeyConstraintDescriptor -> relKeyConstraintInfo,
      nodeExistConstraintDescriptor -> nodeExistConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo,
      nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
      relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo,
      nodeLabelExistenceConstraintDescriptor -> constraintInfo,
      relSourceConstraintDescriptor -> relSourceConstraintInfo,
      relTargetConstraintDescriptor -> relTargetConstraintInfo
    ))
  }

  // Tests

  test("show constraints should give back correct default values") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo
    ))

    // When
    val showConstraints =
      ShowConstraintsCommand(AllConstraints, defaultColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      id = 1,
      constraintType = "NODE_PROPERTY_UNIQUENESS",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = "constraint0",
      propType = Some(null)
    )
    checkResult(
      result.last,
      name = "constraint1",
      id = 2,
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = Some(null)
    )
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List(
        ShowConstraintsClause.optionsColumn,
        ShowConstraintsClause.createStatementColumn
      )
    })
  }

  test("show constraints should give back correct full values") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      id = 1,
      constraintType = "NODE_PROPERTY_UNIQUENESS",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement = s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
    checkResult(
      result.last,
      name = "constraint1",
      id = 2,
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show constraints should return the constraints sorted on name") {
    // Set-up which constraints to return, ordered descending by name:
    when(ctx.getAllConstraints()).thenReturn(Map(
      relExistConstraintDescriptor -> relExistConstraintInfo,
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo
    ))

    // When
    val showConstraints =
      ShowConstraintsCommand(AllConstraints, defaultColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(result.head, name = "constraint0")
    checkResult(result.last, name = "constraint1")
  }

  test("show constraints should show all constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 11
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "NODE_PROPERTY_UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
    checkResult(
      result(1),
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result(2),
      name = "constraint10",
      id = 10,
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = Some(null),
      propType = "BOOLEAN",
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
    checkResult(
      result(3),
      name = "constraint11",
      id = 11,
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = "STRING",
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
    checkResult(
      result(4),
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(5),
      name = "constraint13",
      id = 13,
      constraintType = "RELATIONSHIP_SOURCE_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(6),
      name = "constraint14",
      id = 14,
      constraintType = "RELATIONSHIP_TARGET_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(7),
      name = "constraint2",
      constraintType = "RELATIONSHIP_PROPERTY_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )
    checkResult(
      result(8),
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS KEY"
    )
    checkResult(
      result(9),
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS KEY"
    )
    checkResult(
      result(10),
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show property uniqueness constraints should show property uniqueness constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(UniqueConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "NODE_PROPERTY_UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
    checkResult(
      result.last,
      name = "constraint2",
      constraintType = "RELATIONSHIP_PROPERTY_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )
  }

  test("show node property uniqueness constraints should show node property uniqueness constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(NodeUniqueConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "NODE_PROPERTY_UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
  }

  test(
    "show relationship property uniqueness constraints should show relationship property uniqueness constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(RelUniqueConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint2",
      constraintType = "RELATIONSHIP_PROPERTY_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )
  }

  test("show key constraints should show key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(KeyConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS KEY"
    )
    checkResult(
      result.last,
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS KEY"
    )
  }

  test("show node key constraints should show node key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(NodeKeyConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS KEY"
    )
  }

  test("show relationship key constraints should show relationship key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(RelKeyConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS KEY"
    )
  }

  test("show property existence constraints should show property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(PropExistsConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result.last,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show node property existence constraints should show node property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(NodePropExistsConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test(
    "show relationship property existence constraints should show relationship property existence constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(RelPropExistsConstraints.cypher25, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show existence constraints should show property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(AllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result(1),
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(2),
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show node existence constraints should show node property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(NodeAllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result.last,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test(
    "show relationship existence constraints should show relationship property existence constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(RelAllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show property type constraints should show property type constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(PropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint10",
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      index = Some(null),
      propType = "BOOLEAN",
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
    checkResult(
      result.last,
      name = "constraint11",
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = "STRING",
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
  }

  test("show node property type constraints should show node property type constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(NodePropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint10",
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      index = Some(null),
      propType = "BOOLEAN",
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
  }

  test(
    "show relationship property type constraints should show relationship property type constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints =
      ShowConstraintsCommand(RelPropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint11",
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = "STRING",
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
  }

  private val allowedSingleTypes = Seq(
    ("BOOLEAN", SchemaValueType.BOOLEAN),
    ("STRING", SchemaValueType.STRING),
    ("INTEGER", SchemaValueType.INTEGER),
    ("FLOAT", SchemaValueType.FLOAT),
    ("DATE", SchemaValueType.DATE),
    ("LOCAL TIME", SchemaValueType.LOCAL_TIME),
    ("ZONED TIME", SchemaValueType.ZONED_TIME),
    ("LOCAL DATETIME", SchemaValueType.LOCAL_DATETIME),
    ("ZONED DATETIME", SchemaValueType.ZONED_DATETIME),
    ("DURATION", SchemaValueType.DURATION),
    ("POINT", SchemaValueType.POINT),
    ("LIST<BOOLEAN NOT NULL>", SchemaValueType.LIST_BOOLEAN),
    ("LIST<STRING NOT NULL>", SchemaValueType.LIST_STRING),
    ("LIST<INTEGER NOT NULL>", SchemaValueType.LIST_INTEGER),
    ("LIST<FLOAT NOT NULL>", SchemaValueType.LIST_FLOAT),
    ("LIST<DATE NOT NULL>", SchemaValueType.LIST_DATE),
    ("LIST<LOCAL TIME NOT NULL>", SchemaValueType.LIST_LOCAL_TIME),
    ("LIST<ZONED TIME NOT NULL>", SchemaValueType.LIST_ZONED_TIME),
    ("LIST<LOCAL DATETIME NOT NULL>", SchemaValueType.LIST_LOCAL_DATETIME),
    ("LIST<ZONED DATETIME NOT NULL>", SchemaValueType.LIST_ZONED_DATETIME),
    ("LIST<DURATION NOT NULL>", SchemaValueType.LIST_DURATION),
    ("LIST<POINT NOT NULL>", SchemaValueType.LIST_POINT)
  )

  allowedSingleTypes.zipWithIndex.foreach { case ((propTypeString, propType), currentIndex) =>
    test(s"show normalized property type representation: $propType") {
      // Given
      val nodePropTypeConstraintDescriptor =
        ConstraintDescriptorFactory.typeForSchema(labelDescriptor, PropertyTypeSet.of(propType), false)
          .withName("constraint0")
          .withId(0)
      val relPropTypeConstraintDescriptor =
        ConstraintDescriptorFactory.typeForSchema(relTypeDescriptor, PropertyTypeSet.of(propType), false)
          .withName("constraint1")
          .withId(1)
      when(ctx.getAllConstraints()).thenReturn(Map(
        nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
        relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo
      ))

      // When
      val showConstraints =
        ShowConstraintsCommand(PropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
      val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

      // Then
      result should have size 2
      checkResult(
        result.head,
        name = "constraint0",
        propType = propTypeString,
        createStatement =
          s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: $propTypeString"
      )
      checkResult(
        result.last,
        name = "constraint1",
        propType = propTypeString,
        createStatement =
          s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: $propTypeString"
      )
    }

    // Union of 2 types
    // allowedSingleTypes is ordered so the normalized form should always be `propType | propType2`
    // if we only check the types after the current one
    allowedSingleTypes.drop(currentIndex + 1).foreach { case (propTypeString2, propType2) =>
      test(s"show normalized property type representation: $propType | $propType2") {
        // Given
        val nodePropTypeConstraintDescriptor =
          ConstraintDescriptorFactory.typeForSchema(labelDescriptor, PropertyTypeSet.of(propType, propType2), false)
            .withName("constraint0")
            .withId(0)
        val relPropTypeConstraintDescriptor =
          ConstraintDescriptorFactory.typeForSchema(relTypeDescriptor, PropertyTypeSet.of(propType2, propType), false)
            .withName("constraint1")
            .withId(1)
        when(ctx.getAllConstraints()).thenReturn(Map(
          nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
          relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo
        ))

        // When
        val showConstraints =
          ShowConstraintsCommand(PropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
        val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

        // Then
        result should have size 2
        checkResult(
          result.head,
          name = "constraint0",
          propType = s"$propTypeString | $propTypeString2",
          createStatement =
            s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: $propTypeString | $propTypeString2"
        )
        checkResult(
          result.last,
          name = "constraint1",
          propType = s"$propTypeString | $propTypeString2",
          createStatement =
            s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: $propTypeString | $propTypeString2"
        )
      }
    }
  }

  test("show normalized property type representation for larger unions") {
    // Given
    val nodePropTypeConstraintDescriptor =
      ConstraintDescriptorFactory.typeForSchema(
        labelDescriptor,
        PropertyTypeSet.of(
          SchemaValueType.INTEGER,
          SchemaValueType.LIST_ZONED_TIME,
          SchemaValueType.LIST_DURATION,
          SchemaValueType.LOCAL_TIME,
          SchemaValueType.BOOLEAN
        ),
        false
      )
        .withName("constraint0")
        .withId(0)
    val relPropTypeConstraintDescriptor =
      ConstraintDescriptorFactory.typeForSchema(
        relTypeDescriptor,
        PropertyTypeSet.of(
          SchemaValueType.FLOAT,
          SchemaValueType.LIST_INTEGER,
          SchemaValueType.STRING,
          SchemaValueType.LIST_BOOLEAN,
          SchemaValueType.BOOLEAN,
          SchemaValueType.FLOAT,
          SchemaValueType.STRING
        ),
        false
      )
        .withName("constraint1")
        .withId(1)
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
      relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo
    ))

    // When
    val showConstraints =
      ShowConstraintsCommand(PropTypeConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      propType = "BOOLEAN | INTEGER | LOCAL TIME | LIST<ZONED TIME NOT NULL> | LIST<DURATION NOT NULL>",
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) " +
          s"REQUIRE (n.`$prop`) IS :: BOOLEAN | INTEGER | LOCAL TIME | LIST<ZONED TIME NOT NULL> | LIST<DURATION NOT NULL>"
    )
    checkResult(
      result.last,
      name = "constraint1",
      propType = "BOOLEAN | STRING | FLOAT | LIST<BOOLEAN NOT NULL> | LIST<INTEGER NOT NULL>",
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() " +
          s"REQUIRE (r.`$prop`) IS :: BOOLEAN | STRING | FLOAT | LIST<BOOLEAN NOT NULL> | LIST<INTEGER NOT NULL>"
    )
  }

  test("show constraints should rename columns renamed in YIELD") {
    // Given: YIELD name AS constraint, labelsOrTypes, createStatement AS create, type
    val yieldColumns: List[CommandResultItem] = List(
      CommandResultItem(
        ShowConstraintsClause.nameColumn,
        varFor("constraint")
      )(InputPosition.NONE),
      CommandResultItem(
        ShowConstraintsClause.labelsOrTypesColumn,
        varFor(ShowConstraintsClause.labelsOrTypesColumn)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowConstraintsClause.createStatementColumn,
        varFor("create")
      )(InputPosition.NONE),
      CommandResultItem(
        ShowConstraintsClause.typeColumn,
        varFor(ShowConstraintsClause.typeColumn)
      )(InputPosition.NONE)
    )

    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, yieldColumns, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    result.head should be(Map(
      "constraint" -> Values.stringValue("constraint0"),
      ShowConstraintsClause.labelsOrTypesColumn -> VirtualValues.list(Values.stringValue(label)),
      "create" -> Values.stringValue(
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
      ),
      ShowConstraintsClause.typeColumn -> Values.stringValue("NODE_PROPERTY_UNIQUENESS")
    ))
  }

  test("show all constraints with Cypher 5") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher5)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 11
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
    checkResult(
      result(1),
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result(2),
      name = "constraint10",
      id = 10,
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = Some(null),
      propType = "BOOLEAN",
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
    checkResult(
      result(3),
      name = "constraint11",
      id = 11,
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = "STRING",
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
    checkResult(
      result(4),
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(5),
      name = "constraint13",
      id = 13,
      constraintType = "RELATIONSHIP_SOURCE_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(6),
      name = "constraint14",
      id = 14,
      constraintType = "RELATIONSHIP_TARGET_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(7),
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )
    checkResult(
      result(8),
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NODE KEY"
    )
    checkResult(
      result(9),
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS RELATIONSHIP KEY"
    )
    checkResult(
      result(10),
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show unique constraints with Cypher 5") {
    // Given
    setupAllConstraints()

    // When: all unique
    val showConstraintsAll =
      ShowConstraintsCommand(UniqueConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultAll = showConstraintsAll.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultAll should have size 2
    checkResult(
      resultAll.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )
    checkResult(
      resultAll.last,
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )

    // When: node unique
    val showConstraintsNode =
      ShowConstraintsCommand(NodeUniqueConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultNode = showConstraintsNode.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultNode should have size 1
    checkResult(
      resultNode.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE"
    )

    // When: rel unique
    val showConstraintsRel =
      ShowConstraintsCommand(RelUniqueConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultRel = showConstraintsRel.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultRel should have size 1
    checkResult(
      resultRel.head,
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE"
    )
  }

  test("show property existence constraints with Cypher 5") {
    // Given
    setupAllConstraints()

    // When: all exists
    val showConstraintsAll =
      ShowConstraintsCommand(PropExistsConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultAll = showConstraintsAll.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultAll should have size 2
    checkResult(
      resultAll.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      resultAll.last,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )

    // When: node exists
    val showConstraintsNode =
      ShowConstraintsCommand(NodePropExistsConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultNode = showConstraintsNode.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultNode should have size 1
    checkResult(
      resultNode.head,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )

    // When: rel exists
    val showConstraintsRel =
      ShowConstraintsCommand(RelPropExistsConstraints.cypher5, allColumns, List.empty, CypherVersion.Cypher5)
    val resultRel = showConstraintsRel.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultRel should have size 1
    checkResult(
      resultRel.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show existence constraints with Cypher 5") {
    // Given
    setupAllConstraints()

    // When: all exists
    val showConstraintsAll =
      ShowConstraintsCommand(AllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher5)
    val resultAll = showConstraintsAll.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultAll should have size 3
    checkResult(
      resultAll.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      resultAll(1),
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      resultAll(2),
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )

    // When: node exists
    val showConstraintsNode =
      ShowConstraintsCommand(NodeAllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher5)
    val resultNode = showConstraintsNode.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultNode should have size 2
    checkResult(
      resultNode.head,
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      resultNode.last,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )

    // When: rel exists
    val showConstraintsRel =
      ShowConstraintsCommand(RelAllExistsConstraints, allColumns, List.empty, CypherVersion.Cypher5)
    val resultRel = showConstraintsRel.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultRel should have size 1
    checkResult(
      resultRel.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show key constraints with Cypher 5") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(KeyConstraints, allColumns, List.empty, CypherVersion.Cypher5)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NODE KEY"
    )
    checkResult(
      result.last,
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      propType = Some(null),
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS RELATIONSHIP KEY"
    )
  }

  test("show graph dependent constraints") {
    // Given
    val nodePropTypeConstraintDescriptor =
      ConstraintDescriptorFactory.typeForSchema(labelDescriptor, PropertyTypeSet.of(SchemaValueType.BOOLEAN), true)
        .withName("constraint0")
        .withId(0)
    val relPropTypeConstraintDescriptor =
      ConstraintDescriptorFactory.typeForSchema(relTypeDescriptor, PropertyTypeSet.of(SchemaValueType.STRING), true)
        .withName("constraint1")
        .withId(1)
    val nodeExistConstraintDescriptor =
      ConstraintDescriptorFactory.existsForSchema(labelDescriptor, true)
        .withName("constraint2")
        .withId(2)
    val relExistConstraintDescriptor =
      ConstraintDescriptorFactory.existsForSchema(relTypeDescriptor, true)
        .withName("constraint3")
        .withId(3)
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
      relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo,
      nodeExistConstraintDescriptor -> nodeExistConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 4
    checkResult(
      result.head,
      name = "constraint0",
      id = 0,
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = Some(null),
      propType = "BOOLEAN",
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(1),
      name = "constraint1",
      id = 1,
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = "STRING",
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(2),
      name = "constraint2",
      id = 2,
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result(3),
      name = "constraint3",
      id = 3,
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
  }

  test("show relationship source and target label constraints") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      relSourceConstraintDescriptor -> relSourceConstraintInfo,
      relTargetConstraintDescriptor -> relTargetConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint13",
      id = 13,
      constraintType = "RELATIONSHIP_SOURCE_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
    checkResult(
      result.last,
      name = "constraint14",
      id = 14,
      constraintType = "RELATIONSHIP_TARGET_LABEL",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )
  }

  test("show node label existence constraints") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeLabelExistenceConstraintDescriptor -> constraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, allColumns, List.empty, CypherVersion.Cypher25)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint12",
      id = 12,
      constraintType = "NODE_LABEL_EXISTENCE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = Some(null),
      index = Some(null),
      propType = Some(null),
      options = Values.NO_VALUE,
      createStatement = Some(null)
    )

  }
}
