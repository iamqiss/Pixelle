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
package org.neo4j.dbms.systemgraph;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedCatalogEntry;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public class CommunityTopologyGraphDbmsModel implements TopologyGraphDbmsModel {
    protected final Transaction tx;

    public CommunityTopologyGraphDbmsModel(Transaction tx) {
        this.tx = tx;
    }

    public Map<NamedDatabaseId, TopologyGraphDbmsModel.DatabaseAccess> getAllDatabaseAccess() {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
            return nodes.stream()
                    .collect(Collectors.toMap(
                            CommunityTopologyGraphDbmsModelUtil::getDatabaseId,
                            CommunityTopologyGraphDbmsModelUtil::getDatabaseAccess));
        }
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByAlias(String databaseName) {
        return CommunityTopologyGraphDbmsModelUtil.getDatabaseIdByAlias(tx, databaseName)
                .or(() ->
                        CommunityTopologyGraphDbmsModelUtil.getDatabaseIdBy(tx, DATABASE_NAME_PROPERTY, databaseName));
    }

    @Override
    public Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid) {
        return CommunityTopologyGraphDbmsModelUtil.getDatabaseIdBy(tx, DATABASE_UUID_PROPERTY, uuid.toString());
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences() {
        var primaryRefs = CommunityTopologyGraphDbmsModelUtil.getAllPrimaryStandardDatabaseReferencesInRoot(tx);
        var internalAliasRefs = getAllInternalDatabaseReferencesInRoot();
        var externalRefs = getAllExternalDatabaseReferencesInRoot();
        var compositeRefs = getAllCompositeDatabaseReferencesInRoot();
        var spdEntityShardRefs = getAllSPDEntityShardReferencesInRoot();
        var spdPropertyShardRefs = getAllSPDPropertyShardReferencesInRoot();
        return Stream.of(
                        primaryRefs,
                        internalAliasRefs,
                        externalRefs,
                        compositeRefs,
                        spdEntityShardRefs,
                        spdPropertyShardRefs)
                .flatMap(s -> s)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferences() {
        var primaryRefs = CommunityTopologyGraphDbmsModelUtil.getAllPrimaryStandardDatabaseReferencesInRoot(tx);
        var internalAliasRefs = getAllInternalDatabaseReferencesInRoot();
        var spdEntityShardRefs = getAllSPDEntityShardReferencesInRoot();
        var spdPropertyShardRefs = getAllSPDPropertyShardReferencesInRoot();
        return Stream.of(primaryRefs, internalAliasRefs, spdEntityShardRefs, spdPropertyShardRefs)
                .flatMap(s -> s)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<DatabaseReferenceImpl.External> getAllExternalDatabaseReferences() {
        return getAllExternalDatabaseReferencesInRoot().collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferences() {
        return getAllCompositeDatabaseReferencesInRoot().collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Optional<DatabaseReference> getDatabaseRefByAlias(String databaseName) {
        var normalizedDatabaseName = new NormalizedDatabaseName(databaseName).name();
        // A uniqueness constraint at the Cypher level should prevent two references from ever having the same name, but
        // in case they do, we simply prefer the internal reference.
        return Optional.<DatabaseReference>empty()
                .or(() -> getCompositeDatabaseReferenceInRoot(normalizedDatabaseName))
                .or(() -> getSPDEntityShardReferenceInRoot(normalizedDatabaseName))
                .or(() -> getSPDPropertyShardReferenceInRoot(normalizedDatabaseName))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReference(tx, normalizedDatabaseName))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReference(tx, normalizedDatabaseName));
    }

    @Override
    public Optional<DatabaseReference> getDatabaseRefByAlias(NormalizedCatalogEntry catalogEntry) {
        if (catalogEntry.compositeDb().isPresent()) {
            return resolveConstituent(catalogEntry.compositeDb().get(), catalogEntry.databaseAlias());
        } else {
            return resolveRootReference(catalogEntry.databaseAlias());
        }
    }

    private Optional<DatabaseReference> resolveConstituent(String composite, String constituent) {
        return Optional.<DatabaseReference>empty()
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReference(tx, composite, constituent))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReference(tx, composite, constituent));
    }

    private Optional<DatabaseReference> resolveRootReference(String normalizedDatabaseAlias) {
        return Optional.<DatabaseReference>empty()
                .or(() -> getCompositeDatabaseReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> getSPDEntityShardReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> getSPDPropertyShardReferenceInRoot(normalizedDatabaseAlias))
                .or(() -> CommunityTopologyGraphDbmsModelUtil.getInternalDatabaseReference(tx, normalizedDatabaseAlias))
                .or(() ->
                        CommunityTopologyGraphDbmsModelUtil.getExternalDatabaseReference(tx, normalizedDatabaseAlias));
    }

    @Override
    public Optional<DriverSettings> getDriverSettings(String databaseName, String namespace) {
        databaseName = new NormalizedDatabaseName(databaseName).name();
        namespace = new NormalizedDatabaseName(namespace).name();
        return tx.findNodes(REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName, NAMESPACE_PROPERTY, namespace).stream()
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getDriverSettings);
    }

    @Override
    public Optional<Map<String, Object>> getAliasProperties(String databaseName, String namespace) {
        databaseName = new NormalizedDatabaseName(databaseName).name();
        namespace = new NormalizedDatabaseName(namespace).name();
        return tx.findNodes(DATABASE_NAME_LABEL, NAME_PROPERTY, databaseName, NAMESPACE_PROPERTY, namespace).stream()
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getAliasProperties);
    }

    @Override
    public Optional<ExternalDatabaseCredentials> getExternalDatabaseCredentials(
            DatabaseReferenceImpl.External databaseReference) {
        String databaseName = databaseReference.alias().name();
        String namespace =
                databaseReference.namespace().map(NormalizedDatabaseName::name).orElse(DEFAULT_NAMESPACE);
        return tx.findNodes(REMOTE_DATABASE_LABEL, NAME_PROPERTY, databaseName, NAMESPACE_PROPERTY, namespace).stream()
                .findFirst()
                .flatMap(CommunityTopologyGraphDbmsModelUtil::getDatabaseCredentials);
    }

    private Stream<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferencesInRoot() {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .flatMap(db -> createCompositeReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.Composite> getCompositeDatabaseReferenceInRoot(String databaseName) {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE, databaseName)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.hasLabel(COMPOSITE_DATABASE_LABEL))
                        .flatMap(db -> createCompositeReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Optional<DatabaseReferenceImpl.Composite> createCompositeReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var aliasName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE_NAME, alias);
            var databaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);
            var compositeName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE, db);
            var components = getAllDatabaseReferencesInComposite(compositeName);
            return Optional.of(new DatabaseReferenceImpl.Composite(aliasName, databaseId, components));
        });
    }

    private Stream<DatabaseReferenceImpl.SPD> getAllSPDEntityShardReferencesInRoot() {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.getDegree(HAS_SHARD, Direction.OUTGOING) > 0)
                        .flatMap(db -> createSPDEntityShardReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.SPD> getSPDEntityShardReferenceInRoot(String databaseName) {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE, databaseName)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.getDegree(HAS_SHARD, Direction.OUTGOING) > 0)
                        .flatMap(db -> createSPDEntityShardReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Stream<DatabaseReferenceImpl.SPDShard> getAllSPDPropertyShardReferencesInRoot() {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.getDegree(HAS_SHARD, Direction.INCOMING) > 0)
                        .flatMap(db -> createSPDPropertyShardReference(alias, db))
                        .stream());
    }

    private Optional<DatabaseReferenceImpl.SPDShard> getSPDPropertyShardReferenceInRoot(String databaseName) {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, DEFAULT_NAMESPACE, databaseName)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(db -> db.getDegree(HAS_SHARD, Direction.INCOMING) > 0)
                        .flatMap(db -> createSPDPropertyShardReference(alias, db))
                        .stream())
                .findFirst();
    }

    private Optional<DatabaseReferenceImpl.SPD> createSPDEntityShardReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.ignoreConcurrentDeletes(() -> {
            var aliasName = CommunityTopologyGraphDbmsModelUtil.getNameProperty(DATABASE_NAME, alias);
            var databaseId = CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db);
            var shards = StreamSupport.stream(
                            db.getRelationships(Direction.OUTGOING, TopologyGraphDbmsModel.HAS_SHARD)
                                    .spliterator(),
                            false)
                    .flatMap(rel ->
                            getDatabaseRefByAlias((String) rel.getEndNode().getProperty(DATABASE_NAME_PROPERTY))
                                    .map(ref -> Pair.of((int) rel.getProperty(HAS_SHARD_INDEX_PROPERTY), ref))
                                    .stream())
                    .collect(Collectors.toMap(Pair::first, Pair::other));
            return Optional.of(new DatabaseReferenceImpl.SPD(aliasName, databaseId, shards));
        });
    }

    private static Optional<DatabaseReferenceImpl.SPDShard> createSPDPropertyShardReference(Node alias, Node db) {
        return CommunityTopologyGraphDbmsModelUtil.createInternalReference(
                        alias, CommunityTopologyGraphDbmsModelUtil.getDatabaseId(db))
                .flatMap(internal -> CommunityTopologyGraphDbmsModelUtil.readOwningDatabase(db)
                        .map(internal::asShard));
    }

    private Stream<Node> getAliasNodesInNamespace(Label label, String namespace) {
        return tx.findNodes(label, NAMESPACE_PROPERTY, namespace).stream();
    }

    private Stream<Node> getAliasNodesInNamespace(Label label, String namespace, String databaseName) {
        return tx.findNodes(label, NAMESPACE_PROPERTY, namespace, NAME_PROPERTY, databaseName).stream();
    }

    private Set<DatabaseReference> getAllDatabaseReferencesInComposite(NormalizedDatabaseName compositeName) {
        var internalRefs = getAllInternalDatabaseReferencesInNamespace(compositeName.name());
        var externalRefs = getAllExternalDatabaseReferencesInNamespace(compositeName.name());

        return Stream.concat(internalRefs, externalRefs).collect(Collectors.toUnmodifiableSet());
    }

    private Stream<DatabaseReferenceImpl.External> getAllExternalDatabaseReferencesInRoot() {
        return getAllExternalDatabaseReferencesInNamespace(DEFAULT_NAMESPACE);
    }

    private Stream<DatabaseReferenceImpl.External> getAllExternalDatabaseReferencesInNamespace(String namespace) {
        return getAliasNodesInNamespace(REMOTE_DATABASE_LABEL, namespace)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.createExternalReference(alias).stream());
    }

    private Stream<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferencesInRoot() {
        return getAllInternalDatabaseReferencesInNamespace(DEFAULT_NAMESPACE);
    }

    private Stream<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferencesInNamespace(String namespace) {
        return getAliasNodesInNamespace(DATABASE_NAME_LABEL, namespace)
                .flatMap(alias -> CommunityTopologyGraphDbmsModelUtil.getTargetedDatabaseNode(alias)
                        .filter(node -> !node.hasProperty(DATABASE_VIRTUAL_PROPERTY))
                        .filter(node -> node.getDegree(HAS_SHARD, Direction.OUTGOING) == 0
                                && node.getDegree(HAS_SHARD, Direction.INCOMING) == 0)
                        .map(CommunityTopologyGraphDbmsModelUtil::getDatabaseId)
                        .flatMap(db -> CommunityTopologyGraphDbmsModelUtil.createInternalReference(alias, db))
                        .stream());
    }
}
