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
package org.neo4j.procedure.builtin;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.procedure.Mode.READ;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UnsupportedDatabaseTypes;
import org.neo4j.util.FeatureToggles;

/**
 * Procedures for querying the Fulltext indexes.
 */
@SuppressWarnings("WeakerAccess")
public class FulltextProcedures {
    private static final long INDEX_ONLINE_QUERY_TIMEOUT_SECONDS =
            FeatureToggles.getInteger(FulltextProcedures.class, "INDEX_ONLINE_QUERY_TIMEOUT_SECONDS", 30);

    @Context
    public KernelTransaction tx;

    @Context
    public Transaction transaction;

    @Context
    public GraphDatabaseAPI db;

    @Context
    public DependencyResolver resolver;

    @Context
    public FulltextAdapter accessor;

    @Context
    public ProcedureCallContext callContext;

    @SystemProcedure
    @Description("List the available analyzers that the full-text indexes can be configured with.")
    @Procedure(name = "db.index.fulltext.listAvailableAnalyzers", mode = READ)
    public Stream<AvailableAnalyzer> listAvailableAnalyzers() {
        return accessor.listAvailableAnalyzers().map(AvailableAnalyzer::new);
    }

    @SystemProcedure
    @Description(
            "Wait for the updates from recently committed transactions to be applied to any eventually-consistent full-text indexes.")
    @Procedure(name = "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", mode = READ)
    @UnsupportedDatabaseTypes(UnsupportedDatabaseTypes.DatabaseType.SPD)
    public void awaitRefresh() {
        if (callContext.isSystemDatabase()) {
            return;
        }

        accessor.awaitRefresh();
        resolver.resolveDependency(IndexingService.class).awaitFulltextIndexRefresh();
    }

    @SystemProcedure
    @Description(
            """
            Query the given full-text index. Returns the matching nodes and their Lucene query score, ordered by score.
            Valid _key: value_ pairs for the `options` map are:

            * 'skip' -- to skip the top N results.
            * 'limit' -- to limit the number of results returned.
            * 'analyzer' -- to use the specified analyzer as a search analyzer for this query.

            The `options` map and any of the keys are optional.
            An example of the `options` map: `{skip: 30, limit: 10, analyzer: 'whitespace'}`
            """)
    @Procedure(name = "db.index.fulltext.queryNodes", mode = READ)
    public Stream<NodeOutput> queryFulltextForNodes(
            @Name(value = "indexName", description = "The name of the full-text index.") String name,
            @Name(value = "queryString", description = "The string to find approximate matches for.") String query,
            @Name(
                            value = "options",
                            defaultValue = "{}",
                            description = "{skip :: INTEGER, limit :: INTEGER, analyzer :: STRING}")
                    Map<String, Object> options)
            throws Exception {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        IndexDescriptor indexReference = getValidIndex(name);
        awaitOnline(indexReference);
        EntityType entityType = indexReference.schema().entityType();
        if (entityType != NODE) {
            throw new IllegalArgumentException("The '" + name + "' index (" + indexReference + ") is an index on "
                    + entityType + ", so it cannot be queried for nodes.");
        }
        NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker());
        IndexReadSession indexSession = tx.dataRead().indexReadSession(indexReference);
        IndexQueryConstraints constraints = queryConstraints(options);
        tx.dataRead()
                .nodeIndexSeek(
                        tx.queryContext(),
                        indexSession,
                        cursor,
                        constraints,
                        PropertyIndexQuery.fulltextSearch(query, queryAnalyzer(options)));

        Spliterator<NodeOutput> spliterator = new SpliteratorAdaptor<>() {
            @Override
            public boolean tryAdvance(Consumer<? super NodeOutput> action) {
                while (cursor.next()) {
                    long nodeReference = cursor.nodeReference();
                    float score = cursor.score();
                    NodeOutput nodeOutput = NodeOutput.forExistingEntityOrNull(transaction, nodeReference, score);
                    if (nodeOutput != null) {
                        action.accept(nodeOutput);
                        return true;
                    }
                }
                cursor.close();
                return false;
            }
        };
        Stream<NodeOutput> stream = StreamSupport.stream(spliterator, false);
        return stream.onClose(cursor::close);
    }

    protected static IndexQueryConstraints queryConstraints(Map<String, Object> options) {
        IndexQueryConstraints constraints = unconstrained();
        Object skip;
        if ((skip = options.get("skip")) != null && skip instanceof Number) {
            constraints = constraints.skip(((Number) skip).longValue());
        }
        Object limit;
        if ((limit = options.get("limit")) != null && limit instanceof Number) {
            constraints = constraints.limit(((Number) limit).longValue());
        }
        return constraints;
    }

    protected static String queryAnalyzer(Map<String, Object> options) {
        Object analyzer;
        if ((analyzer = options.get("analyzer")) != null && analyzer instanceof String) {
            return (String) analyzer;
        }
        return null;
    }

    @SystemProcedure
    @Description(
            """
            Query the given full-text index. Returns the matching relationships and their Lucene query score, ordered by score.
            Valid _key: value_ pairs for the `options` map are:

            * 'skip' -- to skip the top N results.
            * 'limit' -- to limit the number of results returned.
            * 'analyzer' -- to use the specified analyzer as a search analyzer for this query.

            The `options` map and any of the keys are optional.
            An example of the `options` map: `{skip: 30, limit: 10, analyzer: 'whitespace'}`
            """)
    @Procedure(name = "db.index.fulltext.queryRelationships", mode = READ)
    public Stream<RelationshipOutput> queryFulltextForRelationships(
            @Name(value = "indexName", description = "The name of the full-text index.") String name,
            @Name(value = "queryString", description = "The string to find approximate matches for.") String query,
            @Name(
                            value = "options",
                            defaultValue = "{}",
                            description = "{skip :: INTEGER, limit :: INTEGER, analyzer :: STRING}")
                    Map<String, Object> options)
            throws Exception {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        IndexDescriptor indexReference = getValidIndex(name);
        awaitOnline(indexReference);
        EntityType entityType = indexReference.schema().entityType();
        if (entityType != RELATIONSHIP) {
            throw new IllegalArgumentException("The '" + name + "' index (" + indexReference + ") is an index on "
                    + entityType + ", so it cannot be queried for relationships.");
        }
        RelationshipValueIndexCursor cursor =
                tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker());
        IndexReadSession indexReadSession = tx.dataRead().indexReadSession(indexReference);
        IndexQueryConstraints constraints = queryConstraints(options);
        tx.dataRead()
                .relationshipIndexSeek(
                        tx.queryContext(),
                        indexReadSession,
                        cursor,
                        constraints,
                        PropertyIndexQuery.fulltextSearch(query, queryAnalyzer(options)));

        Spliterator<RelationshipOutput> spliterator = new SpliteratorAdaptor<>() {
            @Override
            public boolean tryAdvance(Consumer<? super RelationshipOutput> action) {
                while (cursor.next()) {
                    long relationshipReference = cursor.relationshipReference();
                    float score = cursor.score();
                    RelationshipOutput relationshipOutput =
                            RelationshipOutput.forExistingEntityOrNull(transaction, relationshipReference, score);
                    if (relationshipOutput != null) {
                        action.accept(relationshipOutput);
                        return true;
                    }
                }
                cursor.close();
                return false;
            }
        };
        return StreamSupport.stream(spliterator, false).onClose(cursor::close);
    }

    private IndexDescriptor getValidIndex(@Name("indexName") String name) {
        IndexDescriptor indexReference = tx.schemaRead().indexGetForName(name);
        if (indexReference == IndexDescriptor.NO_INDEX || indexReference.getIndexType() != IndexType.FULLTEXT) {
            throw new IllegalArgumentException("There is no such fulltext schema index: " + name);
        }
        return indexReference;
    }

    private void awaitOnline(IndexDescriptor index) {
        // We do the isAdded check on the transaction state first, because indexGetState will grab a schema read-lock,
        // which can deadlock on the write-lock
        // held by the index populator. Also, if the index was created in this transaction, then we will never see it
        // come online in this transaction anyway.
        // Indexes don't come online until the transaction that creates them has committed.
        // It's expensive to check if an index is online on an SPD(since we need to check if it's online for all
        // shards),
        // we will therefor do that when we query the index on each shard instead.
        TxStateHolder txStateHolder = (TxStateHolder) this.tx;
        if ((!txStateHolder.hasTxStateWithChanges()
                        || !txStateHolder
                                .txState()
                                .indexDiffSetsBySchema(index.schema())
                                .isAdded(index))
                && !tx.isSPDTransaction()) {
            // If the index was not created in this transaction, then wait for it to come online before querying.
            Schema schema = transaction.schema();
            schema.awaitIndexOnline(index.getName(), INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        // If the index was created in this transaction, then we skip this check entirely.
        // We will get an exception later, when we try to get an IndexReader, so this is fine.
    }

    private abstract static class SpliteratorAdaptor<T> implements Spliterator<T> {
        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED
                    | Spliterator.SORTED
                    | Spliterator.DISTINCT
                    | Spliterator.NONNULL
                    | Spliterator.IMMUTABLE;
        }

        @Override
        public Comparator<? super T> getComparator() {
            // Returning 'null' here means the items are sorted by their "natural" sort order.
            return null;
        }
    }

    public static final class NodeOutput implements Comparable<NodeOutput> {
        @Description("A node which contains a property similar to the query string.")
        public final Node node;

        @Description("The score measuring how similar the node property is to the query string.")
        public final double score;

        public NodeOutput(Node node, float score) {
            this.node = node;
            this.score = score;
        }

        public static NodeOutput forExistingEntityOrNull(Transaction transaction, long nodeId, float score) {
            try {
                return new NodeOutput(transaction.getNodeById(nodeId), score);
            } catch (NotFoundException ignore) {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }

        @Override
        public int compareTo(NodeOutput that) {
            return Double.compare(that.score, this.score);
        }

        @Override
        public String toString() {
            return "ScoredNode(" + node + ", score=" + score + ')';
        }
    }

    public static final class RelationshipOutput implements Comparable<RelationshipOutput> {
        @Description("A relationship which contains a property similar to the query string.")
        public final Relationship relationship;

        @Description("The score measuring how similar the relationship property is to the query string.")
        public final double score;

        public RelationshipOutput(Relationship relationship, float score) {
            this.relationship = relationship;
            this.score = score;
        }

        public static RelationshipOutput forExistingEntityOrNull(
                Transaction transaction, long relationshipId, float score) {
            try {
                return new RelationshipOutput(transaction.getRelationshipById(relationshipId), score);
            } catch (NotFoundException ignore) {
                // This relationship was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }

        @Override
        public int compareTo(RelationshipOutput that) {
            return Double.compare(that.score, this.score);
        }

        @Override
        public String toString() {
            return "ScoredRelationship(" + relationship + ", score=" + score + ')';
        }
    }

    public static final class AvailableAnalyzer {
        @Description("The name of the analyzer.")
        public final String analyzer;

        @Description("The  description of the analyzer.")
        public final String description;

        @Description("The stopwords used by the analyzer to tokenize strings.")
        public final List<String> stopwords;

        AvailableAnalyzer(AnalyzerProvider provider) {
            this.analyzer = provider.getName();
            this.description = provider.description();
            this.stopwords = provider.stopwords();
        }
    }
}
