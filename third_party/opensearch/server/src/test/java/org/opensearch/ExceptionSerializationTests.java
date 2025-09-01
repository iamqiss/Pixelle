/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.density.action.FailedNodeException;
import org.density.action.OriginalIndices;
import org.density.action.RoutingMissingException;
import org.density.action.TimestampParsingException;
import org.density.action.admin.indices.view.ViewAlreadyExistsException;
import org.density.action.admin.indices.view.ViewNotFoundException;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.ShardSearchFailure;
import org.density.action.support.replication.ReplicationOperation;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.IndexCreateBlockException;
import org.density.cluster.coordination.CoordinationStateRejectedException;
import org.density.cluster.coordination.NoClusterManagerBlockService;
import org.density.cluster.coordination.NodeHealthCheckFailureException;
import org.density.cluster.decommission.DecommissioningFailedException;
import org.density.cluster.decommission.NodeDecommissionedException;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.IllegalShardRoutingStateException;
import org.density.cluster.routing.NodeWeighedAwayException;
import org.density.cluster.routing.PreferenceBasedSearchNotAllowedException;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardRoutingState;
import org.density.cluster.routing.TestShardRouting;
import org.density.cluster.routing.UnsupportedWeightedRoutingStateException;
import org.density.cluster.service.ClusterManagerThrottlingException;
import org.density.common.UUIDs;
import org.density.common.breaker.ResponseLimitBreachedException;
import org.density.common.collect.Tuple;
import org.density.common.io.PathUtils;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.util.CancellableThreadsTests;
import org.density.common.util.set.Sets;
import org.density.core.common.ParsingException;
import org.density.core.common.Strings;
import org.density.core.common.breaker.CircuitBreaker;
import org.density.core.common.breaker.CircuitBreakingException;
import org.density.core.common.io.stream.NotSerializableExceptionWrapper;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.transport.TransportAddress;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.core.index.snapshots.IndexShardSnapshotException;
import org.density.core.index.snapshots.IndexShardSnapshotFailedException;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentLocation;
import org.density.crypto.CryptoRegistryException;
import org.density.env.ShardLockObtainFailedException;
import org.density.index.engine.IngestionEngineException;
import org.density.index.engine.RecoveryEngineException;
import org.density.index.query.QueryShardException;
import org.density.index.seqno.RetentionLeaseAlreadyExistsException;
import org.density.index.seqno.RetentionLeaseInvalidRetainingSeqNoException;
import org.density.index.seqno.RetentionLeaseNotFoundException;
import org.density.index.shard.IllegalIndexShardStateException;
import org.density.index.shard.IndexShardState;
import org.density.index.shard.PrimaryShardClosedException;
import org.density.index.shard.ShardNotInPrimaryModeException;
import org.density.indices.IndexTemplateMissingException;
import org.density.indices.InvalidIndexContextException;
import org.density.indices.InvalidIndexTemplateException;
import org.density.indices.recovery.PeerRecoveryNotFound;
import org.density.indices.recovery.RecoverFilesRecoveryException;
import org.density.indices.replication.common.ReplicationFailedException;
import org.density.ingest.IngestProcessorException;
import org.density.repositories.RepositoryException;
import org.density.rest.action.admin.indices.AliasesNotFoundException;
import org.density.search.SearchContextMissingException;
import org.density.search.SearchException;
import org.density.search.SearchParseException;
import org.density.search.SearchShardTarget;
import org.density.search.aggregations.MultiBucketConsumerService;
import org.density.search.internal.ShardSearchContextId;
import org.density.search.pipeline.SearchPipelineProcessingException;
import org.density.snapshots.Snapshot;
import org.density.snapshots.SnapshotException;
import org.density.snapshots.SnapshotId;
import org.density.snapshots.SnapshotInProgressException;
import org.density.snapshots.SnapshotInUseDeletionException;
import org.density.test.DensityTestCase;
import org.density.test.VersionUtils;
import org.density.transport.ActionNotFoundTransportException;
import org.density.transport.ActionTransportException;
import org.density.transport.ConnectTransportException;
import org.density.transport.NoSeedNodeLeftException;
import org.density.transport.NoSuchRemoteClusterException;
import org.density.transport.TcpTransport;
import org.density.transport.client.node.AbstractClientHeadersTestCase;
import org.density.transport.client.transport.NoNodeAvailableException;
import org.density.transport.stream.StreamException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isInterface;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.density.test.TestSearchContext.SHARD_TARGET;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ExceptionSerializationTests extends DensityTestCase {

    public void testExceptionRegistration() throws ClassNotFoundException, IOException, URISyntaxException {
        final Set<Class<?>> notRegistered = new HashSet<>();
        final Set<Class<?>> hasDedicatedWrite = new HashSet<>();
        final Set<Class<?>> registered = new HashSet<>();
        final String path = "org/density";
        final Path coreLibStartPath = PathUtils.get(DensityException.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final Path startPath = PathUtils.get(DensityServerException.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            .resolve("org")
            .resolve("density");
        final Set<String> ignore = Sets.newHashSet(
            CancellableThreadsTests.CustomException.class.getName(),
            org.density.rest.BytesRestResponseTests.WithHeadersException.class.getName(),
            AbstractClientHeadersTestCase.InternalException.class.getName(),
            "org.density.rest.action.RestActionListenerTests$2"
        );
        FileVisitor<Path> visitor = new FileVisitor<Path>() {
            private Path pkgPrefix = PathUtils.get(path).getParent();

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                pkgPrefix = pkgPrefix.resolve(dir.getFileName());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                checkFile(file.getFileName().toString());
                return FileVisitResult.CONTINUE;
            }

            private void checkFile(String filename) {
                if (filename.endsWith(".class") == false) {
                    return;
                }
                try {
                    checkClass(loadClass(filename));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            private void checkClass(Class<?> clazz) {
                if (ignore.contains(clazz.getName()) || isAbstract(clazz.getModifiers()) || isInterface(clazz.getModifiers())) {
                    return;
                }
                if (isEsException(clazz) == false) {
                    return;
                }
                if (DensityException.isRegistered(clazz.asSubclass(Throwable.class), Version.CURRENT) == false
                    && DensityException.class.equals(clazz.getEnclosingClass()) == false) {
                    notRegistered.add(clazz);
                } else if (DensityException.isRegistered(clazz.asSubclass(Throwable.class), Version.CURRENT)) {
                    registered.add(clazz);
                    try {
                        if (clazz.getMethod("writeTo", StreamOutput.class) != null) {
                            hasDedicatedWrite.add(clazz);
                        }
                    } catch (Exception e) {
                        // fair enough
                    }
                }
            }

            private boolean isEsException(Class<?> clazz) {
                return DensityException.class.isAssignableFrom(clazz);
            }

            private Class<?> loadClass(String filename) throws ClassNotFoundException {
                StringBuilder pkg = new StringBuilder();
                for (Path p : pkgPrefix) {
                    pkg.append(p.getFileName().toString()).append(".");
                }
                pkg.append(filename.substring(0, filename.length() - 6));
                return getClass().getClassLoader().loadClass(pkg.toString());
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                pkgPrefix = pkgPrefix.getParent();
                return FileVisitResult.CONTINUE;
            }
        };

        // walk the core library start path
        Files.walkFileTree(coreLibStartPath, visitor);
        // walk the server module start path
        Files.walkFileTree(startPath, visitor);
        final URI location = ExceptionSerializationTests.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        final Path testStartPath = PathUtils.get(location).resolve(path);
        Files.walkFileTree(testStartPath, visitor);
        assertTrue(notRegistered.remove(TestException.class));
        assertTrue(notRegistered.remove(UnknownHeaderException.class));
        assertTrue(
            "Classes subclassing DensityException must be registered in DensityException.DensityExceptionHandle \n"
                + notRegistered,
            notRegistered.isEmpty()
        );
        assertTrue(registered.removeAll(DensityException.getRegisteredKeys())); // check
        assertEquals(registered.toString(), 0, registered.size());
    }

    public static final class TestException extends DensityException {
        public TestException(StreamInput in) throws IOException {
            super(in);
        }
    }

    private <T extends Exception> T serialize(T exception) throws IOException {
        return serialize(exception, VersionUtils.randomVersion(random()));
    }

    private <T extends Exception> T serialize(T exception, Version version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        out.writeException(exception);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        return in.readException();
    }

    public void testIllegalShardRoutingStateException() throws IOException {
        final ShardRouting routing = TestShardRouting.newShardRouting("test", 0, "xyz", "def", false, ShardRoutingState.STARTED);
        final String routingAsString = routing.toString();
        IllegalShardRoutingStateException serialize = serialize(
            new IllegalShardRoutingStateException(routing, "foo", new NullPointerException())
        );
        assertNotNull(serialize.shard());
        assertEquals(routing, serialize.shard());
        assertEquals(routingAsString + ": foo", serialize.getMessage());
        assertTrue(serialize.getCause() instanceof NullPointerException);

        serialize = serialize(new IllegalShardRoutingStateException(routing, "bar", null));
        assertNotNull(serialize.shard());
        assertEquals(routing, serialize.shard());
        assertEquals(routingAsString + ": bar", serialize.getMessage());
        assertNull(serialize.getCause());
    }

    public void testParsingException() throws IOException {
        ParsingException ex = serialize(new ParsingException(1, 2, "fobar", null));
        assertNull(ex.getIndex());
        assertEquals(ex.getMessage(), "fobar");
        assertEquals(ex.getLineNumber(), 1);
        assertEquals(ex.getColumnNumber(), 2);

        ex = serialize(new ParsingException(1, 2, null, null));
        assertNull(ex.getIndex());
        assertNull(ex.getMessage());
        assertEquals(ex.getLineNumber(), 1);
        assertEquals(ex.getColumnNumber(), 2);
    }

    public void testQueryShardException() throws IOException {
        QueryShardException ex = serialize(new QueryShardException(new Index("foo", "_na_"), "fobar", null));
        assertEquals(ex.getIndex().getName(), "foo");
        assertEquals(ex.getMessage(), "fobar");

        ex = serialize(new QueryShardException((Index) null, null, null));
        assertNull(ex.getIndex());
        assertNull(ex.getMessage());
    }

    public void testSearchException() throws IOException {
        SearchShardTarget target = new SearchShardTarget("foo", new ShardId("bar", "_na_", 1), null, OriginalIndices.NONE);
        SearchException ex = serialize(new SearchException(target, "hello world"));
        assertEquals(target, ex.shard());
        assertEquals(ex.getMessage(), "hello world");

        ex = serialize(new SearchException(null, "hello world", new NullPointerException()));
        assertNull(ex.shard());
        assertEquals(ex.getMessage(), "hello world");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testActionNotFoundTransportException() throws IOException {
        ActionNotFoundTransportException ex = serialize(new ActionNotFoundTransportException("AACCCTION"));
        assertEquals("AACCCTION", ex.action());
        assertEquals("No handler for action [AACCCTION]", ex.getMessage());
    }

    public void testSnapshotException() throws IOException {
        final Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID()));
        SnapshotException ex = serialize(new SnapshotException(snapshot, "no such snapshot", new NullPointerException()));
        assertEquals(ex.getRepositoryName(), snapshot.getRepository());
        assertEquals(ex.getSnapshotName(), snapshot.getSnapshotId().getName());
        assertEquals(ex.getMessage(), "[" + snapshot + "] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);

        ex = serialize(new SnapshotException(null, "no such snapshot", new NullPointerException()));
        assertNull(ex.getRepositoryName());
        assertNull(ex.getSnapshotName());
        assertEquals(ex.getMessage(), "[_na] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testRecoverFilesRecoveryException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        ByteSizeValue bytes = new ByteSizeValue(randomIntBetween(0, 10000));
        RecoverFilesRecoveryException ex = serialize(new RecoverFilesRecoveryException(id, 10, bytes, null));
        assertEquals(ex.getShardId(), id);
        assertEquals(ex.numberOfFiles(), 10);
        assertEquals(ex.totalFilesSize(), bytes);
        assertEquals(ex.getMessage(), "Failed to transfer [10] files with total size of [" + bytes + "]");
        assertNull(ex.getCause());

        ex = serialize(new RecoverFilesRecoveryException(null, 10, bytes, new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals(ex.numberOfFiles(), 10);
        assertEquals(ex.totalFilesSize(), bytes);
        assertEquals(ex.getMessage(), "Failed to transfer [10] files with total size of [" + bytes + "]");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testInvalidIndexTemplateException() throws IOException {
        InvalidIndexTemplateException ex = serialize(new InvalidIndexTemplateException("foo", "bar"));
        assertEquals(ex.getMessage(), "index_template [foo] invalid, cause [bar]");
        assertEquals(ex.name(), "foo");
        ex = serialize(new InvalidIndexTemplateException(null, "bar"));
        assertEquals(ex.getMessage(), "index_template [null] invalid, cause [bar]");
        assertEquals(ex.name(), null);
    }

    public void testActionTransportException() throws IOException {
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        ActionTransportException ex = serialize(new ActionTransportException("name?", transportAddress, "ACTION BABY!", "message?", null));
        assertEquals("ACTION BABY!", ex.action());
        assertEquals(transportAddress, ex.address());
        assertEquals("[name?][" + transportAddress.toString() + "][ACTION BABY!] message?", ex.getMessage());
    }

    public void testSearchContextMissingException() throws IOException {
        ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomLong());
        Version version = VersionUtils.randomVersion(random());
        SearchContextMissingException ex = serialize(new SearchContextMissingException(contextId), version);
        assertThat(ex.contextId().getId(), equalTo(contextId.getId()));
        assertThat(ex.contextId().getSessionId(), equalTo(contextId.getSessionId()));
    }

    public void testCircuitBreakingException() throws IOException {
        CircuitBreakingException ex = serialize(
            new CircuitBreakingException("Too large", 0, 100, CircuitBreaker.Durability.TRANSIENT),
            Version.V_2_0_0
        );
        assertEquals("Too large", ex.getMessage());
        assertEquals(100, ex.getByteLimit());
        assertEquals(0, ex.getBytesWanted());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, ex.getDurability());
    }

    public void testTooManyBucketsException() throws IOException {
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        MultiBucketConsumerService.TooManyBucketsException ex = serialize(
            new MultiBucketConsumerService.TooManyBucketsException("Too many buckets", 100),
            version
        );
        assertEquals("Too many buckets", ex.getMessage());
        assertEquals(100, ex.getMaxBuckets());
    }

    public void testTimestampParsingException() throws IOException {
        TimestampParsingException ex = serialize(new TimestampParsingException("TIMESTAMP", null));
        assertEquals("failed to parse timestamp [TIMESTAMP]", ex.getMessage());
        assertEquals("TIMESTAMP", ex.timestamp());
    }

    public void testAliasesMissingException() throws IOException {
        AliasesNotFoundException ex = serialize(new AliasesNotFoundException("one", "two", "three"));
        assertEquals("aliases [one, two, three] missing", ex.getMessage());
        assertEquals("aliases", ex.getResourceType());
        assertArrayEquals(new String[] { "one", "two", "three" }, ex.getResourceId().toArray(new String[0]));
    }

    public void testSearchParseException() throws IOException {
        SearchParseException ex = serialize(new SearchParseException(SHARD_TARGET, "foo", new XContentLocation(66, 666)));
        assertEquals("foo", ex.getMessage());
        assertEquals(66, ex.getLineNumber());
        assertEquals(666, ex.getColumnNumber());
    }

    public void testIllegalIndexShardStateException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        IndexShardState state = randomFrom(IndexShardState.values());
        IllegalIndexShardStateException ex = serialize(new IllegalIndexShardStateException(id, state, "come back later buddy"));
        assertEquals(id, ex.getShardId());
        assertEquals("CurrentState[" + state.name() + "] come back later buddy", ex.getMessage());
        assertEquals(state, ex.currentState());
    }

    public void testConnectTransportException() throws IOException {
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        DiscoveryNode node = new DiscoveryNode("thenode", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        ConnectTransportException ex = serialize(new ConnectTransportException(node, "msg", "action", null));
        assertEquals("[][" + transportAddress.toString() + "][action] msg", ex.getMessage());
        assertEquals(node, ex.node());
        assertEquals("action", ex.action());
        assertNull(ex.getCause());

        ex = serialize(new ConnectTransportException(node, "msg", "action", new NullPointerException()));
        assertEquals("[][" + transportAddress + "][action] msg", ex.getMessage());
        assertEquals(node, ex.node());
        assertEquals("action", ex.action());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testSearchPhaseExecutionException() throws IOException {
        ShardSearchFailure[] empty = new ShardSearchFailure[0];
        SearchPhaseExecutionException ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), empty));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(empty.length, ex.shardFailures().length);
        ShardSearchFailure[] one = new ShardSearchFailure[] { new ShardSearchFailure(new IllegalArgumentException("nono!")) };

        ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), one));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(one.length, ex.shardFailures().length);
        assertTrue(ex.shardFailures()[0].getCause() instanceof IllegalArgumentException);
    }

    public void testRoutingMissingException() throws IOException {
        RoutingMissingException ex = serialize(new RoutingMissingException("idx", "id"));
        assertEquals("idx", ex.getIndex().getName());
        assertEquals("id", ex.getId());
        assertEquals("routing is required for [idx]/[id]", ex.getMessage());
    }

    public void testRepositoryException() throws IOException {
        RepositoryException ex = serialize(new RepositoryException("repo", "msg"));
        assertEquals("repo", ex.repository());
        assertEquals("[repo] msg", ex.getMessage());

        ex = serialize(new RepositoryException(null, "msg"));
        assertNull(ex.repository());
        assertEquals("[_na] msg", ex.getMessage());
    }

    public void testIndexTemplateMissingException() throws IOException {
        IndexTemplateMissingException ex = serialize(new IndexTemplateMissingException("name"));
        assertEquals("index_template [name] missing", ex.getMessage());
        assertEquals("name", ex.name());

        ex = serialize(new IndexTemplateMissingException((String) null));
        assertEquals("index_template [null] missing", ex.getMessage());
        assertNull(ex.name());
    }

    public void testRecoveryEngineException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        RecoveryEngineException ex = serialize(new RecoveryEngineException(id, 10, "total failure", new NullPointerException()));
        assertEquals(id, ex.getShardId());
        assertEquals("Phase[10] total failure", ex.getMessage());
        assertEquals(10, ex.phase());
        ex = serialize(new RecoveryEngineException(null, -1, "total failure", new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals(-1, ex.phase());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testFailedNodeException() throws IOException {
        FailedNodeException ex = serialize(new FailedNodeException("the node", "the message", null));
        assertEquals("the node", ex.nodeId());
        assertEquals("the message", ex.getMessage());
    }

    public void testClusterBlockException() throws IOException {
        ClusterBlockException ex = serialize(
            new ClusterBlockException(singleton(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES))
        );
        assertEquals("blocked by: [SERVICE_UNAVAILABLE/2/no cluster-manager];", ex.getMessage());
        assertTrue(ex.blocks().contains(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES));
        assertEquals(1, ex.blocks().size());
    }

    public void testNotSerializableExceptionWrapper() throws IOException {
        NotSerializableExceptionWrapper ex = serialize(new NotSerializableExceptionWrapper(new NullPointerException()));
        assertEquals(
            "{\"type\":\"null_pointer_exception\",\"reason\":\"null_pointer_exception: null\"}",
            Strings.toString(MediaTypeRegistry.JSON, ex)
        );
        ex = serialize(new NotSerializableExceptionWrapper(new IllegalArgumentException("nono!")));
        assertEquals(
            "{\"type\":\"illegal_argument_exception\",\"reason\":\"illegal_argument_exception: nono!\"}",
            Strings.toString(MediaTypeRegistry.JSON, ex)
        );

        class UnknownException extends Exception {
            UnknownException(final String message) {
                super(message);
            }
        }

        Exception[] unknowns = new Exception[] {
            new Exception("foobar"),
            new ClassCastException("boom boom boom"),
            new UnknownException("boom") };
        for (Exception t : unknowns) {
            if (randomBoolean()) {
                t.addSuppressed(new UnsatisfiedLinkError("suppressed"));
                t.addSuppressed(new NullPointerException());
            }
            Throwable deserialized = serialize(t);
            assertTrue(deserialized.getClass().toString(), deserialized instanceof NotSerializableExceptionWrapper);
            assertArrayEquals(t.getStackTrace(), deserialized.getStackTrace());
            assertEquals(t.getSuppressed().length, deserialized.getSuppressed().length);
            if (t.getSuppressed().length > 0) {
                assertTrue(deserialized.getSuppressed()[0] instanceof NotSerializableExceptionWrapper);
                assertArrayEquals(t.getSuppressed()[0].getStackTrace(), deserialized.getSuppressed()[0].getStackTrace());
                assertTrue(deserialized.getSuppressed()[1] instanceof NullPointerException);
            }
        }
    }

    public void testUnknownException() throws IOException {
        ParsingException parsingException = new ParsingException(1, 2, "foobar", null);
        final Exception ex = new UnknownException("eggplant", parsingException);
        Exception exception = serialize(ex);
        assertEquals("unknown_exception: eggplant", exception.getMessage());
        assertTrue(exception instanceof DensityException);
        ParsingException e = (ParsingException) exception.getCause();
        assertEquals(parsingException.getIndex(), e.getIndex());
        assertEquals(parsingException.getMessage(), e.getMessage());
        assertEquals(parsingException.getLineNumber(), e.getLineNumber());
        assertEquals(parsingException.getColumnNumber(), e.getColumnNumber());
    }

    public void testWriteThrowable() throws IOException {
        final QueryShardException queryShardException = new QueryShardException(new Index("foo", "_na_"), "foobar", null);
        final UnknownException unknownException = new UnknownException("this exception is unknown", queryShardException);

        final Exception[] causes = new Exception[] {
            new IllegalStateException("foobar"),
            new IllegalArgumentException("alalaal"),
            new NullPointerException("boom"),
            new EOFException("dadada"),
            new DensitySecurityException("nono!"),
            new NumberFormatException("not a number"),
            new CorruptIndexException("baaaam booom", "this is my resource"),
            new IndexFormatTooNewException("tooo new", 1, 2, 3),
            new IndexFormatTooOldException("tooo new", 1, 2, 3),
            new IndexFormatTooOldException("tooo new", "very old version"),
            new ArrayIndexOutOfBoundsException("booom"),
            new StringIndexOutOfBoundsException("booom"),
            new FileNotFoundException("booom"),
            new NoSuchFileException("booom"),
            new AlreadyClosedException("closed!!", new NullPointerException()),
            new LockObtainFailedException("can't lock directory", new NullPointerException()),
            unknownException };
        for (final Exception cause : causes) {
            DensityException ex = new DensityException("topLevel", cause);
            DensityException deserialized = serialize(ex);
            assertEquals(deserialized.getMessage(), ex.getMessage());
            assertTrue(
                "Expected: "
                    + deserialized.getCause().getMessage()
                    + " to contain: "
                    + ex.getCause().getClass().getName()
                    + " but it didn't",
                deserialized.getCause().getMessage().contains(ex.getCause().getMessage())
            );
            if (ex.getCause().getClass() != UnknownException.class) { // unknown exception is not directly mapped
                assertEquals(deserialized.getCause().getClass(), ex.getCause().getClass());
            } else {
                assertEquals(deserialized.getCause().getClass(), NotSerializableExceptionWrapper.class);
            }
            assertArrayEquals(deserialized.getStackTrace(), ex.getStackTrace());
            assertTrue(deserialized.getStackTrace().length > 1);
        }
    }

    public void testWithRestHeadersException() throws IOException {
        {
            DensityException ex = new DensityException("msg");
            ex.addHeader("foo", "foo", "bar");
            ex.addMetadata("density.foo_metadata", "value1", "value2");
            ex = serialize(ex);
            assertEquals("msg", ex.getMessage());
            assertEquals(2, ex.getHeader("foo").size());
            assertEquals("foo", ex.getHeader("foo").get(0));
            assertEquals("bar", ex.getHeader("foo").get(1));
            assertEquals(2, ex.getMetadata("density.foo_metadata").size());
            assertEquals("value1", ex.getMetadata("density.foo_metadata").get(0));
            assertEquals("value2", ex.getMetadata("density.foo_metadata").get(1));
        }
        {
            RestStatus status = randomFrom(RestStatus.values());
            // ensure we are carrying over the headers and metadata even if not serialized
            UnknownHeaderException uhe = new UnknownHeaderException("msg", status);
            uhe.addHeader("foo", "foo", "bar");
            uhe.addMetadata("density.foo_metadata", "value1", "value2");

            DensityException serialize = serialize((DensityException) uhe);
            assertTrue(serialize instanceof NotSerializableExceptionWrapper);
            NotSerializableExceptionWrapper e = (NotSerializableExceptionWrapper) serialize;
            assertEquals("unknown_header_exception: msg", e.getMessage());
            assertEquals(2, e.getHeader("foo").size());
            assertEquals("foo", e.getHeader("foo").get(0));
            assertEquals("bar", e.getHeader("foo").get(1));
            assertEquals(2, e.getMetadata("density.foo_metadata").size());
            assertEquals("value1", e.getMetadata("density.foo_metadata").get(0));
            assertEquals("value2", e.getMetadata("density.foo_metadata").get(1));
            assertSame(status, e.status());
        }
    }

    public void testNoLongerPrimaryShardException() throws IOException {
        ShardId shardId = new ShardId(new Index(randomAlphaOfLength(4), randomAlphaOfLength(4)), randomIntBetween(0, Integer.MAX_VALUE));
        String msg = randomAlphaOfLength(4);
        ShardStateAction.NoLongerPrimaryShardException ex = serialize(new ShardStateAction.NoLongerPrimaryShardException(shardId, msg));
        assertEquals(shardId, ex.getShardId());
        assertEquals(msg, ex.getMessage());
    }

    public static class UnknownHeaderException extends DensityException {
        private final RestStatus status;

        UnknownHeaderException(String msg, RestStatus status) {
            super(msg);
            this.status = status;
        }

        @Override
        public RestStatus status() {
            return status;
        }
    }

    public void testDensitySecurityException() throws IOException {
        DensitySecurityException ex = new DensitySecurityException("user [{}] is not allowed", RestStatus.UNAUTHORIZED, "foo");
        DensitySecurityException e = serialize(ex);
        assertEquals(ex.status(), e.status());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
    }

    public void testInterruptedException() throws IOException {
        InterruptedException orig = randomBoolean() ? new InterruptedException("boom") : new InterruptedException();
        InterruptedException ex = serialize(orig);
        assertEquals(orig.getMessage(), ex.getMessage());
    }

    public void testThatIdsArePositive() {
        for (final int id : DensityException.ids()) {
            assertThat("negative id", id, greaterThanOrEqualTo(0));
        }
    }

    public void testThatIdsAreUnique() {
        final Set<Integer> ids = new HashSet<>();
        for (final int id : DensityException.ids()) {
            assertTrue("duplicate id", ids.add(id));
        }
    }

    public void testIds() {
        Map<Integer, Class<? extends DensityException>> ids = new HashMap<>();
        ids.put(0, IndexShardSnapshotFailedException.class);
        ids.put(1, org.density.search.dfs.DfsPhaseExecutionException.class);
        ids.put(2, org.density.common.util.CancellableThreads.ExecutionCancelledException.class);
        ids.put(3, org.density.discovery.ClusterManagerNotDiscoveredException.class);
        ids.put(4, org.density.DensitySecurityException.class);
        ids.put(5, org.density.index.snapshots.IndexShardRestoreException.class);
        ids.put(6, org.density.indices.IndexClosedException.class);
        ids.put(7, org.density.http.BindHttpException.class);
        ids.put(8, org.density.action.search.ReduceSearchPhaseException.class);
        ids.put(9, org.density.node.NodeClosedException.class);
        ids.put(10, org.density.index.engine.SnapshotFailedEngineException.class);
        ids.put(11, org.density.index.shard.ShardNotFoundException.class);
        ids.put(12, org.density.transport.ConnectTransportException.class);
        ids.put(13, org.density.transport.NotSerializableTransportException.class);
        ids.put(14, org.density.transport.ResponseHandlerFailureTransportException.class);
        ids.put(15, org.density.indices.IndexCreationException.class);
        ids.put(16, org.density.index.IndexNotFoundException.class);
        ids.put(17, org.density.cluster.routing.IllegalShardRoutingStateException.class);
        ids.put(18, org.density.action.support.broadcast.BroadcastShardOperationFailedException.class);
        ids.put(19, org.density.ResourceNotFoundException.class);
        ids.put(20, org.density.transport.ActionTransportException.class);
        ids.put(21, org.density.DensityGenerationException.class);
        ids.put(22, null); // was CreateFailedEngineException
        ids.put(23, org.density.index.shard.IndexShardStartedException.class);
        ids.put(24, org.density.search.SearchContextMissingException.class);
        ids.put(25, org.density.script.GeneralScriptException.class);
        ids.put(26, null);
        ids.put(27, org.density.snapshots.SnapshotCreationException.class);
        ids.put(28, null); // was DeleteFailedEngineException, deprecated in 6.0 and removed in 7.0
        ids.put(29, org.density.index.engine.DocumentMissingException.class);
        ids.put(30, org.density.snapshots.SnapshotException.class);
        ids.put(31, org.density.indices.InvalidAliasNameException.class);
        ids.put(32, org.density.indices.InvalidIndexNameException.class);
        ids.put(33, org.density.indices.IndexPrimaryShardNotAllocatedException.class);
        ids.put(34, org.density.transport.TransportException.class);
        ids.put(35, org.density.DensityParseException.class);
        ids.put(36, org.density.search.SearchException.class);
        ids.put(37, org.density.index.mapper.MapperException.class);
        ids.put(38, org.density.indices.InvalidTypeNameException.class);
        ids.put(39, org.density.snapshots.SnapshotRestoreException.class);
        ids.put(40, ParsingException.class);
        ids.put(41, org.density.index.shard.IndexShardClosedException.class);
        ids.put(42, org.density.indices.recovery.RecoverFilesRecoveryException.class);
        ids.put(43, org.density.index.translog.TruncatedTranslogException.class);
        ids.put(44, org.density.indices.recovery.RecoveryFailedException.class);
        ids.put(45, org.density.index.shard.IndexShardRelocatedException.class);
        ids.put(46, org.density.transport.NodeShouldNotConnectException.class);
        ids.put(47, null);
        ids.put(48, org.density.index.translog.TranslogCorruptedException.class);
        ids.put(49, org.density.cluster.block.ClusterBlockException.class);
        ids.put(50, org.density.search.fetch.FetchPhaseExecutionException.class);
        ids.put(51, null);
        ids.put(52, org.density.index.engine.VersionConflictEngineException.class);
        ids.put(53, org.density.index.engine.EngineException.class);
        ids.put(54, null); // was DocumentAlreadyExistsException, which is superseded with VersionConflictEngineException
        ids.put(55, org.density.action.NoSuchNodeException.class);
        ids.put(56, org.density.common.settings.SettingsException.class);
        ids.put(57, org.density.indices.IndexTemplateMissingException.class);
        ids.put(58, org.density.transport.SendRequestTransportException.class);
        ids.put(59, null); // was DensityRejectedExecutionException, which is no longer an instance of DensityException
        ids.put(60, null); // EarlyTerminationException was removed in 6.0
        ids.put(61, null); // RoutingValidationException was removed in 5.0
        ids.put(62, NotSerializableExceptionWrapper.class);
        ids.put(63, org.density.indices.AliasFilterParsingException.class);
        ids.put(64, null); // DeleteByQueryFailedEngineException was removed in 3.0
        ids.put(65, org.density.gateway.GatewayException.class);
        ids.put(66, org.density.index.shard.IndexShardNotRecoveringException.class);
        ids.put(67, org.density.http.HttpException.class);
        ids.put(68, org.density.DensityException.class);
        ids.put(69, org.density.snapshots.SnapshotMissingException.class);
        ids.put(70, org.density.action.PrimaryMissingActionException.class);
        ids.put(71, org.density.action.FailedNodeException.class);
        ids.put(72, org.density.search.SearchParseException.class);
        ids.put(73, org.density.snapshots.ConcurrentSnapshotExecutionException.class);
        ids.put(74, org.density.common.blobstore.BlobStoreException.class);
        ids.put(75, org.density.cluster.IncompatibleClusterStateVersionException.class);
        ids.put(76, org.density.index.engine.RecoveryEngineException.class);
        ids.put(77, org.density.common.util.concurrent.UncategorizedExecutionException.class);
        ids.put(78, org.density.action.TimestampParsingException.class);
        ids.put(79, org.density.action.RoutingMissingException.class);
        ids.put(80, null); // was IndexFailedEngineException, deprecated in 6.0 and removed in 7.0
        ids.put(81, org.density.index.snapshots.IndexShardRestoreFailedException.class);
        ids.put(82, org.density.repositories.RepositoryException.class);
        ids.put(83, org.density.transport.ReceiveTimeoutTransportException.class);
        ids.put(84, org.density.transport.NodeDisconnectedException.class);
        ids.put(85, null);
        ids.put(86, org.density.search.aggregations.AggregationExecutionException.class);
        ids.put(88, org.density.indices.InvalidIndexTemplateException.class);
        ids.put(90, org.density.index.engine.RefreshFailedEngineException.class);
        ids.put(91, org.density.search.aggregations.AggregationInitializationException.class);
        ids.put(92, org.density.indices.recovery.DelayRecoveryException.class);
        ids.put(94, NoNodeAvailableException.class);
        ids.put(95, null);
        ids.put(96, org.density.snapshots.InvalidSnapshotNameException.class);
        ids.put(97, org.density.index.shard.IllegalIndexShardStateException.class);
        ids.put(98, IndexShardSnapshotException.class);
        ids.put(99, org.density.index.shard.IndexShardNotStartedException.class);
        ids.put(100, org.density.action.search.SearchPhaseExecutionException.class);
        ids.put(101, org.density.transport.ActionNotFoundTransportException.class);
        ids.put(102, org.density.transport.TransportSerializationException.class);
        ids.put(103, org.density.transport.RemoteTransportException.class);
        ids.put(104, org.density.index.engine.EngineCreationFailureException.class);
        ids.put(105, org.density.cluster.routing.RoutingException.class);
        ids.put(106, org.density.index.shard.IndexShardRecoveryException.class);
        ids.put(107, org.density.repositories.RepositoryMissingException.class);
        ids.put(108, null);
        ids.put(109, org.density.index.engine.DocumentSourceMissingException.class);
        ids.put(110, null); // FlushNotAllowedEngineException was removed in 5.0
        ids.put(111, org.density.common.settings.NoClassSettingsException.class);
        ids.put(112, org.density.transport.BindTransportException.class);
        ids.put(113, org.density.rest.action.admin.indices.AliasesNotFoundException.class);
        ids.put(114, org.density.index.shard.IndexShardRecoveringException.class);
        ids.put(115, org.density.index.translog.TranslogException.class);
        ids.put(116, org.density.cluster.metadata.ProcessClusterEventTimeoutException.class);
        ids.put(117, ReplicationOperation.RetryOnPrimaryException.class);
        ids.put(118, org.density.DensityTimeoutException.class);
        ids.put(119, org.density.search.query.QueryPhaseExecutionException.class);
        ids.put(120, org.density.repositories.RepositoryVerificationException.class);
        ids.put(121, org.density.search.aggregations.InvalidAggregationPathException.class);
        ids.put(122, null);
        ids.put(123, org.density.ResourceAlreadyExistsException.class);
        ids.put(124, null);
        ids.put(125, TcpTransport.HttpRequestOnTransportException.class);
        ids.put(126, org.density.index.mapper.MapperParsingException.class);
        ids.put(127, null); // was org.density.search.SearchContextException.class
        ids.put(128, org.density.search.builder.SearchSourceBuilderException.class);
        ids.put(129, null); // was org.density.index.engine.EngineClosedException.class
        ids.put(130, org.density.action.NoShardAvailableActionException.class);
        ids.put(131, org.density.action.UnavailableShardsException.class);
        ids.put(132, org.density.index.engine.FlushFailedEngineException.class);
        ids.put(133, org.density.core.common.breaker.CircuitBreakingException.class);
        ids.put(134, org.density.transport.NodeNotConnectedException.class);
        ids.put(135, org.density.index.mapper.StrictDynamicMappingException.class);
        ids.put(136, org.density.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class);
        ids.put(137, org.density.indices.TypeMissingException.class);
        ids.put(138, null);
        ids.put(139, null);
        ids.put(140, org.density.cluster.coordination.FailedToCommitClusterStateException.class);
        ids.put(141, org.density.index.query.QueryShardException.class);
        ids.put(142, ShardStateAction.NoLongerPrimaryShardException.class);
        ids.put(143, org.density.script.ScriptException.class);
        ids.put(144, org.density.cluster.NotClusterManagerException.class);
        ids.put(145, org.density.DensityStatusException.class);
        ids.put(146, org.density.core.tasks.TaskCancelledException.class);
        ids.put(147, org.density.env.ShardLockObtainFailedException.class);
        ids.put(148, null);
        ids.put(149, MultiBucketConsumerService.TooManyBucketsException.class);
        ids.put(150, CoordinationStateRejectedException.class);
        ids.put(151, SnapshotInProgressException.class);
        ids.put(152, NoSuchRemoteClusterException.class);
        ids.put(153, RetentionLeaseAlreadyExistsException.class);
        ids.put(154, RetentionLeaseNotFoundException.class);
        ids.put(155, ShardNotInPrimaryModeException.class);
        ids.put(156, RetentionLeaseInvalidRetainingSeqNoException.class);
        ids.put(157, IngestProcessorException.class);
        ids.put(158, PeerRecoveryNotFound.class);
        ids.put(159, NodeHealthCheckFailureException.class);
        ids.put(160, NoSeedNodeLeftException.class);
        ids.put(161, ReplicationFailedException.class);
        ids.put(162, PrimaryShardClosedException.class);
        ids.put(163, DecommissioningFailedException.class);
        ids.put(164, NodeDecommissionedException.class);
        ids.put(165, ClusterManagerThrottlingException.class);
        ids.put(166, SnapshotInUseDeletionException.class);
        ids.put(167, UnsupportedWeightedRoutingStateException.class);
        ids.put(168, PreferenceBasedSearchNotAllowedException.class);
        ids.put(169, NodeWeighedAwayException.class);
        ids.put(170, SearchPipelineProcessingException.class);
        ids.put(171, CryptoRegistryException.class);
        ids.put(172, ViewNotFoundException.class);
        ids.put(173, ViewAlreadyExistsException.class);
        ids.put(174, InvalidIndexContextException.class);
        ids.put(175, ResponseLimitBreachedException.class);
        ids.put(176, IngestionEngineException.class);
        ids.put(177, StreamException.class);
        ids.put(10001, IndexCreateBlockException.class);

        Map<Class<? extends DensityException>, Integer> reverse = new HashMap<>();
        for (Map.Entry<Integer, Class<? extends DensityException>> entry : ids.entrySet()) {
            if (entry.getValue() != null) {
                reverse.put(entry.getValue(), entry.getKey());
            }
        }

        for (final Tuple<Integer, Class<? extends DensityException>> tuple : DensityException.classes()) {
            assertNotNull(tuple.v1());
            assertNotNull(
                tuple.v2().getName() + " not found in ExceptionSerializationTests.testIds. Please add it.",
                reverse.get(tuple.v2())
            );
            assertEquals((int) reverse.get(tuple.v2()), (int) tuple.v1());
        }

        for (Map.Entry<Integer, Class<? extends DensityException>> entry : ids.entrySet()) {
            if (entry.getValue() != null) {
                assertEquals((int) entry.getKey(), DensityException.getId(entry.getValue()));
            }
        }
    }

    public void testIOException() throws IOException {
        IOException serialize = serialize(new IOException("boom", new NullPointerException()));
        assertEquals("boom", serialize.getMessage());
        assertTrue(serialize.getCause() instanceof NullPointerException);
    }

    public void testFileSystemExceptions() throws IOException {
        for (FileSystemException ex : Arrays.asList(
            new FileSystemException("a", "b", "c"),
            new NoSuchFileException("a", "b", "c"),
            new NotDirectoryException("a"),
            new DirectoryNotEmptyException("a"),
            new AtomicMoveNotSupportedException("a", "b", "c"),
            new FileAlreadyExistsException("a", "b", "c"),
            new AccessDeniedException("a", "b", "c"),
            new FileSystemLoopException("a")
        )) {

            FileSystemException serialize = serialize(ex);
            assertEquals(serialize.getClass(), ex.getClass());
            assertEquals("a", serialize.getFile());
            if (serialize.getClass() == NotDirectoryException.class
                || serialize.getClass() == FileSystemLoopException.class
                || serialize.getClass() == DirectoryNotEmptyException.class) {
                assertNull(serialize.getOtherFile());
                assertNull(serialize.getReason());
            } else {
                assertEquals(serialize.getClass().toString(), "b", serialize.getOtherFile());
                assertEquals(serialize.getClass().toString(), "c", serialize.getReason());
            }
        }
    }

    public void testDensityRemoteException() throws IOException {
        DensityStatusException ex = new DensityStatusException("something", RestStatus.TOO_MANY_REQUESTS);
        DensityStatusException e = serialize(ex);
        assertEquals(ex.status(), e.status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    }

    public void testShardLockObtainFailedException() throws IOException {
        ShardId shardId = new ShardId("foo", "_na_", 1);
        ShardLockObtainFailedException orig = new ShardLockObtainFailedException(shardId, "boom");
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        ShardLockObtainFailedException ex = serialize(orig, version);
        assertEquals(orig.getMessage(), ex.getMessage());
        assertEquals(orig.getShardId(), ex.getShardId());
    }

    public void testSnapshotInProgressException() throws IOException {
        SnapshotInProgressException orig = new SnapshotInProgressException("boom");
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        SnapshotInProgressException ex = serialize(orig, version);
        assertEquals(orig.getMessage(), ex.getMessage());
    }

    private static class UnknownException extends Exception {
        UnknownException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
