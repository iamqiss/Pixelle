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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.INDEX_TYPES;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.token.ReadOnlyTokenCreator.READ_ONLY;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.primitive.ObjectFloatMaps;
import org.eclipse.collections.impl.factory.Multimaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.IndexImporterFactory.CreationContext;
import org.neo4j.batchimport.api.IndexesCreator;
import org.neo4j.batchimport.api.IndexesCreator.CreationListener;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.BulkIndexCreationContext;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@Neo4jLayoutExtension
@PageCacheExtension
class BulkIndexesCreatorTest {

    // for relationships and nodes and for all the different index types
    private static final int TOKEN_COUNT = INDEX_TYPES.size() * 2 * 2;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private PageCache pageCache;

    private final JobScheduler scheduler = JobSchedulerFactory.createScheduler();

    private final ReadableStorageEngine storageEngine = mock(ReadableStorageEngine.class);

    private final StoreCursors storeCursors = mock(StoreCursors.class);

    private final StorageReader storageReader = mock(StorageReader.class);

    private final StorageNodeCursor nodeCursor = mock(StorageNodeCursor.class);

    private final StorageRelationshipScanCursor relCursor = mock(StorageRelationshipScanCursor.class);

    @BeforeEach
    void setUp() throws Exception {
        scheduler.init();

        when(storageEngine.getOpenOptions()).thenReturn(immutable.empty());
        when(storageEngine.createStorageCursors(any())).thenReturn(storeCursors);
        when(storageEngine.newReader()).thenReturn(storageReader);
        when(storageEngine.indexingBehaviour()).thenReturn(StorageEngineIndexingBehaviour.EMPTY);

        when(storageReader.allocateNodeCursor(any(), any(), any())).thenReturn(nodeCursor);
        when(storageReader.allocateRelationshipScanCursor(any(), any(), any())).thenReturn(relCursor);
    }

    @AfterEach
    void shutdown() throws Exception {
        scheduler.shutdown();
    }

    @Test
    void factoryRequiresCorrectContext() {
        final var factory = new IndexImporterFactoryImpl();
        assertThatThrownBy(() -> factory.getCreator(new DuffContext(fs)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Index creation requires an instance of BulkIndexCreationContext");
    }

    private static Stream<Arguments> completeConfiguration() {
        return INDEX_TYPES.entrySet().stream()
                // skipping VectorV1 as it requires the config to already be set before completion
                .filter(entry -> entry.getKey() != VECTOR_V1_DESCRIPTOR)
                .map(entry -> {
                    final var indexProviderDescriptor = entry.getKey();
                    final var indexType = entry.getValue();
                    // only Point and Fulltext add config - VectorV2 defaults come from Cypher-land and not the provider
                    final var addsDefaultConfig = indexType == IndexType.POINT || indexType == IndexType.FULLTEXT;
                    return Arguments.of(indexProviderDescriptor, indexType, addsDefaultConfig);
                });
    }

    @ParameterizedTest
    @MethodSource
    void completeConfiguration(IndexProviderDescriptor providerDescriptor, IndexType type, boolean addsDefaultConfig)
            throws IOException {
        runWithIndexCreator(indexesCreator -> {
            final var descriptor = forSchema(forLabel(1, 2))
                    .withName("basic")
                    .withIndexType(type)
                    .withIndexProvider(providerDescriptor)
                    .materialise(3);
            assertThat(descriptor.getCapability()).isEqualTo(IndexCapability.NO_CAPABILITY);

            final var completedDescriptor = indexesCreator.completeConfiguration(descriptor);
            assertThat(completedDescriptor.getCapability())
                    .as("completion should set the correct capability")
                    .isNotEqualTo(IndexCapability.NO_CAPABILITY);
            if (addsDefaultConfig) {
                assertThat(descriptor.getIndexConfig().asMap())
                        .as("use the default index config for descriptor")
                        .isEmpty();

                assertThat(completedDescriptor.getIndexConfig().asMap())
                        .as("completion should set the default index config for descriptor")
                        .isNotEmpty();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void createSingle(IndexDescriptor descriptor) throws Exception {
        assertCreation(descriptor);
    }

    @Test
    void createAll() throws Exception {
        assertCreation(indexes().map(args -> (IndexDescriptor) args.get()[0]).toArray(IndexDescriptor[]::new));
    }

    @Test
    void createWithError() throws IOException {
        runWithIndexCreator(indexesCreator -> {
            final var deltas = ObjectFloatMaps.mutable.<IndexDescriptor>empty();
            final var completed = new MutableBoolean();
            final var checkPointed = new MutableBoolean();

            // this isn't valid config (RANGE + LOOKUP) => boom
            final var descriptor = indexesCreator.completeConfiguration(forSchema(forLabel(1, 2))
                    .withName("duff")
                    .withIndexType(IndexType.RANGE)
                    .withIndexProvider(TOKEN_DESCRIPTOR)
                    .materialise(0));
            indexesCreator.create(
                    new CreationListener() {
                        @Override
                        public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                            deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                        }

                        @Override
                        public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                            assertThat(indexDescriptor).isEqualTo(descriptor);
                            assertThat(error)
                                    .hasMessageContainingAll(
                                            "Failed to populate index",
                                            "type='RANGE'",
                                            "indexProvider='token-lookup-1.0'");
                        }

                        @Override
                        public void onCreationCompleted(boolean withSuccess) {
                            completed.setValue(withSuccess);
                        }

                        @Override
                        public void onCheckpointingCompleted() {
                            checkPointed.setTrue();
                        }
                    },
                    List.of(descriptor));

            assertThat(deltas.get(descriptor))
                    .as("should not have completed the progress")
                    .isEqualTo(0.0f);

            assertThat(fs.fileExists(directoriesByProvider(databaseLayout.databaseDirectory())
                            .forProvider(descriptor.getIndexProvider())
                            .directoryForIndex(descriptor.getId())))
                    .as("should still create the directory structure of the index")
                    .isTrue();

            assertThat(completed.booleanValue())
                    .as("should complete the creation steps with failure flag")
                    .isFalse();
            assertThat(checkPointed.booleanValue())
                    .as("should NOT checkpoint on failure")
                    .isFalse();
        });
    }

    @Test
    void longerRunningIndexingReportsCorrectly() throws IOException {
        final var config = Config.defaults();
        final var context = new BulkIndexCreationContext(
                config,
                storageEngine,
                databaseLayout,
                fs,
                pageCache,
                new MetadataCache(KernelVersion.getLatestVersion(config)),
                scheduler,
                new TokenHolders(
                        tokenHolder(TokenHolder.TYPE_LABEL),
                        tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE),
                        tokenHolder(TokenHolder.TYPE_PROPERTY_KEY)),
                ElementIdMapper.PLACEHOLDER,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                NullLogService.getInstance(),
                INSTANCE);
        final var indexingService = mock(IndexingService.class);
        try (var indexesCreator = new BulkIndexesCreator(context) {
            @Override
            protected IndexingService createIndexingService(LifeSupport life, BulkIndexCreationContext context) {
                return indexingService;
            }
        }) {
            final var descriptor1 = forSchema(forLabel(1, 2))
                    .withName("descriptor1")
                    .withIndexType(IndexType.RANGE)
                    .withIndexProvider(RANGE_DESCRIPTOR)
                    .materialise(1);
            final var descriptor2 = forSchema(forLabel(3, 4))
                    .withName("descriptor2")
                    .withIndexType(IndexType.RANGE)
                    .withIndexProvider(RANGE_DESCRIPTOR)
                    .materialise(2);

            final var d1p1 = 0.0f;
            final var d2p1 = 0.0f;
            final var d1p2 = 0.1f;
            final var d2p2 = 0.1f;
            final var d2p3 = 0.2f;
            final var d2p4 = 0.5f;
            final var d2p5 = 0.9f;
            final var d2p6 = 1.0f;

            final var state1 = Sets.mutable.of(populatingProxy(descriptor1, d1p1), populatingProxy(descriptor2, d2p1));
            final var state2 = Sets.mutable.of(populatingProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p2));
            final var state3 = Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p3));
            final var state4 = Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p4));
            final var state5 = Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p5));
            final var state6 = Sets.mutable.of(failedProxy(descriptor1, d1p2), populatingProxy(descriptor2, d2p6));
            //noinspection unchecked
            when(indexingService.getIndexProxies()).thenReturn(state1, state2, state3, state4, state5, state6);

            final var completed = new MutableBoolean();
            final var errors = Lists.mutable.<IndexDescriptor>empty();
            final var progress = Multimaps.mutable.list.<IndexDescriptor, Float>empty();
            indexesCreator.create(
                    new CreationListener() {
                        @Override
                        public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                            progress.put(indexDescriptor, percentDelta);
                        }

                        @Override
                        public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                            errors.add(indexDescriptor);
                        }

                        @Override
                        public void onCreationCompleted(boolean withSuccess) {
                            completed.setValue(withSuccess);
                        }

                        @Override
                        public void onCheckpointingCompleted() {
                            fail("should not perform checkpointing as errors expected");
                        }
                    },
                    List.of(descriptor1, descriptor2));

            assertThat(completed.booleanValue())
                    .as("should finish with an error")
                    .isFalse();
            assertThat(errors).as("should complete with the one error").containsExactly(descriptor1);
            assertThat(progress.get(descriptor1)).containsExactly(d1p2);
            assertThat(progress.get(descriptor2)).hasSize(5).satisfies(deltas -> {
                for (var delta : deltas) {
                    assertThat(delta).isGreaterThan(0.0f);
                }
            });
        }
    }

    private void assertCreation(IndexDescriptor... descriptors) throws Exception {
        runWithIndexCreator(indexesCreator -> {
            final var deltas = ObjectFloatMaps.mutable.<IndexDescriptor>empty();
            final var completed = new MutableBoolean();
            final var checkPointed = new MutableBoolean();
            final var indexDescriptors = Arrays.stream(descriptors)
                    .map(indexesCreator::completeConfiguration)
                    .toList();
            indexesCreator.create(
                    new CreationListener() {
                        @Override
                        public void onUpdate(IndexDescriptor indexDescriptor, float percentDelta) {
                            deltas.updateValue(indexDescriptor, 0.0f, f -> f + percentDelta);
                        }

                        @Override
                        public void onFailure(IndexDescriptor indexDescriptor, KernelException error) {
                            fail("should not report any error for %s: %s", indexDescriptor, error);
                        }

                        @Override
                        public void onCreationCompleted(boolean withSuccess) {
                            completed.setValue(withSuccess);
                        }

                        @Override
                        public void onCheckpointingCompleted() {
                            checkPointed.setTrue();
                        }
                    },
                    indexDescriptors);

            // THEN
            for (var descriptor : descriptors) {
                assertThat(deltas.get(descriptor))
                        .as("should have completed the progress")
                        .isEqualTo(1.0f);

                assertThat(fs.fileExists(directoriesByProvider(databaseLayout.databaseDirectory())
                                .forProvider(descriptor.getIndexProvider())
                                .directoryForIndex(descriptor.getId())))
                        .as("should create the directory structure of the index")
                        .isTrue();
            }

            assertThat(completed.booleanValue())
                    .as("should complete the creation steps")
                    .isTrue();
            assertThat(checkPointed.booleanValue())
                    .as("should checkpoint the results indexes")
                    .isTrue();
        });
    }

    private void runWithIndexCreator(ThrowingConsumer<IndexesCreator, IOException> consumer) throws IOException {
        final var config = Config.defaults();
        try (var indexesCreator = new BulkIndexesCreator(new BulkIndexCreationContext(
                config,
                storageEngine,
                databaseLayout,
                fs,
                pageCache,
                new MetadataCache(KernelVersion.getLatestVersion(config)),
                scheduler,
                new TokenHolders(
                        tokenHolder(TokenHolder.TYPE_LABEL),
                        tokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE),
                        tokenHolder(TokenHolder.TYPE_PROPERTY_KEY)),
                ElementIdMapper.PLACEHOLDER,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                NullLogService.getInstance(),
                INSTANCE))) {
            consumer.accept(indexesCreator);
        }
    }

    private static IndexProxy populatingProxy(IndexDescriptor descriptor, float percent) {
        final var populationProgress = mock(PopulationProgress.class);
        when(populationProgress.getProgress()).thenReturn(percent);

        final var proxy = mock(IndexProxy.class);
        when(proxy.getState()).thenReturn(InternalIndexState.POPULATING);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        when(proxy.getIndexPopulationProgress()).thenReturn(populationProgress);
        return proxy;
    }

    @SuppressWarnings("SameParameterValue")
    private static IndexProxy failedProxy(IndexDescriptor descriptor, float percent) {
        final var populationProgress = mock(PopulationProgress.class);
        when(populationProgress.getProgress()).thenReturn(percent);

        final var failure = mock(IndexPopulationFailure.class);
        when(failure.asIndexPopulationFailure(any(), any()))
                .thenReturn(new IndexPopulationFailedKernelException("boom", new IOException()));

        final var proxy = mock(IndexProxy.class);
        when(proxy.getState()).thenReturn(InternalIndexState.FAILED);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        when(proxy.getIndexPopulationProgress()).thenReturn(populationProgress);
        when(proxy.getPopulationFailure()).thenReturn(failure);
        return proxy;
    }

    private static TokenHolder tokenHolder(String typePropertyKey) {
        var tokenHolder = new CreatingTokenHolder(READ_ONLY, typePropertyKey);
        tokenHolder.setInitialTokens(IntStream.range(0, TOKEN_COUNT)
                .mapToObj(i -> new NamedToken(typePropertyKey + i, i))
                .toList());
        return tokenHolder;
    }

    private static Stream<Arguments> indexes() {
        final var counter = new MutableInt();
        return Stream.of(EntityType.NODE, EntityType.RELATIONSHIP)
                .flatMap(entityType -> INDEX_TYPES.entrySet().stream().map(entry -> {
                    final var providerDescriptor = entry.getKey();
                    final var indexType = entry.getValue();
                    final var ruleId = counter.getAndIncrement();
                    IndexPrototype indexPrototype;
                    if (providerDescriptor == TOKEN_DESCRIPTOR) {
                        indexPrototype = forSchema(forAnyEntityTokens(entityType));
                    } else {
                        if (entityType == EntityType.NODE) {
                            indexPrototype = forSchema(forLabel(ruleId + 1, ruleId + 2));
                        } else {
                            indexPrototype = forSchema(forRelType(ruleId + 1, ruleId + 2));
                        }

                        if (providerDescriptor == AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR) {
                            indexPrototype = indexPrototype.withIndexConfig(IndexConfig.with(Map.of(
                                    IndexSetting.vector_Dimensions().getSettingName(),
                                    Values.intValue(666),
                                    IndexSetting.vector_Similarity_Function().getSettingName(),
                                    Values.stringValue("COSINE"))));
                        }
                    }

                    return Arguments.of(indexPrototype
                            .withName("index_" + ruleId)
                            .withIndexProvider(providerDescriptor)
                            .withIndexType(indexType)
                            .materialise(ruleId));
                }));
    }

    private record DuffContext(FileSystemAbstraction fs) implements CreationContext {}
}
