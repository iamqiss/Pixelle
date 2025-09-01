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

package org.density.indices;

import org.density.action.admin.indices.rollover.Condition;
import org.density.action.admin.indices.rollover.MaxAgeCondition;
import org.density.action.admin.indices.rollover.MaxDocsCondition;
import org.density.action.admin.indices.rollover.MaxSizeCondition;
import org.density.action.resync.TransportResyncReplicationAction;
import org.density.common.inject.AbstractModule;
import org.density.core.ParseField;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.NamedWriteableRegistry.Entry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.index.SegmentReplicationPressureService;
import org.density.index.mapper.BinaryFieldMapper;
import org.density.index.mapper.BooleanFieldMapper;
import org.density.index.mapper.CompletionFieldMapper;
import org.density.index.mapper.ConstantKeywordFieldMapper;
import org.density.index.mapper.DataStreamFieldMapper;
import org.density.index.mapper.DateFieldMapper;
import org.density.index.mapper.DerivedFieldMapper;
import org.density.index.mapper.DocCountFieldMapper;
import org.density.index.mapper.FieldAliasMapper;
import org.density.index.mapper.FieldNamesFieldMapper;
import org.density.index.mapper.FlatObjectFieldMapper;
import org.density.index.mapper.GeoPointFieldMapper;
import org.density.index.mapper.IdFieldMapper;
import org.density.index.mapper.IgnoredFieldMapper;
import org.density.index.mapper.IndexFieldMapper;
import org.density.index.mapper.IpFieldMapper;
import org.density.index.mapper.KeywordFieldMapper;
import org.density.index.mapper.Mapper;
import org.density.index.mapper.MatchOnlyTextFieldMapper;
import org.density.index.mapper.MetadataFieldMapper;
import org.density.index.mapper.NestedPathFieldMapper;
import org.density.index.mapper.NumberFieldMapper;
import org.density.index.mapper.ObjectMapper;
import org.density.index.mapper.RangeType;
import org.density.index.mapper.RoutingFieldMapper;
import org.density.index.mapper.SemanticVersionFieldMapper;
import org.density.index.mapper.SeqNoFieldMapper;
import org.density.index.mapper.SourceFieldMapper;
import org.density.index.mapper.StarTreeMapper;
import org.density.index.mapper.TextFieldMapper;
import org.density.index.mapper.VersionFieldMapper;
import org.density.index.mapper.WildcardFieldMapper;
import org.density.index.remote.RemoteStorePressureService;
import org.density.index.seqno.GlobalCheckpointSyncAction;
import org.density.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.density.index.seqno.RetentionLeaseSyncAction;
import org.density.index.seqno.RetentionLeaseSyncer;
import org.density.index.shard.PrimaryReplicaSyncer;
import org.density.indices.cluster.IndicesClusterStateService;
import org.density.indices.mapper.MapperRegistry;
import org.density.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.density.indices.store.IndicesStore;
import org.density.indices.store.TransportNodesListShardStoreMetadata;
import org.density.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.density.plugins.MapperPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Configures classes and services that are shared by indices on each node.
 *
 * @density.internal
 */
public class IndicesModule extends AbstractModule {
    private final List<Entry> namedWritables = new ArrayList<>();
    private final MapperRegistry mapperRegistry;

    public IndicesModule(List<MapperPlugin> mapperPlugins) {
        this.mapperRegistry = new MapperRegistry(
            getMappers(mapperPlugins),
            getMetadataMappers(mapperPlugins),
            getFieldFilter(mapperPlugins)
        );
        registerBuiltinWritables();
    }

    private void registerBuiltinWritables() {
        namedWritables.add(new NamedWriteableRegistry.Entry(Condition.class, MaxAgeCondition.NAME, MaxAgeCondition::new));
        namedWritables.add(new NamedWriteableRegistry.Entry(Condition.class, MaxDocsCondition.NAME, MaxDocsCondition::new));
        namedWritables.add(new NamedWriteableRegistry.Entry(Condition.class, MaxSizeCondition.NAME, MaxSizeCondition::new));
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return namedWritables;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxAgeCondition.NAME),
                (p, c) -> MaxAgeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxDocsCondition.NAME),
                (p, c) -> MaxDocsCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxSizeCondition.NAME),
                (p, c) -> MaxSizeCondition.fromXContent(p)
            )
        );
    }

    public static Map<String, Mapper.TypeParser> getMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();

        // builtin mappers
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            mappers.put(type.typeName(), type.parser());
        }
        for (RangeType type : RangeType.values()) {
            mappers.put(type.typeName(), type.parser());
        }
        mappers.put(BooleanFieldMapper.CONTENT_TYPE, BooleanFieldMapper.PARSER);
        mappers.put(BinaryFieldMapper.CONTENT_TYPE, BinaryFieldMapper.PARSER);
        DateFieldMapper.Resolution milliseconds = DateFieldMapper.Resolution.MILLISECONDS;
        mappers.put(milliseconds.type(), DateFieldMapper.MILLIS_PARSER);
        DateFieldMapper.Resolution nanoseconds = DateFieldMapper.Resolution.NANOSECONDS;
        mappers.put(nanoseconds.type(), DateFieldMapper.NANOS_PARSER);
        mappers.put(IpFieldMapper.CONTENT_TYPE, IpFieldMapper.PARSER);
        mappers.put(TextFieldMapper.CONTENT_TYPE, TextFieldMapper.PARSER);
        mappers.put(MatchOnlyTextFieldMapper.CONTENT_TYPE, MatchOnlyTextFieldMapper.PARSER);
        mappers.put(KeywordFieldMapper.CONTENT_TYPE, KeywordFieldMapper.PARSER);
        mappers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(CompletionFieldMapper.CONTENT_TYPE, CompletionFieldMapper.PARSER);
        mappers.put(FieldAliasMapper.CONTENT_TYPE, new FieldAliasMapper.TypeParser());
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());
        mappers.put(FlatObjectFieldMapper.CONTENT_TYPE, FlatObjectFieldMapper.PARSER);
        mappers.put(ConstantKeywordFieldMapper.CONTENT_TYPE, new ConstantKeywordFieldMapper.TypeParser());
        mappers.put(DerivedFieldMapper.CONTENT_TYPE, DerivedFieldMapper.PARSER);
        mappers.put(WildcardFieldMapper.CONTENT_TYPE, WildcardFieldMapper.PARSER);
        mappers.put(StarTreeMapper.CONTENT_TYPE, new StarTreeMapper.TypeParser());
        mappers.put(SemanticVersionFieldMapper.CONTENT_TYPE, SemanticVersionFieldMapper.PARSER);

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, Mapper.TypeParser> entry : mapperPlugin.getMappers().entrySet()) {
                if (mappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Mapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(mappers);
    }

    private static final Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers = initBuiltInMetadataMappers();

    private static Set<String> builtInMetadataFields = Collections.unmodifiableSet(builtInMetadataMappers.keySet());

    private static Map<String, MetadataFieldMapper.TypeParser> initBuiltInMetadataMappers() {
        Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers;
        // Use a LinkedHashMap for metadataMappers because iteration order matters
        builtInMetadataMappers = new LinkedHashMap<>();
        // _ignored first so that we always load it, even if only _id is requested
        builtInMetadataMappers.put(IgnoredFieldMapper.NAME, IgnoredFieldMapper.PARSER);
        // ID second so it will be the first (if no ignored fields) stored field to load
        // (so will benefit from "fields: []" early termination
        builtInMetadataMappers.put(IdFieldMapper.NAME, IdFieldMapper.PARSER);
        builtInMetadataMappers.put(RoutingFieldMapper.NAME, RoutingFieldMapper.PARSER);
        builtInMetadataMappers.put(IndexFieldMapper.NAME, IndexFieldMapper.PARSER);
        builtInMetadataMappers.put(DataStreamFieldMapper.NAME, DataStreamFieldMapper.PARSER);
        builtInMetadataMappers.put(SourceFieldMapper.NAME, SourceFieldMapper.PARSER);
        builtInMetadataMappers.put(NestedPathFieldMapper.NAME, NestedPathFieldMapper.PARSER);
        builtInMetadataMappers.put(VersionFieldMapper.NAME, VersionFieldMapper.PARSER);
        builtInMetadataMappers.put(SeqNoFieldMapper.NAME, SeqNoFieldMapper.PARSER);
        builtInMetadataMappers.put(DocCountFieldMapper.NAME, DocCountFieldMapper.PARSER);
        // _field_names must be added last so that it has a chance to see all the other mappers
        builtInMetadataMappers.put(FieldNamesFieldMapper.NAME, FieldNamesFieldMapper.PARSER);
        return Collections.unmodifiableMap(builtInMetadataMappers);
    }

    public static Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, MetadataFieldMapper.TypeParser> metadataMappers = new LinkedHashMap<>();

        int i = 0;
        Map.Entry<String, MetadataFieldMapper.TypeParser> fieldNamesEntry = null;
        for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : builtInMetadataMappers.entrySet()) {
            if (i < builtInMetadataMappers.size() - 1) {
                metadataMappers.put(entry.getKey(), entry.getValue());
            } else {
                assert entry.getKey().equals(FieldNamesFieldMapper.NAME) : "_field_names must be the last registered mapper, order counts";
                fieldNamesEntry = entry;
            }
            i++;
        }
        assert fieldNamesEntry != null;

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : mapperPlugin.getMetadataMappers().entrySet()) {
                if (entry.getKey().equals(FieldNamesFieldMapper.NAME)) {
                    throw new IllegalArgumentException("Plugin cannot contain metadata mapper [" + FieldNamesFieldMapper.NAME + "]");
                }
                if (metadataMappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("MetadataFieldMapper [" + entry.getKey() + "] is already registered");
                }
            }
        }

        // we register _field_names here so that it has a chance to see all the other mappers, including from plugins
        metadataMappers.put(fieldNamesEntry.getKey(), fieldNamesEntry.getValue());
        return Collections.unmodifiableMap(metadataMappers);
    }

    /**
     * Returns a set containing all of the builtin metadata fields
     */
    public static Set<String> getBuiltInMetadataFields() {
        return builtInMetadataFields;
    }

    private static Function<String, Predicate<String>> getFieldFilter(List<MapperPlugin> mapperPlugins) {
        Function<String, Predicate<String>> fieldFilter = MapperPlugin.NOOP_FIELD_FILTER;
        for (MapperPlugin mapperPlugin : mapperPlugins) {
            fieldFilter = and(fieldFilter, mapperPlugin.getFieldFilter());
        }
        return fieldFilter;
    }

    private static Function<String, Predicate<String>> and(
        Function<String, Predicate<String>> first,
        Function<String, Predicate<String>> second
    ) {
        // the purpose of this method is to not chain no-op field predicates, so that we can easily find out when no plugins plug in
        // a field filter, hence skip the mappings filtering part as a whole, as it requires parsing mappings into a map.
        if (first == MapperPlugin.NOOP_FIELD_FILTER) {
            return second;
        }
        if (second == MapperPlugin.NOOP_FIELD_FILTER) {
            return first;
        }
        return index -> {
            Predicate<String> firstPredicate = first.apply(index);
            Predicate<String> secondPredicate = second.apply(index);
            if (firstPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
                return secondPredicate;
            }
            if (secondPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
                return firstPredicate;
            }
            return firstPredicate.and(secondPredicate);
        };
    }

    @Override
    protected void configure() {
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetadata.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetadataBatch.class).asEagerSingleton();
        bind(GlobalCheckpointSyncAction.class).asEagerSingleton();
        bind(TransportResyncReplicationAction.class).asEagerSingleton();
        bind(PrimaryReplicaSyncer.class).asEagerSingleton();
        bind(RetentionLeaseSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseBackgroundSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseSyncer.class).asEagerSingleton();
        bind(SegmentReplicationCheckpointPublisher.class).asEagerSingleton();
        bind(SegmentReplicationPressureService.class).asEagerSingleton();
        bind(RemoteStorePressureService.class).asEagerSingleton();
    }

    /**
     * A registry for all field mappers.
     */
    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }
}
