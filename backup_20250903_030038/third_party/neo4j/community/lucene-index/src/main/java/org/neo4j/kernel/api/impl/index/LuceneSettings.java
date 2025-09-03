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
package org.neo4j.kernel.api.impl.index;

import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.INT;

import org.apache.lucene.index.IndexWriterConfig;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
public class LuceneSettings implements SettingsDeclaration {
    @Internal
    @Description("Determines the minimal number of documents required before the buffered in-memory documents are "
            + "flushed as a new Segment. Large values generally give faster indexing.")
    public static final Setting<Integer> lucene_writer_max_buffered_docs = newBuilder(
                    "internal.dbms.index.lucene.writer_max_buffered_docs", INT, 100000)
            .build();

    @Internal
    @Description("Determines the minimal number of documents required before the buffered in-memory documents are "
            + "flushed as a new Segment during population. Large values generally give faster indexing.")
    public static final Setting<Integer> lucene_population_max_buffered_docs = newBuilder(
                    "internal.dbms.index.lucene.population_max_buffered_docs",
                    INT,
                    // Pass in DISABLE_AUTO_FLUSH to prevent triggering a flush due to number of buffered documents
                    IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster.")
    public static final Setting<Integer> lucene_merge_factor =
            newBuilder("internal.dbms.index.lucene.merge_factor", INT, 2).build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster. "
            + "This is only used on vector indexes after they have been created.")
    public static final Setting<Integer> vector_standard_merge_factor = newBuilder(
                    "internal.dbms.index.vector.standard_merge_factor", INT, 50)
            .build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster. "
            + "This is only used on vector indexes during initial creation.")
    public static final Setting<Integer> vector_population_merge_factor = newBuilder(
                    "internal.dbms.index.vector.population_merge_factor", INT, 1000)
            .build();

    @Internal
    @Description("If a merged segment will be more than this percentage of the total size of the index, "
            + "leave the segment as non-compound file even if compound file is enabled. "
            + "Set to 1.0 to always use CFS regardless of merge size.")
    public static final Setting<Double> lucene_nocfs_ratio =
            newBuilder("internal.dbms.index.lucene.nocfs.ratio", DOUBLE, 1.0).build();

    @Internal
    @Description("Sets the minimum size for the lowest level segments. Any segments below this size are "
            + "candidates for full-flush merges and be merged more aggressively in order to avoid having "
            + "a long tail of small segments. Large values of this parameter increase the merging cost during "
            + "indexing if you flush small segments")
    public static final Setting<Double> lucene_min_merge =
            newBuilder("internal.dbms.index.lucene.min_merge", DOUBLE, 0.1).build();

    @Internal
    @Description("Determines the largest segment (measured by total byte size of the segment's files, in MB) "
            + "that may be merged with other segments. Small values (e. g., less than 50 MB) are best for interactive "
            + "indexing, as this limits the length of pauses while indexing to a few seconds. Larger values are best "
            + "for batched indexing and speedier searches.")
    public static final Setting<Double> lucene_max_merge =
            newBuilder("internal.dbms.index.lucene.max_merge", DOUBLE, 2048D).build();

    @Internal
    @Description("Determines the amount of RAM that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can.")
    public static final Setting<Double> lucene_standard_ram_buffer_size = newBuilder(
                    "internal.dbms.index.lucene.standard_ram_buffer_size",
                    DOUBLE,
                    IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB)
            .build();

    @Internal
    @Description("Determines the amount of RAM that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can. "
            + "This is only used during the creation of the index.")
    public static final Setting<Double> lucene_population_ram_buffer_size = newBuilder(
                    "internal.dbms.index.lucene.population_ram_buffer_size", DOUBLE, 50D)
            .build();

    @Internal
    @Description("Determines the amount of RAM that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can. "
            + "This is only used for vector indexes during the creation of the index.")
    public static final Setting<Double> vector_population_ram_buffer_size = newBuilder(
                    "internal.dbms.index.vector.population_ram_buffer_size", DOUBLE, 1D)
            .build();

    @Internal
    @Description("Sets the merge scheduler used by this writer. The default is ConcurrentMergeScheduler. "
            + "If 'false' separate threads will be used for merge, if 'true' the merges will be done "
            + "sequentially by the current thread.")
    public static final Setting<Boolean> lucene_population_serial_merge_scheduler = newBuilder(
                    "internal.dbms.index.lucene.population_serial_merge_scheduler", BOOL, TRUE)
            .build();
}
