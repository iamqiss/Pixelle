/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.startree.StarTreeField;
import org.density.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.density.index.mapper.CompositeMappedFieldType;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.StarTreeMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builder to construct star-trees based on multiple star-tree fields.
 *
 * @density.experimental
 */
@ExperimentalApi
public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);

    private final List<StarTreeField> starTreeFields;
    private final SegmentWriteState state;
    private final MapperService mapperService;
    private AtomicInteger fieldNumberAcrossStarTrees;

    public StarTreesBuilder(SegmentWriteState segmentWriteState, MapperService mapperService, AtomicInteger fieldNumberAcrossStarTrees) {
        List<StarTreeField> starTreeFields = new ArrayList<>();
        for (CompositeMappedFieldType compositeMappedFieldType : mapperService.getCompositeFieldTypes()) {
            if (compositeMappedFieldType != null && compositeMappedFieldType.unwrap() instanceof StarTreeMapper.StarTreeFieldType) {
                StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) compositeMappedFieldType;
                starTreeFields.add(
                    new StarTreeField(
                        starTreeFieldType.name(),
                        starTreeFieldType.getDimensions(),
                        starTreeFieldType.getMetrics(),
                        starTreeFieldType.getStarTreeConfig()
                    )
                );
            }
        }
        this.starTreeFields = starTreeFields;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.fieldNumberAcrossStarTrees = fieldNumberAcrossStarTrees;
    }

    /**
     * Builds all star-trees for given star-tree fields.
     *
     * @param metaOut                      an IndexInput for star-tree metadata
     * @param dataOut                      an IndexInput for star-tree data
     * @param fieldProducerMap             fetches iterators for the fields (dimensions and metrics)
     * @param starTreeDocValuesConsumer    a consumer to write star-tree doc values
     * @throws IOException                 when an error occurs while building the star-trees
     */
    public void build(
        IndexOutput metaOut,
        IndexOutput dataOut,
        Map<String, DocValuesProducer> fieldProducerMap,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        if (starTreeFields.isEmpty()) {
            logger.debug("no star-tree fields found, returning from star-tree builder");
            return;
        }
        long startTime = System.currentTimeMillis();

        int numStarTrees = starTreeFields.size();
        logger.debug("Starting building {} star-trees with star-tree fields", numStarTrees);

        // Build all star-trees
        for (StarTreeField starTreeField : starTreeFields) {
            try (StarTreeBuilder starTreeBuilder = getStarTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService)) {
                starTreeBuilder.build(fieldProducerMap, fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
            }
        }
        logger.debug("Took {} ms to build {} star-trees with star-tree fields", System.currentTimeMillis() - startTime, numStarTrees);
    }

    @Override
    public void close() throws IOException {
        // TODO : close files
    }

    /**
     * Merges star tree fields from multiple segments
     *
     * @param metaOut                    an IndexInput for star-tree metadata
     * @param dataOut                    an IndexInput for star-tree data
     * @param starTreeValuesSubsPerField starTreeValuesSubs per field
     * @param starTreeDocValuesConsumer  a consumer to write star-tree doc values
     */
    public void buildDuringMerge(
        IndexOutput metaOut,
        IndexOutput dataOut,
        final Map<String, List<StarTreeValues>> starTreeValuesSubsPerField,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        logger.debug("Starting merge of {} star-trees with star-tree fields", starTreeValuesSubsPerField.size());
        long startTime = System.currentTimeMillis();
        for (Map.Entry<String, List<StarTreeValues>> entry : starTreeValuesSubsPerField.entrySet()) {
            List<StarTreeValues> starTreeValuesList = entry.getValue();
            if (starTreeValuesList.isEmpty()) {
                logger.debug("StarTreeValues is empty for all segments for field : {}", entry.getKey());
                continue;
            }
            StarTreeField starTreeField = starTreeValuesList.get(0).getStarTreeField();
            try (StarTreeBuilder builder = getStarTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService)) {
                builder.build(starTreeValuesList, fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
            }
        }
        logger.debug(
            "Took {} ms to merge {} star-trees with star-tree fields",
            System.currentTimeMillis() - startTime,
            starTreeValuesSubsPerField.size()
        );
    }

    /**
     * Get star-tree builder based on build mode.
     */
    StarTreeBuilder getStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (starTreeField.getStarTreeConfig().getBuildMode()) {
            case ON_HEAP:
                return new OnHeapStarTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService);
            case OFF_HEAP:
                return new OffHeapStarTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No star tree implementation is available for [%s] build mode",
                        starTreeField.getStarTreeConfig().getBuildMode()
                    )
                );
        }
    }
}
