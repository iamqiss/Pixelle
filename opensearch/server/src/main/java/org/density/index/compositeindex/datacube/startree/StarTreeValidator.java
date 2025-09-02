/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.IndexSettings;
import org.density.index.compositeindex.CompositeIndexSettings;
import org.density.index.compositeindex.datacube.Dimension;
import org.density.index.compositeindex.datacube.Metric;
import org.density.index.mapper.CompositeMappedFieldType;
import org.density.index.mapper.DocCountFieldMapper;
import org.density.index.mapper.MappedFieldType;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.StarTreeMapper;

import java.util.Locale;
import java.util.Set;

/**
 * Validations for star tree fields as part of mappings
 *
 * @density.experimental
 */
@ExperimentalApi
public class StarTreeValidator {
    public static void validate(MapperService mapperService, CompositeIndexSettings compositeIndexSettings, IndexSettings indexSettings) {
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        if (compositeFieldTypes.size() > StarTreeIndexSettings.STAR_TREE_MAX_FIELDS_SETTING.get(indexSettings.getSettings())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Index cannot have more than [%s] star tree fields",
                    StarTreeIndexSettings.STAR_TREE_MAX_FIELDS_SETTING.get(indexSettings.getSettings())
                )
            );
        }
        for (CompositeMappedFieldType compositeFieldType : compositeFieldTypes) {
            if (!(compositeFieldType != null && compositeFieldType.unwrap() instanceof StarTreeMapper.StarTreeFieldType)) {
                continue;
            }

            if (indexSettings.getSettings()
                .getAsBoolean(
                    StarTreeIndexSettings.STAR_TREE_SEARCH_ENABLED_SETTING.getKey(),
                    compositeIndexSettings.isStarTreeIndexCreationEnabled()
                ) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "star tree index cannot be created, enable it using [%s] cluster setting or [%s] index setting",
                        CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(),
                        StarTreeIndexSettings.STAR_TREE_SEARCH_ENABLED_SETTING.getKey()
                    )
                );
            }
            StarTreeMapper.StarTreeFieldType dataCubeFieldType = (StarTreeMapper.StarTreeFieldType) compositeFieldType;
            for (Dimension dim : dataCubeFieldType.getDimensions()) {
                MappedFieldType ft = mapperService.fieldType(dim.getField());
                if (ft == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "unknown dimension field [%s] as part of star tree field", dim.getField())
                    );
                }
                if (ft.isAggregatable() == false) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Aggregations not supported for the dimension field [%s] with field type [%s] as part of star tree field",
                            dim.getField(),
                            ft.typeName()
                        )
                    );
                }
            }
            for (Metric metric : dataCubeFieldType.getMetrics()) {
                MappedFieldType ft = mapperService.fieldType(metric.getField());
                if (ft == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "unknown metric field [%s] as part of star tree field", metric.getField())
                    );
                }
                if (ft.isAggregatable() == false && ft.unwrap() instanceof DocCountFieldMapper.DocCountFieldType == false) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Aggregations not supported for the metrics field [%s] with field type [%s] as part of star tree field",
                            metric.getField(),
                            ft.typeName()
                        )
                    );
                }
            }
        }
    }
}
