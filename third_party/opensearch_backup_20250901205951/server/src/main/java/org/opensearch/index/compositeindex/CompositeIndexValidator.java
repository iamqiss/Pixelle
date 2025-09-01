/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.IndexSettings;
import org.density.index.compositeindex.datacube.startree.StarTreeValidator;
import org.density.index.mapper.MapperService;

import java.util.Locale;

/**
 * Validation for composite indices as part of mappings
 *
 * @density.experimental
 */
@ExperimentalApi
public class CompositeIndexValidator {

    public static void validate(MapperService mapperService, CompositeIndexSettings compositeIndexSettings, IndexSettings indexSettings) {
        StarTreeValidator.validate(mapperService, compositeIndexSettings, indexSettings);
    }

    public static void validate(
        MapperService mapperService,
        CompositeIndexSettings compositeIndexSettings,
        IndexSettings indexSettings,
        boolean isCompositeFieldPresent
    ) {
        if (!isCompositeFieldPresent && mapperService.isCompositeIndexPresent()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Composite fields must be specified during index creation, addition of new composite fields during update is not supported"
                )
            );
        }
        StarTreeValidator.validate(mapperService, compositeIndexSettings, indexSettings);
    }
}
