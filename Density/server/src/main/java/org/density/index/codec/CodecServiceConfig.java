/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.codec;

import org.apache.logging.log4j.Logger;
import org.density.common.Nullable;
import org.density.index.IndexSettings;
import org.density.index.mapper.MapperService;

import java.util.Objects;

/**
 * The configuration parameters necessary for the {@link CodecService} instance construction.
 *
 * @density.internal
 */
public final class CodecServiceConfig {
    private final IndexSettings indexSettings;
    private final MapperService mapperService;
    private final Logger logger;

    public CodecServiceConfig(IndexSettings indexSettings, @Nullable MapperService mapperService, @Nullable Logger logger) {
        this.indexSettings = Objects.requireNonNull(indexSettings);
        this.mapperService = mapperService;
        this.logger = logger;
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    @Nullable
    public MapperService getMapperService() {
        return mapperService;
    }

    @Nullable
    public Logger getLogger() {
        return logger;
    }
}
