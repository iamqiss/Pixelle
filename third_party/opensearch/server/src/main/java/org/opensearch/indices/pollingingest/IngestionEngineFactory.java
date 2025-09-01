/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.pollingingest;

import org.density.index.IngestionConsumerFactory;
import org.density.index.engine.Engine;
import org.density.index.engine.EngineConfig;
import org.density.index.engine.EngineFactory;
import org.density.index.engine.IngestionEngine;
import org.density.index.engine.NRTReplicationEngine;

import java.util.Objects;

/**
 * Engine Factory implementation used with streaming ingestion.
 */
public class IngestionEngineFactory implements EngineFactory {

    private final IngestionConsumerFactory ingestionConsumerFactory;

    public IngestionEngineFactory(IngestionConsumerFactory ingestionConsumerFactory) {
        this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        if (config.isReadOnlyReplica()) {
            return new NRTReplicationEngine(config);
        }

        IngestionEngine ingestionEngine = new IngestionEngine(config, ingestionConsumerFactory);
        ingestionEngine.start();
        return ingestionEngine;
    }
}
