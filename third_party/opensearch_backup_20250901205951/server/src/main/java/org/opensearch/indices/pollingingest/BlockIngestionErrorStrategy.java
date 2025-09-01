/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.pollingingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This error handling strategy blocks on failures preventing processing of remaining updates in the ingestion source.
 */
public class BlockIngestionErrorStrategy implements IngestionErrorStrategy {
    private static final Logger logger = LogManager.getLogger(BlockIngestionErrorStrategy.class);
    private static final String NAME = "BLOCK";
    private final String ingestionSource;

    public BlockIngestionErrorStrategy(String ingestionSource) {
        this.ingestionSource = ingestionSource;
    }

    @Override
    public void handleError(Throwable e, ErrorStage stage) {
        logger.error("Error processing update from {}: {}", ingestionSource, e);

        // todo: record blocking update and emit metrics
    }

    @Override
    public boolean shouldIgnoreError(Throwable e, ErrorStage stage) {
        return false;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
