/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.kafka;

import org.density.index.IngestionConsumerFactory;
import org.density.plugins.IngestionConsumerPlugin;
import org.density.plugins.Plugin;

import java.util.Map;

/**
 * A plugin for ingestion source of Kafka.
 */
public class KafkaPlugin extends Plugin implements IngestionConsumerPlugin {
    /**
     * The type of the ingestion source.
     */
    public static final String TYPE = "KAFKA";

    /**
     * Constructor.
     */
    public KafkaPlugin() {}

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new KafkaConsumerFactory());
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
