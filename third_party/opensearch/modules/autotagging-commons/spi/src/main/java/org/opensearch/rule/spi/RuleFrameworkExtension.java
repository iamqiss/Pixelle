/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.spi;

import org.density.rule.RulePersistenceService;
import org.density.rule.RuleRoutingService;
import org.density.rule.autotagging.FeatureType;

import java.util.function.Supplier;

/**
 * This interface exposes methods for the RuleFrameworkPlugin to extract framework related
 * implementations from the consumer plugins of this extension
 */
public interface RuleFrameworkExtension {
    /**
     * This method is used to flow implementation from consumer plugins into framework plugin
     * @return the plugin specific implementation of RulePersistenceService
     */
    Supplier<RulePersistenceService> getRulePersistenceServiceSupplier();

    /**
     * This method is used to flow implementation from consumer plugins into framework plugin
     * @return the plugin specific implementation of RuleRoutingService
     */
    Supplier<RuleRoutingService> getRuleRoutingServiceSupplier();

    /**
     * Flow implementation from consumer plugins into framework plugin
     * @return the specific implementation of FeatureType
     */
    Supplier<FeatureType> getFeatureTypeSupplier();
}
