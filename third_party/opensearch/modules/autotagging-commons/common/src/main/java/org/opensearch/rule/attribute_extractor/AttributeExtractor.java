/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.attribute_extractor;

import org.density.rule.autotagging.Attribute;

/**
 * This interface defines the contract for extracting the attributes for Rule based auto-tagging feature
 * @param <V>
 */
public interface AttributeExtractor<V> {
    /**
     * This method returns the Attribute which it is responsible for extracting
     * @return attribute
     */
    Attribute getAttribute();

    /**
     * This method returns the attribute values in context of the current request
     * @return attribute value
     */
    Iterable<V> extract();
}
