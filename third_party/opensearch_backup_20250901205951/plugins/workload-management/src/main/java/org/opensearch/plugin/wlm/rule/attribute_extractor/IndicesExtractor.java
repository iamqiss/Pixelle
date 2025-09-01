/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule.attribute_extractor;

import org.density.action.IndicesRequest;
import org.density.rule.RuleAttribute;
import org.density.rule.attribute_extractor.AttributeExtractor;
import org.density.rule.autotagging.Attribute;

import java.util.List;

/**
 * This class extracts the indices from a request
 */
public class IndicesExtractor implements AttributeExtractor<String> {
    private final IndicesRequest indicesRequest;

    /**
     * Default constructor
     * @param indicesRequest
     */
    public IndicesExtractor(IndicesRequest indicesRequest) {
        this.indicesRequest = indicesRequest;
    }

    @Override
    public Attribute getAttribute() {
        return RuleAttribute.INDEX_PATTERN;
    }

    @Override
    public Iterable<String> extract() {
        return List.of(indicesRequest.indices());
    }
}
