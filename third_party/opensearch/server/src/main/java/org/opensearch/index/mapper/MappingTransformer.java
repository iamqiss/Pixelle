/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.mapper;

import org.density.core.action.ActionListener;

import java.util.Map;

import reactor.util.annotation.NonNull;

/**
 * A transformer to allow plugins to implement logic to transform the index mapping during
 * index creation/update and index template creation/update on transport layer.
 *
 */
public interface MappingTransformer {
    default void transform(
        final Map<String, Object> mapping,
        final TransformContext context,
        @NonNull final ActionListener<Void> listener
    ) {
        listener.onResponse(null);
    }

    /**
     * Context for mapping transform. For now, we don't need any context, but it's defined for future scalability.
     * It can be used to provide the info like we are transforming the mapping for what transport action. Or provide
     * index setting info to help transform the mapping.
     */
    class TransformContext {}
}
