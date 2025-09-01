
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.codec.composite;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.mapper.CompositeMappedFieldType;

/**
 * Field info details of composite index fields
 *
 * @density.experimental
 */
@ExperimentalApi
public class CompositeIndexFieldInfo {
    private final String field;
    private final CompositeMappedFieldType.CompositeFieldType type;

    public CompositeIndexFieldInfo(String field, CompositeMappedFieldType.CompositeFieldType type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public CompositeMappedFieldType.CompositeFieldType getType() {
        return type;
    }
}
