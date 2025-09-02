/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.mapper;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.Dimension;
import org.density.index.compositeindex.datacube.Metric;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for multi field data cube fields
 *
 * @density.experimental
 */
@ExperimentalApi
public abstract class CompositeDataCubeFieldType extends CompositeMappedFieldType {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    private final List<Dimension> dimensions;
    private final List<Metric> metrics;

    public CompositeDataCubeFieldType(String name, List<Dimension> dims, List<Metric> metrics, CompositeFieldType type) {
        super(name, getFields(dims, metrics), type);
        this.dimensions = dims;
        this.metrics = metrics;
    }

    private static List<String> getFields(List<Dimension> dims, List<Metric> metrics) {
        Set<String> fields = new HashSet<>();
        for (Dimension dim : dims) {
            fields.add(dim.getField());
        }
        for (Metric metric : metrics) {
            fields.add(metric.getField());
        }
        return new ArrayList<>(fields);
    }

    public List<Dimension> getDimensions() {
        return dimensions;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }
}
