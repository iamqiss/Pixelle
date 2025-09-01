/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.impl.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.Arrays;
import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.procedure.memory.HeapEstimator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

@ExtendWith(RandomExtension.class)
class ProcedureHeapEstimatorTest {
    private final MemoryMeter meter = MemoryMeter.builder().build();
    private final HeapEstimator estimator = ProcedureHeapEstimator.INSTANCE;

    @Inject
    private RandomSupport rand;

    @Test
    void shallowSize() {
        assertThat(estimator.shallowSize(new SomeClass())).isEqualTo(meter.measure(new SomeClass()));
    }

    @Test
    void shallowSizeRandomNeo4jValues() {
        for (final var type : ValueType.values()) {
            final var value = rand.nextValue(type);

            assertThat(estimator.shallowSize(value))
                    .describedAs("value=%s valueClass=%s valueType=%s", value, value.getClass(), type)
                    .isEqualTo(meter.measure(value));
        }
    }

    @Test
    void shallowSizeRandomJavaValues() {
        for (final var type :
                Arrays.stream(ValueType.values()).filter(t -> !t.arrayType).toList()) {
            final var value = rand.nextValue(type).asObjectCopy();

            assertThat(estimator.shallowSize(value))
                    .describedAs("value=%s valueClass=%s valueType=%s", value, value.getClass(), type)
                    .satisfies(est -> {
                        if (type.valueGroup == ValueGroup.BOOLEAN) {
                            assertThat(est).isZero();
                        } else if (type.valueGroup == ValueGroup.TEXT) {
                            assertThat(est).isGreaterThan(0).isCloseTo(meter.measure(value), withinPercentage(500));
                        } else {
                            assertThat(est).isGreaterThan(0).isCloseTo(meter.measure(value), withinPercentage(300));
                        }
                    });
        }
    }

    @Test
    void shallowSizeOfInstance() {
        assertThat(estimator.shallowSizeOfInstance(SomeClass.class)).isEqualTo(meter.measure(new SomeClass()));
    }

    @Test
    void shallowSizeOfObjectArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.shallowSizeOfObjectArray(size)).isEqualTo(meter.measureArray(new Object[size]));
    }

    @Test
    void sizeOfByteArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.sizeOfByteArray(size)).isEqualTo(meter.measureArray(new byte[size]));
    }

    @Test
    void sizeOfIntArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.sizeOfIntArray(size)).isEqualTo(meter.measureArray(new int[size]));
    }

    @Test
    void sizeOfLongArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.sizeOfLongArray(size)).isEqualTo(meter.measureArray(new long[size]));
    }

    @Test
    void sizeOfFloatArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.sizeOfFloatArray(size)).isEqualTo(meter.measureArray(new float[size]));
    }

    @Test
    void sizeOfDoubleArray() {
        final var size = rand.nextInt(8192);
        assertThat(estimator.sizeOfDoubleArray(size)).isEqualTo(meter.measureArray(new double[size]));
    }

    public static class SomeClass {
        long a;
        int b;
        byte c;
        char d;
        double e;
        float f;
        boolean g;
        Long h;
        Integer i;
        Byte j;
        Double k;
        Float l;
        Boolean m;
        Character n;
    }
}
