/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations.bucket.missing;

import org.density.common.inject.Provider;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Composite Aggregation Missing bucket order.
 *
 * @density.internal
 */
public enum MissingOrder implements Writeable {
    /**
     * missing first.
     */
    FIRST {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : -1;
            } else if (rightIsMissing.get()) {
                return 1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "first";
        }
    },

    /**
     * missing last.
     */
    LAST {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : 1;
            } else if (rightIsMissing.get()) {
                return -1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "last";
        }
    },

    /**
     * Default: ASC missing first / DESC missing last
     */
    DEFAULT {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : -1 * reverseMul;
            } else if (rightIsMissing.get()) {
                return reverseMul;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "default";
        }
    };

    public static final String NAME = "missing_order";

    private static int MISSING_ORDER_UNKNOWN = Integer.MIN_VALUE;

    public static MissingOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(MissingOrder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static boolean isDefault(MissingOrder order) {
        return order == DEFAULT;
    }

    public static MissingOrder fromString(String order) {
        return valueOf(order.toUpperCase(Locale.ROOT));
    }

    public static boolean unknownOrder(int v) {
        return v == MISSING_ORDER_UNKNOWN;
    }

    public abstract int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul);
}
