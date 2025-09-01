/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.common;

/**
 * Enumerates the types of replication strategies supported by Density.
 * For more information, see https://github.com/density-project/Density/issues/1694
 *
 * @density.internal
 */
public enum ReplicationType {

    DOCUMENT,
    SEGMENT;

    public static ReplicationType parseString(String replicationType) {
        try {
            return ReplicationType.valueOf(replicationType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not parse ReplicationStrategy for [" + replicationType + "]");
        } catch (NullPointerException npe) {
            // return a default value for null input
            return DOCUMENT;
        }
    }
}
