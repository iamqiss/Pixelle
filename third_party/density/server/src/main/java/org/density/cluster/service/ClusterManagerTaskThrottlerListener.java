/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.service;

/**
 * Listener interface for master task throttling
 */
public interface ClusterManagerTaskThrottlerListener {
    void onThrottle(String type, final int counts);
}
