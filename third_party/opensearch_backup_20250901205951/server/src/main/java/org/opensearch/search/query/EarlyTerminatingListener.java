/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.query;

/**
 * Early termination event listener. It is used during concurrent segment search
 * to propagate the early termination intent.
 *
 * @density.internal
 */
public interface EarlyTerminatingListener {
    /**
     * Early termination event notification
     * @param maxCountHits desired maximum number of hits
     * @param forcedTermination :true" if forced termination has been requested, "false" otherwise
     */
    void onEarlyTermination(int maxCountHits, boolean forcedTermination);
}
