/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.profile.fetch;

import org.density.common.annotation.ExperimentalApi;
import org.density.search.profile.AbstractProfileBreakdown;
import org.density.search.profile.ProfileMetricUtil;

/**
 * A record of timings for the various operations that may happen during fetch execution.
 */
@ExperimentalApi()
public class FetchProfileBreakdown extends AbstractProfileBreakdown {
    public FetchProfileBreakdown() {
        super(ProfileMetricUtil.getFetchProfileMetrics());
    }
}
