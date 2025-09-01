/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index;

import org.density.common.unit.TimeValue;
import org.density.common.util.concurrent.AbstractAsyncTask;

public final class IndexServiceTestUtils {
    private IndexServiceTestUtils() {}

    public static void setTrimTranslogTaskInterval(IndexService indexService, TimeValue interval) {
        ((AbstractAsyncTask) indexService.getTrimTranslogTask()).setInterval(interval);
    }
}
