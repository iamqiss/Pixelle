/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.wlm.MutableWorkloadGroupFragment;

public class WorkloadGroupActionTestUtils {
    public static UpdateWorkloadGroupRequest updateWorkloadGroupRequest(
        String name,
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment
    ) {
        return new UpdateWorkloadGroupRequest(name, mutableWorkloadGroupFragment);
    }
}
