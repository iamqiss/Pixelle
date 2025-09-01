/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.annotation.processor;

import org.density.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class PublicApiWithProtectedInterface {
    public void method(ProtectedInterface iface) {}

    /**
     * The type could expose protected inner types which are still considered to be a public API when used
     */
    @PublicApi(since = "1.0.0")
    protected interface ProtectedInterface {}
}
