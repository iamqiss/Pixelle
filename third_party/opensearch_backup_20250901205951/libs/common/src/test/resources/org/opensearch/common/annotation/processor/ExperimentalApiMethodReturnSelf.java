/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.annotation.processor;

import org.density.common.annotation.ExperimentalApi;

@ExperimentalApi
public class ExperimentalApiMethodReturnSelf {
    public ExperimentalApiMethodReturnSelf method() {
        return new ExperimentalApiMethodReturnSelf();
    }
}
