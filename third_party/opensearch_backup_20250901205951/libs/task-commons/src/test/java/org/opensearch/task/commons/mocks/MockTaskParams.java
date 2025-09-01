/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.mocks;

import org.density.task.commons.task.TaskParams;

public class MockTaskParams extends TaskParams {

    private final String value;

    public MockTaskParams(String mockValue) {
        super();
        value = mockValue;
    }

    public String getValue() {
        return value;
    }
}
