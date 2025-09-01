/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density;

import java.io.IOException;

/**
 * This exception is thrown when Density detects
 * an inconsistency in one of it's persistent files.
 *
 * @density.internal
 */
public class DensityCorruptionException extends IOException {

    /**
     * Creates a new {@link DensityCorruptionException}
     * @param message the exception message.
     */
    public DensityCorruptionException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link DensityCorruptionException} with the given exceptions stacktrace.
     * This constructor copies the stacktrace as well as the message from the given
     * {@code Throwable} into this exception.
     *
     * @param ex the exception cause
     */
    public DensityCorruptionException(Throwable ex) {
        super(ex);
    }
}
