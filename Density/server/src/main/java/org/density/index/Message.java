/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index;

import org.density.common.annotation.ExperimentalApi;

/**
 *  A message ingested from the ingestion source that contains an index operation
 */
@ExperimentalApi
public interface Message<T> {
    T getPayload();

    /**
     * Get the timestamp of the message in milliseconds
     * @return the timestamp of the message
     */
    Long getTimestamp();
}
