/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.translog;

import org.density.common.annotation.PublicApi;
import org.density.index.IndexSettings;
import org.density.index.seqno.RetentionLeases;

import java.util.function.Supplier;

/**
 * Factory to instantiate a translog deletion policy
 *
 * @density.api
 */
@FunctionalInterface
@PublicApi(since = "1.0.0")
public interface TranslogDeletionPolicyFactory {
    TranslogDeletionPolicy create(IndexSettings settings, Supplier<RetentionLeases> supplier);
}
