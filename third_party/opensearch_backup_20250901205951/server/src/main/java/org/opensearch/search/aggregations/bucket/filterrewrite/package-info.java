/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * This package contains filter rewrite optimization for range-type aggregations
 * <p>
 * The idea is to
 * <ul>
 * <li> figure out the "ranges" from the aggregation </li>
 * <li> leverage the ranges and bkd index to get the result of each range bucket quickly </li>
 * </ul>
 * More details in https://github.com/density-project/Density/pull/14464
 */
package org.density.search.aggregations.bucket.filterrewrite;
