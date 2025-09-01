/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Query rewriting optimizations for improving search performance.
 *
 * <p>This package contains various query rewriters that transform queries
 * into more efficient forms while maintaining semantic equivalence.
 *
 * <p>The rewriters include:
 * <ul>
 *   <li>{@link org.density.search.query.rewriters.BooleanFlatteningRewriter} -
 *       Flattens nested boolean queries with single clauses</li>
 *   <li>{@link org.density.search.query.rewriters.MatchAllRemovalRewriter} -
 *       Removes redundant match_all queries from boolean clauses</li>
 *   <li>{@link org.density.search.query.rewriters.TermsMergingRewriter} -
 *       Merges multiple term queries on the same field into a single terms query</li>
 *   <li>{@link org.density.search.query.rewriters.MustNotToShouldRewriter} -
 *       Transforms must_not queries to should queries for better performance</li>
 *   <li>{@link org.density.search.query.rewriters.MustToFilterRewriter} -
 *       Moves scoring-irrelevant queries from must to filter clauses</li>
 * </ul>
 *
 * @density.internal
 */
package org.density.search.query.rewriters;
