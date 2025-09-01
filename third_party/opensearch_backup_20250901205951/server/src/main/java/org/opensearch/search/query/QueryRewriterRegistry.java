/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryShardContext;
import org.density.search.SearchService;
import org.density.search.query.rewriters.BooleanFlatteningRewriter;
import org.density.search.query.rewriters.MatchAllRemovalRewriter;
import org.density.search.query.rewriters.MustNotToShouldRewriter;
import org.density.search.query.rewriters.MustToFilterRewriter;
import org.density.search.query.rewriters.TermsMergingRewriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for query rewriters
 *
 * @density.internal
 */
public final class QueryRewriterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryRewriterRegistry.class);

    public static final QueryRewriterRegistry INSTANCE = new QueryRewriterRegistry();

    /**
     * Default rewriters.
     * CopyOnWriteArrayList is used for thread-safety during registration.
     */
    private final CopyOnWriteArrayList<QueryRewriter> rewriters;

    /**
     * Whether query rewriting is enabled.
     */
    private volatile boolean enabled;

    private QueryRewriterRegistry() {
        this.rewriters = new CopyOnWriteArrayList<>();

        // Register default rewriters using singletons
        registerRewriter(BooleanFlatteningRewriter.INSTANCE);
        registerRewriter(MustToFilterRewriter.INSTANCE);
        registerRewriter(MustNotToShouldRewriter.INSTANCE);
        registerRewriter(MatchAllRemovalRewriter.INSTANCE);
        registerRewriter(TermsMergingRewriter.INSTANCE);
    }

    /**
     * Register a custom query rewriter.
     *
     * @param rewriter The rewriter to register
     */
    public void registerRewriter(QueryRewriter rewriter) {
        if (rewriter != null) {
            rewriters.add(rewriter);
            logger.info("Registered query rewriter: {}", rewriter.name());
        }
    }

    /**
     * Initialize the registry with cluster settings.
     * This must be called once during system startup to properly configure
     * the TermsMergingRewriter with settings and update consumers.
     *
     * @param settings Initial cluster settings
     * @param clusterSettings Cluster settings for registering update consumers
     */
    public void initialize(Settings settings, ClusterSettings clusterSettings) {
        TermsMergingRewriter.INSTANCE.initialize(settings, clusterSettings);
        this.enabled = SearchService.QUERY_REWRITING_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            SearchService.QUERY_REWRITING_ENABLED_SETTING,
            (Boolean enabled) -> this.enabled = enabled
        );
    }

    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!enabled || query == null) {
            return query;
        }

        List<QueryRewriter> sortedRewriters = new ArrayList<>(rewriters);
        sortedRewriters.sort(Comparator.comparingInt(QueryRewriter::priority));

        QueryBuilder current = query;
        for (QueryRewriter rewriter : sortedRewriters) {
            try {
                QueryBuilder rewritten = rewriter.rewrite(current, context);
                if (rewritten != current) {
                    current = rewritten;
                }
            } catch (Exception e) {
                logger.warn("Query rewriter {} failed: {}", rewriter.name(), e.getMessage());
            }
        }

        return current;
    }
}
