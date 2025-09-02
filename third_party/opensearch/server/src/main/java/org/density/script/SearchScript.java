/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.script;

import org.density.common.unit.TimeValue;

import java.util.Map;

/**
 * A script used by the Search Script Processor.
 *
 * @density.internal
 */
public abstract class SearchScript {

    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link SearchScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "search",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public SearchScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute(Map<String, Object> ctx);

    /**
     * Factory for search script
     *
     * @density.internal
     */
    public interface Factory {
        SearchScript newInstance(Map<String, Object> params);
    }
}
