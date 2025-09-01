/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule.sync.detect;

/**
 * This interface represents a rule event which can be consumed by {@link org.density.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 */
public interface RuleEvent {
    /**
     * This method is used to consume this event
     *
     */
    void process();
}
