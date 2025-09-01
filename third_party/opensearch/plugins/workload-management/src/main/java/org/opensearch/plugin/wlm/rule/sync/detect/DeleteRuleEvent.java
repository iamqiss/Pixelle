/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule.sync.detect;

import org.density.rule.InMemoryRuleProcessingService;
import org.density.rule.autotagging.Rule;

/**
 * This class represents a delete rule event which can be consumed by {@link org.density.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 */
public class DeleteRuleEvent implements RuleEvent {
    private Rule deletedRule;
    private final InMemoryRuleProcessingService ruleProcessingService;

    /**
     * Constructor
     * @param deletedRule
     * @param ruleProcessingService
     */
    public DeleteRuleEvent(Rule deletedRule, InMemoryRuleProcessingService ruleProcessingService) {
        this.deletedRule = deletedRule;
        this.ruleProcessingService = ruleProcessingService;
    }

    @Override
    public void process() {
        ruleProcessingService.remove(deletedRule);
    }
}
