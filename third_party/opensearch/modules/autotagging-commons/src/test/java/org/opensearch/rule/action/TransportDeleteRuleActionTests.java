/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.action.support.ActionFilters;
import org.density.rule.RulePersistenceService;
import org.density.rule.RulePersistenceServiceRegistry;
import org.density.test.DensityTestCase;
import org.density.transport.TransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteRuleActionTests extends DensityTestCase {
    TransportDeleteRuleAction sut;

    public void testExecute() {
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry = mock(RulePersistenceServiceRegistry.class);
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        RulePersistenceService rulePersistenceService = mock(RulePersistenceService.class);
        DeleteRuleRequest deleteRuleRequest = mock(DeleteRuleRequest.class);

        when(deleteRuleRequest.getFeatureType()).thenReturn(null);
        when(rulePersistenceServiceRegistry.getRulePersistenceService(any())).thenReturn(rulePersistenceService);
        doNothing().when(rulePersistenceService).deleteRule(any(), any());

        sut = new TransportDeleteRuleAction(transportService, actionFilters, rulePersistenceServiceRegistry);
        sut.doExecute(null, deleteRuleRequest, null);

        verify(rulePersistenceService, times(1)).deleteRule(any(), any());
    }
}
