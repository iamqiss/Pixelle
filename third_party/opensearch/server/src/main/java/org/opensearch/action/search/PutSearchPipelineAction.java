/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionType;
import org.density.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action type to put a new search pipeline
 *
 * @density.internal
 */
public class PutSearchPipelineAction extends ActionType<AcknowledgedResponse> {
    public static final PutSearchPipelineAction INSTANCE = new PutSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/put";

    public PutSearchPipelineAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
