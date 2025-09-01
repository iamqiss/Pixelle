/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionType;

/**
 * Action type to get search pipelines
 *
 * @density.internal
 */
public class GetSearchPipelineAction extends ActionType<GetSearchPipelineResponse> {
    public static final GetSearchPipelineAction INSTANCE = new GetSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/get";

    public GetSearchPipelineAction() {
        super(NAME, GetSearchPipelineResponse::new);
    }
}
