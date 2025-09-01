/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.density.action.ActionType;

/**
 * Transport action for getting ingestion state.
 *
 * @density.experimental
 */
public class GetIngestionStateAction extends ActionType<GetIngestionStateResponse> {

    public static final GetIngestionStateAction INSTANCE = new GetIngestionStateAction();
    public static final String NAME = "indices:monitor/ingestion/state";

    private GetIngestionStateAction() {
        super(NAME, GetIngestionStateResponse::new);
    }
}
