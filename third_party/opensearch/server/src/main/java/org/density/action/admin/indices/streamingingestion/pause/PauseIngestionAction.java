/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.pause;

import org.density.action.ActionType;

/**
 * Transport action for pausing ingestion.
 *
 * @density.experimental
 */
public class PauseIngestionAction extends ActionType<PauseIngestionResponse> {

    public static final PauseIngestionAction INSTANCE = new PauseIngestionAction();
    public static final String NAME = "indices:admin/ingestion/pause";

    private PauseIngestionAction() {
        super(NAME, PauseIngestionResponse::new);
    }
}
