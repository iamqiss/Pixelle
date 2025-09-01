/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.script.mustache;

import org.density.action.ActionType;

public class RenderSearchTemplateAction extends ActionType<SearchTemplateResponse> {

    public static final RenderSearchTemplateAction INSTANCE = new RenderSearchTemplateAction();
    public static final String NAME = "indices:data/read/search/template/render";

    private RenderSearchTemplateAction() {
        super(NAME, SearchTemplateResponse::new);
    }
}
