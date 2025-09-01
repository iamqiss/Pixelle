/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.indices.template.delete;

import org.density.action.ActionRequestValidationException;
import org.density.action.ActionType;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.ClusterManagerNodeRequest;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.density.action.ValidateActions.addValidationError;

/**
 * Transport action for deleting an index template component
 *
 * @density.internal
 */
public class DeleteComponentTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComponentTemplateAction INSTANCE = new DeleteComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/delete";

    private DeleteComponentTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Inner Request class for deleting component template
     *
     * @density.internal
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {

        private String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        public Request() {}

        /**
         * Constructs a new delete index request for the specified name.
         */
        public Request(String name) {
            this.name = name;
        }

        /**
         * Set the index template name to delete.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        /**
         * The index template name to delete.
         */
        public String name() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }
    }
}
