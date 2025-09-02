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

package org.density.action.admin.cluster.storedscripts;

import org.density.Version;
import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.AcknowledgedRequest;
import org.density.common.annotation.PublicApi;
import org.density.common.xcontent.XContentHelper;
import org.density.common.xcontent.XContentType;
import org.density.core.common.Strings;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.MediaType;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.density.action.ValidateActions.addValidationError;

/**
 * Transport request for putting stored script
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> implements ToXContentFragment {

    private String id;
    private String context;
    private BytesReference content;
    private MediaType mediaType;
    private StoredScriptSource source;

    public PutStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        content = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            mediaType = in.readMediaType();
        } else {
            mediaType = in.readEnum(XContentType.class);
        }
        context = in.readOptionalString();
        source = new StoredScriptSource(in);
    }

    public PutStoredScriptRequest() {
        super();
    }

    public PutStoredScriptRequest(String id, String context, BytesReference content, MediaType mediaType, StoredScriptSource source) {
        super();
        this.id = id;
        this.context = context;
        this.content = content;
        this.mediaType = Objects.requireNonNull(mediaType);
        this.source = source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        if (content == null) {
            validationException = addValidationError("must specify code for stored script", validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public PutStoredScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    public String context() {
        return context;
    }

    public PutStoredScriptRequest context(String context) {
        this.context = context;
        return this;
    }

    public BytesReference content() {
        return content;
    }

    public MediaType mediaType() {
        return mediaType;
    }

    public StoredScriptSource source() {
        return source;
    }

    /**
     * Set the script source and the content type of the bytes.
     */
    public PutStoredScriptRequest content(BytesReference content, MediaType mediaType) {
        this.content = content;
        this.mediaType = Objects.requireNonNull(mediaType);
        this.source = StoredScriptSource.parse(content, mediaType);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            mediaType.writeTo(out);
        } else {
            out.writeEnum((XContentType) mediaType);
        }
        out.writeOptionalString(context);
        source.writeTo(out);
    }

    @Override
    public String toString() {
        String source = Strings.UNKNOWN_UUID_VALUE;

        try {
            source = XContentHelper.convertToJson(content, false, mediaType);
        } catch (Exception e) {
            // ignore
        }

        return "put stored script {id ["
            + id
            + "]"
            + (context != null ? ", context [" + context + "]" : "")
            + ", content ["
            + source
            + "]}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script");
        source.toXContent(builder, params);

        return builder;
    }
}
