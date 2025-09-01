
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionRequest;
import org.density.action.ActionRequestValidationException;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.density.action.ValidateActions.addValidationError;

/**
 * Request to delete one or more PIT search contexts based on IDs.
 *
 * @density.api
 */
@PublicApi(since = "2.3.0")
public class DeletePitRequest extends ActionRequest implements ToXContentObject {

    /**
     * List of PIT IDs to be deleted , and use "_all" to delete all PIT reader contexts
     */
    private final List<String> pitIds = new ArrayList<>();

    public DeletePitRequest(StreamInput in) throws IOException {
        super(in);
        pitIds.addAll(Arrays.asList(in.readStringArray()));
    }

    public DeletePitRequest(String... pitIds) {
        this.pitIds.addAll(Arrays.asList(pitIds));
    }

    public DeletePitRequest(List<String> pitIds) {
        this.pitIds.addAll(pitIds);
    }

    public void clearAndSetPitIds(List<String> pitIds) {
        this.pitIds.clear();
        this.pitIds.addAll(pitIds);
    }

    public DeletePitRequest() {}

    public List<String> getPitIds() {
        return pitIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pitIds == null || pitIds.isEmpty()) {
            validationException = addValidationError("no pit ids specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (pitIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(pitIds.toArray(new String[0]));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("pit_id");
        for (String pitId : pitIds) {
            builder.value(pitId);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        pitIds.clear();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("pit_id".equals(currentFieldName)) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("pit_id array element should only contain pit_id");
                            }
                            pitIds.add(parser.text());
                        }
                    } else {
                        if (token.isValue() == false) {
                            throw new IllegalArgumentException("pit_id element should only contain pit_id");
                        }
                        pitIds.add(parser.text());
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] "
                    );
                }
            }
        }
    }

}
