/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.stats;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.ratelimitting.admissioncontrol.controllers.AdmissionController;

import java.io.IOException;
import java.util.Map;

/**
 * Class for admission controller ( such as CPU ) stats which includes rejection count for each action type
 * @density.internal
 */
public class AdmissionControllerStats implements Writeable, ToXContentFragment {
    public Map<String, Long> rejectionCount;
    public String admissionControllerName;

    public AdmissionControllerStats(AdmissionController admissionController) {
        this.rejectionCount = admissionController.getRejectionStats();
        this.admissionControllerName = admissionController.getName();
    }

    public AdmissionControllerStats(StreamInput in) throws IOException {
        this.rejectionCount = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.admissionControllerName = in.readString();
    }

    public String getAdmissionControllerName() {
        return admissionControllerName;
    }

    public Map<String, Long> getRejectionCount() {
        return rejectionCount;
    }

    /**
     * Writes this instance into a {@link StreamOutput}
     * @param out the {@link StreamOutput} to write to
     * @throws IOException if an error occurs while writing to the StreamOutput
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.rejectionCount, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeString(this.admissionControllerName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("transport");
        {
            builder.startObject("rejection_count");
            {
                for (Map.Entry<String, Long> rejectionCountEntry : this.rejectionCount.entrySet()) {
                    builder.field(rejectionCountEntry.getKey(), rejectionCountEntry.getValue());
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder.endObject();
    }
}
