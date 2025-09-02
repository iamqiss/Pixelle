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

import java.io.IOException;
import java.util.List;

/**
 * Class for admission control stats used as part of node stats
 * @density.internal
 */
public class AdmissionControlStats implements ToXContentFragment, Writeable {

    private final List<AdmissionControllerStats> admissionControllerStatsList;

    /**
     *
     * @param admissionControllerStatsList list of admissionControllerStats
     */
    public AdmissionControlStats(List<AdmissionControllerStats> admissionControllerStatsList) {
        this.admissionControllerStatsList = admissionControllerStatsList;
    }

    /**
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public AdmissionControlStats(StreamInput in) throws IOException {
        this.admissionControllerStatsList = in.readList(AdmissionControllerStats::new);
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out the output stream to write entity content to
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.admissionControllerStatsList);
    }

    public List<AdmissionControllerStats> getAdmissionControllerStatsList() {
        return admissionControllerStatsList;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("admission_control");
        for (AdmissionControllerStats admissionControllerStats : this.admissionControllerStatsList) {
            builder.field(admissionControllerStats.getAdmissionControllerName(), admissionControllerStats);
        }
        return builder.endObject();
    }
}
