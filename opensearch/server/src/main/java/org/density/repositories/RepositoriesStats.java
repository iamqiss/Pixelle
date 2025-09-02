/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.repositories;

import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.common.util.CollectionUtils;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Encapsulates stats for multiple repositories
 *
 * @density.api
 */
@PublicApi(since = "2.11.0")
public class RepositoriesStats implements Writeable, ToXContentObject {

    List<RepositoryStatsSnapshot> repositoryStatsSnapshots;

    public RepositoriesStats(List<RepositoryStatsSnapshot> repositoryStatsSnapshots) {
        this.repositoryStatsSnapshots = repositoryStatsSnapshots;
    }

    public RepositoriesStats(StreamInput in) throws IOException {
        this.repositoryStatsSnapshots = in.readList(RepositoryStatsSnapshot::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(repositoryStatsSnapshots);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("repositories");
        if (CollectionUtils.isEmpty(repositoryStatsSnapshots) == false) {
            for (RepositoryStatsSnapshot repositoryStatsSnapshot : repositoryStatsSnapshots) {
                repositoryStatsSnapshot.toXContent(builder, params);
            }
        }
        builder.endArray();
        return builder;
    }
}
