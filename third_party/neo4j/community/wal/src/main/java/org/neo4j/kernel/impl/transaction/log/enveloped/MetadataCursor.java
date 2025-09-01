/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.enveloped;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.cursor.RawCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class MetadataCursor implements RawCursor<LogHeader, IOException> {
    private final LogsRepository logsRepository;
    private final long[] versions;
    private final EnvelopedLogFiles logFiles;
    private int currentVersionIndex = -1;
    private EnvelopeReadChannel channel;
    private boolean closed;
    private Path currentPath;

    MetadataCursor(LogsRepository logsRepository, EnvelopedLogFiles logFiles, boolean reversed) throws IOException {
        this.logsRepository = logsRepository;
        this.versions = logsRepository.logVersions(reversed);
        this.logFiles = logFiles;
    }

    @Override
    public boolean next() throws IOException {
        if (closed) {
            return false;
        }
        if (++currentVersionIndex >= versions.length) {
            return false;
        }
        var version = currentVersion();
        var logChannel = logsRepository.openReadChannel(version);
        currentPath = logChannel.path();
        if (channel != null) {
            channel.close();
        }
        channel = logFiles.envelopedReadChannel(logChannel, version);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
            closed = true;
        }
    }

    public Path currentPath() {
        return currentPath;
    }

    long currentVersion() {
        return versions[currentVersionIndex];
    }

    @Override
    public LogHeader get() {
        return channel != null ? channel.logHeader() : null;
    }
}
