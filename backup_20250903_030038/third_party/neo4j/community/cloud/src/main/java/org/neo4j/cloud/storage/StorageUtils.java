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
package org.neo4j.cloud.storage;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.Sets;

public interface StorageUtils {

    Set<OpenOption> WRITE_OPTIONS = Set.of(WRITE, CREATE);
    Set<OpenOption> READ_OPTIONS = Set.of(READ);
    Set<OpenOption> APPEND_OPTIONS = Set.of(WRITE, APPEND, CREATE);

    /**
     * @param path the path
     * @return the full string form of the path, For {@link StoragePath} this would be the full {@link java.net.URI}
     * form of the path
     */
    static String toString(Path path) {
        if (path instanceof StoragePath storagePath) {
            return storagePath.toUri().toString();
        } else {
            return path.toString();
        }
    }

    /**
     *
     * @param options the options to check
     * @return <code>true</code> if the options contains any create options,
     * i.e. {@link java.nio.file.StandardOpenOption#CREATE} or {@link java.nio.file.StandardOpenOption#CREATE_NEW}
     */
    static boolean hasCreateOption(Set<? extends OpenOption> options) {
        return options.contains(CREATE) || options.contains(CREATE_NEW);
    }

    /**
     * @param options the options to normalize
     * @return the normalized options
     */
    static Set<OpenOption> normalizeForRead(OpenOption... options) throws IOException {
        return normalizeForRead(Sets.mutable.of(options));
    }

    /**
     * @param options the options to normalize
     * @return the normalized options
     */
    static Set<OpenOption> normalizeForRead(Set<? extends OpenOption> options) throws IOException {
        return normalize(READ, options);
    }

    /**
     * @param options the options to normalize
     * @return the normalized options
     */
    static Set<OpenOption> normalizeForWrite(OpenOption... options) throws IOException {
        return normalizeForWrite(Sets.mutable.of(options));
    }

    /**
     * @param options the options to normalize
     * @return the normalized options
     */
    static Set<OpenOption> normalizeForWrite(Set<? extends OpenOption> options) throws IOException {
        return normalize(WRITE, options);
    }

    /**
     * @param ex the error
     * @return the underlying error as an {@link IOException} if it's cause is an IO problem
     */
    static Exception toIOException(Exception ex) {
        if (ex instanceof IOException ioEx) {
            return ioEx;
        } else if (ex instanceof UncheckedIOException ioEx) {
            return ioEx.getCause();
        } else if (ex instanceof ExecutionException eEx && eEx.getCause() instanceof Exception) {
            return toIOException((Exception) eEx.getCause());
        } else {
            return ex;
        }
    }

    /**
     * @param ex the error
     * @param ioMessage the message to use when wrapping the original error
     * @return the error as an {@link IOException}
     */
    static IOException toIOException(Exception ex, Supplier<String> ioMessage) {
        if (ex instanceof IOException ioEx) {
            return ioEx;
        } else if (ex instanceof UncheckedIOException ioEx) {
            return ioEx.getCause();
        } else if (ex instanceof ExecutionException eEx && eEx.getCause() instanceof Exception) {
            final var exception = toIOException((Exception) eEx.getCause());
            if (exception instanceof IOException ioEx) {
                return ioEx;
            }

            return new IOException(ioMessage.get(), exception);
        } else {
            return new IOException(ioMessage.get(), ex);
        }
    }

    private static Set<OpenOption> normalize(OpenOption defaultOption, Set<? extends OpenOption> options)
            throws IOException {
        requireNonNull(defaultOption);

        final var normalized = Sets.mutable.<OpenOption>withAll(options);
        normalized.add(defaultOption);

        final var nonStandards = nonStandards(normalized);
        if (!nonStandards.isEmpty()) {
            throw new IOException("Unsupported OpenOption(s): " + nonStandards);
        }

        if (normalized.contains(SYNC)) {
            throw new IOException("Unsupported OpenOption: " + SYNC);
        }
        if (normalized.contains(DSYNC)) {
            throw new IOException("Unsupported OpenOption: " + DSYNC);
        }

        normalized.remove(SPARSE);
        normalized.remove(DELETE_ON_CLOSE);

        if (normalized.contains(CREATE) && normalized.contains(CREATE_NEW)) {
            normalized.remove(CREATE);
        }

        if (normalized.contains(READ) && normalized.contains(TRUNCATE_EXISTING)) {
            normalized.remove(TRUNCATE_EXISTING);
        }

        if ((normalized.contains(CREATE) || normalized.contains(CREATE_NEW))) {
            normalized.add(WRITE);
        }

        if ((normalized.contains(APPEND) || normalized.contains(TRUNCATE_EXISTING))) {
            normalized.add(WRITE);
        }

        if (normalized.contains(TRUNCATE_EXISTING) && normalized.contains(APPEND)) {
            // now it's just a replace operation
            normalized.remove(TRUNCATE_EXISTING);
            normalized.remove(APPEND);
        }

        if (normalized.contains(READ) && normalized.contains(WRITE)) {
            throw new IOException("Storage channels cannot be opened for both READ and WRITE operations");
        }

        return normalized;
    }

    private static String nonStandards(Set<? extends OpenOption> options) {
        return options.stream()
                .filter(opt -> !(opt instanceof StandardOpenOption))
                .map(opt -> opt.getClass().getSimpleName())
                .collect(Collectors.joining(", "));
    }
}
