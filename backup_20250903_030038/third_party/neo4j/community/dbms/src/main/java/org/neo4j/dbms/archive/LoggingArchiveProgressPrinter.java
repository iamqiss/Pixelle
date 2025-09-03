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
package org.neo4j.dbms.archive;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters.EmptyOutputProgressPrinter;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

class LoggingArchiveProgressPrinter implements ArchiveProgressPrinter {
    private static final long PRINT_INTERVAL = Duration.ofSeconds(60).toMillis();
    private final OutputProgressPrinter progressPrinter;

    private long currentBytes;
    private long currentFiles;
    private boolean done;
    private long maxBytes;
    private long maxFiles;
    private long startTime;
    private double printingTime;
    private final SystemNanoClock clock;

    private boolean force;
    private Deadline deadline = null;
    private PercentageCondition percentage = null;

    public static ArchiveProgressPrinter createProgressPrinter(OutputProgressPrinter progressPrinter) {
        requireNonNull(progressPrinter);
        if (progressPrinter instanceof EmptyOutputProgressPrinter) {
            return ArchiveProgressPrinter.EMPTY;
        }
        return new LoggingArchiveProgressPrinter(progressPrinter, Clocks.nanoClock());
    }

    LoggingArchiveProgressPrinter(OutputProgressPrinter progressPrinter, SystemNanoClock clock) {
        this.progressPrinter = requireNonNull(progressPrinter);
        this.clock = clock;
    }

    @Override
    public Resource startPrinting() {
        startTime = clock.millis();
        deadline = new Deadline(startTime - PRINT_INTERVAL, PRINT_INTERVAL);
        return () -> {
            done();
            printProgress();
        };
    }

    @Override
    public void reset() {
        maxBytes = 0;
        maxFiles = 0;
        currentBytes = 0;
        currentFiles = 0;
        deadline = null;
        percentage = null;
    }

    @Override
    public void maxBytes(long value) {
        maxBytes = value;
        percentage = new PercentageCondition(value);
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public void maxFiles(long value) {
        maxFiles = value;
    }

    @Override
    public long maxFiles() {
        return maxFiles;
    }

    @Override
    public void beginFile() {
        currentFiles++;
    }

    @Override
    public void printOnNextUpdate() {
        force = true;
    }

    @Override
    public void addBytes(long n) {
        currentBytes += n;

        var when = clock.millis();
        var deadlineReached = (deadline != null && deadline.reached(when));
        var percentageReached = (percentage != null && percentage.updateAndCheckIfReached(currentBytes));

        if (force || deadlineReached || percentageReached) {
            printProgress();

            // If we manage to print, for whatever reason, move the deadline into the future.
            if (deadline != null) {
                deadline.next(when);
            }
            force = false;
        }
    }

    @Override
    public void endFile() {
        printProgress();
    }

    @Override
    public void done() {
        var now = clock.millis();
        printingTime = (double) (now - startTime) / 1000; // convert to seconds
        done = true;
    }

    @VisibleForTesting
    double getPrintingTime() {
        return printingTime;
    }

    @Override
    public void printProgress() {
        if (done) {
            progressPrinter.print("Done: " + currentFiles + " files, " + ByteUnit.bytesToString(currentBytes)
                    + " processed in " + String.format("%.3f", printingTime) + " seconds.");
            progressPrinter.complete();
        } else if (maxFiles > 0 && maxBytes > 0) {
            double progress = (currentBytes / (double) maxBytes) * 100;
            progressPrinter.print(
                    "Files: " + currentFiles + '/' + maxFiles + ", data: " + String.format("%4.1f%%", progress));
        } else {
            progressPrinter.print("Files: " + currentFiles + "/?" + ", data: ??.?%");
        }
    }

    static class Deadline {
        private final long interval;
        private long target;

        Deadline(long now, long interval) {
            this.interval = interval;
            this.target = now + interval;
        }

        boolean reached(long when) {
            return when >= target;
        }

        void next(long now) {
            target = now + interval;
        }
    }

    static class PercentageCondition {
        final long bucket;
        long current;

        PercentageCondition(long maxBytes) {
            bucket = maxBytes / 100;
            current = 0;
        }

        boolean updateAndCheckIfReached(long currentBytes) {
            // If we have less than 100 bytes, disable the check
            if (bucket == 0) {
                return false;
            }

            long previous = current;
            current = currentBytes / bucket;
            return current > previous;
        }
    }
}
