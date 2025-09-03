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
package org.neo4j.dbms.diagnostics.profile;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.greaterThan;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.output.NullPrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

@TestDirectoryExtension
@DisabledOnOs(OS.WINDOWS)
class JstackProfilerTest {

    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    private JfrProfiler profiler;

    @AfterEach
    void after() {
        if (profiler != null) {
            profiler.stop();
        }
    }

    @Test
    void shouldDumpStackWithUniqueNameOnTick() throws IOException {
        FakeClock clock = Clocks.fakeClock();
        JstackProfiler profiler = profiler(clock);
        profiler.tick();
        clock.forward(1, TimeUnit.SECONDS);
        profiler.tick();
        clock.forward(1, TimeUnit.SECONDS);
        profiler.tick();
        Path[] paths = getThreadDumps();
        assertThat(paths.length).isEqualTo(3);
        assertThat(read(paths[0])).contains("Full thread dump");
    }

    @Test
    void shouldRunPeriodicallyInTool() throws IOException {
        JstackProfiler profiler = profiler(Clocks.nanoClock());
        try (ProfileTool tool = new ProfileTool()) {
            tool.add(profiler);
            tool.start();
            assertEventually(() -> getThreadDumps().length, greaterThan(3), 1, MINUTES);
        }
    }

    String read(Path p) throws IOException {
        try (InputStream is = fs.openAsInputStream(p)) {
            return new String(is.readAllBytes());
        }
    }

    Path[] getThreadDumps() throws IOException {
        return fs.listFiles(
                dir.homePath(), path -> path.getFileName().toString().startsWith("threads"));
    }

    JstackProfiler profiler(SystemNanoClock clock) throws IOException {
        assertThat(profiler).isNull();
        Path pidFile = dir.file("test.pid");
        Config cfg = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, dir.homePath())
                .set(BootloaderSettings.pid_file, pidFile)
                .build();
        FileSystemUtils.writeString(
                fs, pidFile, format("%s%n", ProcessHandle.current().pid()), EmptyMemoryTracker.INSTANCE);
        JMXDumper jmxDumper = new JMXDumper(cfg, fs, NullPrintStream.INSTANCE, NullPrintStream.INSTANCE, true);
        Optional<JmxDump> maybeDump = jmxDumper.getJMXDump();
        assumeThat(maybeDump).isPresent(); // IF not, then no point in running tests
        JmxDump jmxDump = maybeDump.get();
        return new JstackProfiler(jmxDump, fs, dir.homePath(), Duration.ofMillis(100), clock);
    }
}
