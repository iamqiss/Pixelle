/*
* SPDX-License-Identifier: Apache-2.0
*
* The Density Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/
/*
* Modifications Copyright Density Contributors. See
* GitHub history for details.
*/

package org.density.systemdinteg;
import org.apache.lucene.tests.util.LuceneTestCase;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;


public class SystemdIntegTests extends LuceneTestCase {

    private static String densityPid;

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        densityPid = getDensityPid();

        if (densityPid.isEmpty()) {
            throw new RuntimeException("Failed to find Density process ID");
        }
    }

    private static String getDensityPid() throws IOException, InterruptedException {
        String command = "systemctl show --property=MainPID density";
        String output = executeCommand(command, "Failed to get Density PID");
        return output.replace("MainPID=", "").trim();
    }

    private boolean checkPathExists(String path) throws IOException, InterruptedException {
        String command = String.format(Locale.ROOT, "test -e %s && echo true || echo false", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check path existence"));
    }

    private boolean checkPathReadable(String path) throws IOException, InterruptedException {
        String command = String.format(Locale.ROOT, "sudo su density -s /bin/sh -c 'test -r %s && echo true || echo false'", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check read permission"));
    }

    private boolean checkPathWritable(String path) throws IOException, InterruptedException {
        String command = String.format(Locale.ROOT, "sudo su density -s /bin/sh -c 'test -w %s && echo true || echo false'", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check write permission"));
    }

    private String getPathOwnership(String path) throws IOException, InterruptedException {
        String command = String.format(Locale.ROOT, "stat -c '%%U:%%G' %s", path);
        return executeCommand(command, "Failed to get path ownership");
    }

    private static String executeCommand(String command, String errorMessage) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            if (process.waitFor() != 0) {
                throw new RuntimeException(errorMessage);
            }
            return output.toString().trim();
        }
    }

    public void testReadOnlyPaths() throws IOException, InterruptedException {
        String[] readOnlyPaths = {
            "/etc/os-release", "/usr/lib/os-release", "/etc/system-release",
            "/proc/self/mountinfo", "/proc/diskstats",
            "/proc/self/cgroup", "/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu/-",
            "/sys/fs/cgroup/cpuacct", "/sys/fs/cgroup/cpuacct/-",
            "/sys/fs/cgroup/memory", "/sys/fs/cgroup/memory/-"
        };

        for (String path : readOnlyPaths) {
            if (checkPathExists(path)) {
                assertTrue("Path should be readable: " + path, checkPathReadable(path));
                assertFalse("Path should not be writable: " + path, checkPathWritable(path));
            }
        }
    }

    public void testReadWritePaths() throws IOException, InterruptedException {
        String[] readWritePaths = {"/var/log/density", "/var/lib/density"};
        for (String path : readWritePaths) {
            assertTrue("Path should exist: " + path, checkPathExists(path));
            assertTrue("Path should be readable: " + path, checkPathReadable(path));
            assertTrue("Path should be writable: " + path, checkPathWritable(path));
            String ownership = getPathOwnership(path);
            assertTrue("Path should be owned by density:density or density:adm", ownership.equals("density:density") || ownership.equals("density:adm"));
        }
    }

    public void testMaxProcesses() throws IOException, InterruptedException {
        String limits = executeCommand("sudo su -c 'cat /proc/" + densityPid + "/limits'", "Failed to read process limits");
        assertTrue("Max processes limit should be 4096 or unlimited", 
                limits.contains("Max processes             4096                 4096") ||
                limits.contains("Max processes             unlimited            unlimited"));
    }

    public void testFileDescriptorLimit() throws IOException, InterruptedException {
        String limits = executeCommand("sudo su -c 'cat /proc/" + densityPid + "/limits'", "Failed to read process limits");
        assertTrue("File descriptor limit should be at least 65535", 
                limits.contains("Max open files            65535                65535") ||
                limits.contains("Max open files            unlimited            unlimited"));
    }

    public void testSeccompEnabled() throws IOException, InterruptedException {
        // Check if Seccomp is enabled
        String seccomp = executeCommand("grep \"^Seccomp:\" /proc/" + densityPid + "/status", "Failed to read Seccomp status");
        int seccompValue = Integer.parseInt(seccomp.split(":\\s*")[1].trim());
        assertNotEquals("Seccomp should be enabled", 0, seccompValue);
    }

    public void testRebootSysCall() throws IOException, InterruptedException {
        String rebootResult = executeCommand("sudo su density -c 'kill -s SIGHUP 1' 2>&1 || echo 'Operation not permitted'", "Failed to test reboot system call");
        assertTrue("Reboot system call should be blocked", rebootResult.contains("Operation not permitted"));
    }

    public void testDensityProcessCannotExit() throws IOException, InterruptedException {

        String scriptPath;
        try {
            scriptPath = SystemdIntegTests.class.getResource("/scripts/terminate.sh").toURI().getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to convert URL to URI", e);
        }

        if (scriptPath == null) {
            throw new IllegalStateException("Could not find terminate.sh script in resources");
        }
        ProcessBuilder processBuilder = new ProcessBuilder(scriptPath, densityPid);
        Process process = processBuilder.start();

        // Wait a moment for any potential termination to take effect
        Thread.sleep(2000);

        // Verify the Density service status
        String serviceStatus = executeCommand(
            "systemctl is-active density",
            "Failed to check Density service status"
        );

        assertEquals("Density service should be active", "active", serviceStatus.trim());
    }

}
