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
package org.neo4j.server.startup;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine;

@CommandLine.Command(name = "generate-cli-dump", description = "Generates cli dump")
public class CliDumpGenerator implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Output directory")
    private Path output;

    private static final String SEPARATOR = ";"; // can't ba a comma (,) since it's used internally in options

    private void dumpCli(Neo4jAdminCommand command) throws IOException {
        CommandLine commandLine = command.getActualAdminCommand(new ExecutionContext(output, output));
        String name = commandLine
                .getCommand()
                .getClass()
                .getAnnotation(CommandLine.Command.class)
                .name();
        Path file = output.resolve(name + "_dump.csv");

        String dump = dump(commandLine);
        Files.writeString(file, dump, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static String dump(CommandLine cmd) {
        StringBuilder stringBuilder = new StringBuilder();
        int numCommands = getNumCommands(cmd, 1);
        stringBuilder.append(header(numCommands));
        dump(cmd, 0, new String[numCommands], stringBuilder);
        return stringBuilder.toString();
    }

    private static void dump(CommandLine cmd, int depth, String[] commands, StringBuilder stringBuilder) {
        CommandLine.Model.CommandSpec spec = cmd.getCommandSpec();
        String name = cmd.getCommandName();
        if (spec.usageMessage().hidden()) {
            name += " <HIDDEN>";
        }
        commands[depth] = name;

        StringBuilder commandAndParams = new StringBuilder();
        for (String com : commands) {
            commandAndParams.append(com != null ? com : "").append(SEPARATOR);
        }
        String paramOptions = addParams(commandAndParams, spec);
        String comAndParams = commandAndParams.append(SEPARATOR).toString();

        for (CommandLine.Model.OptionSpec os : spec.options()) {
            stringBuilder
                    .append(comAndParams)
                    .append(Arrays.toString(os.names()))
                    .append(os.hidden() ? " <HIDDEN>" : "")
                    .append(SEPARATOR)
                    .append(paramOptions)
                    .append(SEPARATOR)
                    .append(asOneLine(os.description()))
                    .append(System.lineSeparator());
        }
        cmd.getSubcommands().values().stream()
                .distinct()
                .forEach(subCmd -> dump(subCmd, depth + 1, commands, stringBuilder));
    }

    private static int getNumCommands(CommandLine cmd, int depth) {
        return cmd.getSubcommands().values().stream()
                .distinct()
                .map(c -> getNumCommands(c, depth + 1))
                .max(Integer::compare)
                .orElse(depth);
    }

    private static String header(int numCommands) {
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= numCommands; i++) {
            header.append("Command ").append(i).append(SEPARATOR);
        }
        header.append("Parameters")
                .append(SEPARATOR)
                .append("Option")
                .append(SEPARATOR)
                .append("Param descriptions")
                .append(SEPARATOR)
                .append("Option description")
                .append(System.lineSeparator());
        return header.toString();
    }

    private static String addParams(StringBuilder commandAndParams, CommandLine.Model.CommandSpec spec) {
        var params = spec.positionalParameters();
        StringBuilder paramOptions = new StringBuilder();
        for (CommandLine.Model.PositionalParamSpec param : params) {
            commandAndParams.append(param.paramLabel());
            commandAndParams.append(param.hidden() ? " <HIDDEN>" : "");
            commandAndParams.append(" ");
            paramOptions.append(asOneLine(param.description())).append(" ");
        }
        return paramOptions.toString();
    }

    private static String asOneLine(String[] strings) {
        return Arrays.toString(strings).replace(System.lineSeparator(), " ").replace("%n", "");
    }

    @Override
    public Integer call() throws Exception {
        if (Files.exists(output) && !Files.isDirectory(output)) {
            throw new FileAlreadyExistsException(output.toString() + " Needs to be a directory");
        }
        Files.createDirectories(output);

        dumpCli(new Neo4jAdminCommand(Environment.SYSTEM));
        dumpCli(new Neo4jCommand(Environment.SYSTEM));
        return 0;
    }

    public static void main(String[] args) {
        new CommandLine(new CliDumpGenerator()).execute(args);
    }
}
