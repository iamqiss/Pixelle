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
package org.neo4j.logging.internal;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;

import java.util.regex.Pattern;
import org.neo4j.logging.InternalLog;

public final class LogMessageUtil {
    public static int LOG_LINE_MAX_LENGTH =
            getInteger(getProperty("log4j.layout.jsonTemplate.maxStringLength"), 32 * 1024);
    private static final Pattern SLF4J_PLACEHOLDER = Pattern.compile("\\{}");

    private LogMessageUtil() {}

    /**
     * Replace SLF4J-style placeholders like {@code {}} with {@link String#format(String, Object...)} placeholders like {@code %s} in the given string.
     * This is necessary for logging adapters that redirect SLF4J loggers to neo4j {@link InternalLog}. Former uses {@code {}} while later {@code %s}.
     *
     * @param template the message template to modify.
     * @return new message template which can be safely formatted using {@link String#format(String, Object...)}.
     */
    public static String slf4jToStringFormatPlaceholders(String template) {
        return SLF4J_PLACEHOLDER.matcher(template).replaceAll("%s");
    }
}
