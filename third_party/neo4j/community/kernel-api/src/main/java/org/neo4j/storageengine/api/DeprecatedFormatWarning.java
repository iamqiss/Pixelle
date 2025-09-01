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
package org.neo4j.storageengine.api;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.InternalLog;

public class DeprecatedFormatWarning {
    private static final String FORMAT_INFO =
            "For details on deprecated store formats, see https://neo4j.com/docs/store-format-deprecations.";

    public static String getFormatWarning(String databaseName, String format) {
        return "The database: " + databaseName + " has a deprecated store format: " + format + ". " + FORMAT_INFO;
    }

    public static String getTargetFormatWarning(String format) {
        return "The targeted store format: " + format + " is deprecated. " + FORMAT_INFO;
    }

    public static String getConfigFormatWarning(String format) {
        return "The specified format: " + format + " set in the config is deprecated. " + FORMAT_INFO;
    }

    @ServiceProvider
    public static class FormatDeprecationWarningMigrator implements SettingMigrator {

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            String format = values.get(db_format.name());
            if (isNotBlank(format) && StorageEngineFactory.isFormatDeprecated(format)) {
                log.warn(getConfigFormatWarning(format));
            }
        }
    }
}
