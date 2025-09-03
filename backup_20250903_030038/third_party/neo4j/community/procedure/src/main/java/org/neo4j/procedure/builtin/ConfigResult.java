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
package org.neo4j.procedure.builtin;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.procedure.Description;

public class ConfigResult {
    @Description("The name of the setting.")
    public final String name;

    @Description("The description of the setting.")
    public final String description;

    @Description("The set value of the setting.")
    public final String value;

    @Description("If the setting can be set dynamically or not.")
    public final boolean dynamic;

    @Description("The default value of the setting.")
    public final String defaultValue;

    @Description("The value of the setting when the database started.")
    public final String startupValue;

    @Description("Whether or not the setting was explicitly set.")
    public final boolean explicitlySet;

    @Description("A description of the valid values.")
    public final String validValues;

    public ConfigResult(Setting<Object> setting, Config config) {
        SettingImpl<Object> settingImpl = (SettingImpl<Object>) setting;
        this.name = setting.name();
        this.description = setting.description();
        this.value = settingImpl.valueToString(config.get(setting));
        this.dynamic = setting.dynamic();
        this.defaultValue = settingImpl.valueToString(config.getDefault(setting));
        this.startupValue = settingImpl.valueToString(config.getStartupValue(setting));
        this.explicitlySet = config.isExplicitlySet(setting);
        this.validValues = settingImpl.validValues();
    }
}
