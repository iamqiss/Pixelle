/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.util.StringBuilders;
import org.density.core.common.Strings;

/**
 * Pattern converter to populate DensityMessageField in a pattern.
 * It will only populate these if the event have message of type <code>DensityLogMessage</code>.
 *
 * @density.internal
 */
@Plugin(category = PatternConverter.CATEGORY, name = "DensityMessageField")
@ConverterKeys({ "DensityMessageField" })
public final class DensityMessageFieldConverter extends LogEventPatternConverter {

    private String key;

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static DensityMessageFieldConverter newInstance(final Configuration config, final String[] options) {
        final String key = options[0];

        return new DensityMessageFieldConverter(key);
    }

    public DensityMessageFieldConverter(String key) {
        super("DensityMessageField", "DensityMessageField");
        this.key = key;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage() instanceof DensityLogMessage) {
            DensityLogMessage logMessage = (DensityLogMessage) event.getMessage();
            final String value = logMessage.getValueFor(key);
            if (Strings.isNullOrEmpty(value) == false) {
                StringBuilders.appendValue(toAppendTo, value);
                return;
            }
        }
        StringBuilders.appendValue(toAppendTo, "");
    }
}
