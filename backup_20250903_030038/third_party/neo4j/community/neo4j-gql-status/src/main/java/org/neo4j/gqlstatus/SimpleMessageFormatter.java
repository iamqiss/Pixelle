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
package org.neo4j.gqlstatus;

import java.util.List;
import java.util.Map;

public class SimpleMessageFormatter {

    // Without param-specific processors
    public static String format(String template, String substitution, int[] offsets, Object[] params) {
        return format(null, null, template, substitution, offsets, params);
    }

    public static String format(
            GqlParams.GqlParam[] statusParameterKeys,
            Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles,
            String template,
            String substitution,
            int[] offsets,
            Object[] params) {
        final var message = new StringBuilder();
        var currentParam = 0;
        int prevOffset = 0;

        for (int offset : offsets) {
            message.append(template, prevOffset, offset);
            var formattedParam = processParam(statusParameterKeys, joinStyles, params, currentParam++);
            message.append(formattedParam);
            offset += substitution.length();
            prevOffset = offset;
        }
        if (prevOffset < template.length()) message.append(template, prevOffset, template.length());
        return message.toString();
    }

    // TODO: As Love said in his formatting that I had to squish for the new handling, we could skip this and just use
    private static String processParam(
            GqlParams.GqlParam[] statusParameterKeys,
            Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles,
            Object[] params,
            int currentParam) {
        if (params == null) return "null";
        if (statusParameterKeys == null) return String.valueOf(params[currentParam]);

        var paramKey = statusParameterKeys[currentParam];
        if (paramKey instanceof GqlParams.ListParam key && params[currentParam] instanceof List<?> list) {
            var joinStyle = joinStyles.get(paramKey);
            return key.process(list, joinStyle);
        } else {
            return paramKey.process(params[currentParam]);
        }
    }
}
