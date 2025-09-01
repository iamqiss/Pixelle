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
package org.neo4j.exceptions;

import static java.lang.String.format;

import java.util.Arrays;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

public class InvalidCypherOption extends InvalidArgumentException {

    @Deprecated
    private InvalidCypherOption(String message) {
        super(message);
    }

    private InvalidCypherOption(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static InvalidCypherOption invalidCombination(
            String optionName1, String option1, String optionName2, String option2) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                        .withParam(GqlParams.StringParam.option1, optionName1 + ": " + option1)
                        .withParam(GqlParams.StringParam.option2, optionName2 + ": " + option2)
                        .build())
                .build();
        return new InvalidCypherOption(
                gql, format("Cannot combine %s '%s' with %s '%s'", optionName1, option1, optionName2, option2));
    }

    public static InvalidCypherOption parallelRuntimeIsDisabled() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N44)
                        .build())
                .build();
        return new InvalidCypherOption(
                gql, "Parallel runtime has been disabled, please enable it or upgrade to a bigger Aura instance.");
    }

    public static InvalidCypherOption invalidOption(String input, String name, String... validOptions) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N10)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.option, name)
                        .withParam(
                                GqlParams.ListParam.optionList,
                                Arrays.stream(validOptions).toList())
                        .build())
                .build();
        return new InvalidCypherOption(
                gql,
                format(
                        "%s is not a valid option for %s. Valid options are: %s",
                        input, name, String.join(", ", validOptions)));
    }

    public static InvalidCypherOption conflictingOptionForName(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                        .withParam(GqlParams.StringParam.option, name)
                        .build())
                .build();
        return new InvalidCypherOption(gql, "Can't specify multiple conflicting values for " + name);
    }

    public static InvalidCypherOption unsupportedOptions(String... keys) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N07)
                        .withParam(
                                GqlParams.ListParam.optionList,
                                Arrays.stream(keys).toList())
                        .build())
                .build();
        return new InvalidCypherOption(gql, format("Unsupported options: %s", String.join(", ", keys)));
    }

    public static InvalidCypherOption irEagerAnalyzerUnsupported(String operation) {
        return new InvalidCypherOption(format(
                "The Cypher option `eagerAnalyzer=ir` is not supported while %s. Use `eagerAnalyzer=lp` instead.",
                operation));
    }

    // NOTE: this is an internal error and should probably not have any GQL code
    public static InvalidCypherOption sourceGenerationDisabled() {
        return new InvalidCypherOption("In order to use source generation you need to enable "
                + "`internal.cypher.pipelined.allow_source_generation`");
    }
}
