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
package org.neo4j.kernel.api.exceptions;

import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.values.AnyValue;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

public class InvalidArgumentsException extends GqlException implements Status.HasStatus {
    private final Status status;

    @Deprecated
    public InvalidArgumentsException(String message) {
        this(message, null);
    }

    public InvalidArgumentsException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, null);
    }

    @Deprecated
    public InvalidArgumentsException(String message, Throwable cause) {
        super(message, cause);
        this.status = Status.General.InvalidArguments;
    }

    public InvalidArgumentsException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
        this.status = Status.General.InvalidArguments;
    }

    public static InvalidArgumentsException requiresPositiveInteger(String option, int value) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N02)
                        .withParam(GqlParams.StringParam.option, option)
                        .withParam(GqlParams.NumberParam.value, value)
                        .build())
                .build();
        return new InvalidArgumentsException(
                gql, String.format("Option `%s` requires positive integer argument, got `%d`", option, value));
    }

    public static InvalidArgumentsException invalidResource(String typeString) {
        var gqlMsg = String.format("Found invalid resource (%s) in the system graph.", typeString);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), gqlMsg);
        return new InvalidArgumentsException(
                gql, String.format("Found not valid resource (%s) in the system graph.", typeString));
    }

    public static InvalidArgumentsException entityResourceInvalidAction(
            String entity, String action, String legacyFormat) {
        var msg = String.format(
                "%s resource cannot be combined with action %s%s%s", entity, legacyFormat, action, legacyFormat);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), msg);
        return new InvalidArgumentsException(gql, msg);
    }

    public static InvalidArgumentsException internalAlterServer(String name) {
        var msg = String.format("Server '%s' can't be altered: must specify options", name);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), msg);
        return new InvalidArgumentsException(gql, msg);
    }

    @Override
    public Status status() {
        return status;
    }

    private static ErrorGqlStatusObject getIdxGql(MapValue itemsMap, java.util.List<String> validConfigSettingNames) {
        var prettyVal = new PrettyPrinter();
        itemsMap.writeTo(prettyVal);
        return GqlHelper.getGql42001_22N04(prettyVal.value(), "config options", validConfigSettingNames);
    }

    protected static String invalidConfigValueString(PrettyPrinter pp, AnyValue value, String schemaType) {
        value.writeTo(pp);
        return invalidConfigValueString(pp.value(), schemaType);
    }

    protected static String invalidConfigValueString(String value, String schemaType) {
        return String.format("Could not create %s with specified index config '%s'", schemaType, value);
    }

    public static InvalidArgumentsException pointOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains spatial config settings options.\nTo create point index, please use 'CREATE POINT INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException fulltextOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains fulltext config options.\nTo create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException vectorOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains vector config options.\nTo create vector index, please use 'CREATE VECTOR INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException expectedNonePrimarySecondary(
            String value, String modeConstraint, List<String> expected) {
        var gql = GqlHelper.getGql42001_22N04(value, modeConstraint, expected);
        return new InvalidArgumentsException(
                gql, String.format("%s expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '%s'.", modeConstraint, value));
    }

    public static InvalidArgumentsException unrecognisedOptionGivenValue(
            String operation, String value, String key, String validValue, Boolean formatValidValuesForOld) {
        var validValues = List.of(validValue);
        var gql = GqlHelper.getGql42001_22N04(value, key, validValues);
        if (formatValidValuesForOld) {
            validValue = "'" + validValue + "'";
        }
        return new InvalidArgumentsException(
                gql,
                String.format("Could not %s with specified %s '%s'. Expected %s.", operation, key, value, validValue));
    }

    public static InvalidArgumentsException unrecognisedOptionGivenValue(
            String operation, String key, String value, List<String> validValues) {
        var gql = GqlHelper.getGql42001_22N04(value, key, validValues);
        String validValuesString =
                validValues.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with specified %s '%s', Expected one of %s.",
                        operation, key, value, validValuesString));
    }

    public static InvalidArgumentsException unrecognisedOptionsOnlyKeys(
            String operation, String invalidKeys, List<String> permittedOptions) {
        var permittedOptionsString = String.join(", ", permittedOptions);
        var gql = GqlHelper.getGql42001_22N04(
                invalidKeys, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with unrecognised option(s): %s. Expected %s.",
                        operation, invalidKeys, permittedOptionsString));
    }

    public static InvalidArgumentsException unrecognisedOptionsNoOperation(
            String invalidKey, List<String> permittedOptions) {
        var permittedOptionsString =
                permittedOptions.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var gql = GqlHelper.getGql42001_22N04(
                invalidKey, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql, String.format("Unrecognised option '%s', expected %s.", invalidKey, permittedOptionsString));
    }

    public static InvalidArgumentsException unrecognisedCreateDbOptions(
            String operation, String invalidKeys, List<String> permittedOptions) {
        var permittedOptionsString = String.join(", ", permittedOptions);
        var gql = GqlHelper.getGql42001_22N04(
                invalidKeys, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with 'CREATE DATABASE' option(s): %s. Expected %s.",
                        operation, invalidKeys, permittedOptionsString));
    }

    public static InvalidArgumentsException invalidDriverSettings(
            String operation, String invalidKeys, List<String> validKeys) {
        var validKeysString = String.join(", ", validKeys);
        var gql = GqlHelper.getGql42001_22N04(invalidKeys, GqlParams.StringParam.input.process("DRIVER"), validKeys);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver setting(s) provided: %s. Valid driver settings are: %s",
                        operation, invalidKeys, validKeysString));
    }

    public static InvalidArgumentsException unexpectedDriverSettingValue(
            String operation, String value, String settingKey, List<String> validValues) {
        var validValuesString = String.join(", ", validValues);
        var gql = GqlHelper.getGql42001_22N04(value, GqlParams.StringParam.input.process("DRIVER"), validValues);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver settings value for '%s'. Expected one of %s.",
                        operation, settingKey, validValuesString));
    }

    public static InvalidArgumentsException invalidOptionFormat(
            String operation, String key, String value, List<String> validFormats) {
        var gql = GqlHelper.getGql42001_22N04(value, key, validFormats);
        var validFormatsString =
                validFormats.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", "));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with specified %s '%s'. Unknown format, supported formats are %s",
                        operation, key, value, validFormatsString));
    }

    public static InvalidArgumentsException invalidIndexOptionValue(String providedOption, String schemaType) {
        var validOptions = java.util.List.of("indexProvider", "indexConfig");
        var gql = GqlHelper.getGql42001_22N04(
                providedOption, GqlParams.StringParam.input.process("OPTIONS"), validOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to create %s: Invalid option provided, valid options are `indexProvider` and `indexConfig`.",
                        schemaType));
    }

    public static InvalidArgumentsException invalidIndexConfig(
            String schemaType, String indexConfigOptions, String indexType) {
        var gql = GqlHelper.getGql42001_22N04("indexConfig", "config option", java.util.List.of("indexProvider"));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s with specified index config '%s': %s indexes have no valid config values.",
                        schemaType, indexConfigOptions, indexType));
    }

    public static InvalidArgumentsException invalidIndexProviderSuggestIndex(
            String schemaDescription,
            String providerString,
            String indexDescription,
            String providerIndexType,
            List<String> indexProviders) {
        var indexProvidersString =
                "[" + indexProviders.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", ")) + "]";
        var gql = GqlHelper.getGql42001_22N04(providerString, "index provider type", indexProviders);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "To create %s index, please use 'CREATE %s INDEX ...'.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexDescription, providerIndexType, indexProvidersString));
    }

    public static InvalidArgumentsException invalidIndexProvider(
            Boolean correctCypherVersion,
            String schemaDescription,
            String providerString,
            List<String> indexProviders) {
        var indexProvidersString =
                "[" + indexProviders.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", ")) + "]";
        var gql = GqlHelper.getGql42001_22N04(providerString, "index provider type", indexProviders);
        var message = correctCypherVersion
                        && (providerString.equalsIgnoreCase("native-btree-1.0")
                                || providerString.equalsIgnoreCase("lucene+native-3.0"))
                ? String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "Invalid index type b-tree, use range, point or text index instead.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexProvidersString)
                : String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexProvidersString);
        return new InvalidArgumentsException(gql, message);
    }

    public static InvalidArgumentsException compositeUsingOptions(String operation) {
        var gql = GqlHelper.getGql22N81(
                GqlParams.StringParam.cmd.process("OPTIONS"),
                GqlParams.StringParam.cmd.process("CREATE COMPOSITE DATABASE"));
        return new InvalidArgumentsException(
                gql, String.format("Could not %s: composite databases have no valid options values.", operation));
    }

    public static InvalidArgumentsException noValidPropertyConstraintOptions(String entity, String constraintType) {
        var gql = GqlHelper.getGql22N81(
                GqlParams.StringParam.cmd.process("OPTIONS"),
                String.format("%s property %s constraints", entity, constraintType));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s property %s constraint: property %s constraints have no valid options values.",
                        entity, constraintType, constraintType));
    }

    public static InvalidArgumentsException connectionPoolSizeZeroNotAllowed(String operation, String pool_max_size) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, "0")
                .withParam(GqlParams.StringParam.context, operation)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N86)
                        .build())
                .build();
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver settings value for '%s'. Zero is not allowed.",
                        operation, pool_max_size));
    }

    public static InvalidArgumentsException optionRequiresInteger(String option, Object value) {
        var gql = GqlHelper.getGql22G03_22N27(option, value.getClass().getTypeName(), List.of("INTEGER"));
        return new InvalidArgumentsException(
                gql, String.format("Option `%s` requires integer argument, got `%s`", option, value));
    }

    public static InvalidArgumentsException inputContainsInvalidCharacters(
            String invalidInput, String context, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, invalidInput)
                .withParam(GqlParams.StringParam.context, context)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
                        .withParam(GqlParams.StringParam.input, invalidInput)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
        return new InvalidArgumentsException(gql, legacyMessage);
    }
}
