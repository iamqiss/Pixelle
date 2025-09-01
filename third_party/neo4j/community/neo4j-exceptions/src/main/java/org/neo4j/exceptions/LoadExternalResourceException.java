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

import java.net.URL;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class LoadExternalResourceException extends Neo4jException {

    private LoadExternalResourceException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    private LoadExternalResourceException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static LoadExternalResourceException withInnerErrorMessage(String url, Throwable cause) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(String.valueOf(url)), cause.getMessage(), cause);
    }

    public static LoadExternalResourceException couldNotLoadExternalResource(String url, Throwable cause) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(String.valueOf(url)),
                String.format("Couldn't load the external resource at: %s", url),
                cause);
    }

    public static LoadExternalResourceException failedToAccessResource(URL url) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(String.valueOf(url)),
                String.format(
                        "LOAD CSV failed to access resource. The request to %s was at some point redirected to from which it could not proceed. This may happen if %s redirects to a resource which uses a different protocol than the original request.",
                        url, url));
    }

    public static LoadExternalResourceException invalidUrl(String url, Throwable cause) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(url),
                String.format("Invalid URL '%s': %s", url, cause.getMessage()),
                cause);
    }

    public static LoadExternalResourceException invalidUrlNoScheme(String url) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(url), String.format("Invalid URL '%s': no scheme", url));
    }

    public static LoadExternalResourceException invalidUrlNoProtocol(String url) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(url),
                String.format("Invalid URL '%s': no protocol: %s", url, url));
    }

    public static LoadExternalResourceException invalidUrlNoProtocol(String url, Throwable cause) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(url),
                String.format("Invalid URL '%s': no protocol: %s", url, url),
                cause);
    }

    public static LoadExternalResourceException cannotLoadFromUrl(String url, Throwable cause) {
        return new LoadExternalResourceException(
                unableToLoadExternalResourceErrorObject(url),
                String.format("Cannot load from URL '%s': %s", url, cause.getMessage()),
                cause);
    }

    private static ErrorGqlStatusObject unableToLoadExternalResourceErrorObject(String url) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N43)
                        .withParam(GqlParams.StringParam.url, url)
                        .build())
                .build();
    }

    @Override
    public Status status() {
        return Status.Statement.ExternalResourceFailed;
    }
}
