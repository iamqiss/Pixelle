/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.common;

import org.density.core.common.Strings;
import org.density.core.xcontent.XContentParser;
import org.density.protobufs.SourceConfig;
import org.density.protobufs.SourceConfigParam;
import org.density.protobufs.SourceFilter;
import org.density.rest.RestRequest;
import org.density.search.fetch.subphase.FetchSourceContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SourceConfig Protocol Buffers to FetchSourceContext objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding Density objects.
 */
public class FetchSourceContextProtoUtils {

    private FetchSourceContextProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#parseFromRestRequest(RestRequest)}
     *
     * @param request The BulkRequest Protocol Buffer containing source configuration
     * @return A FetchSourceContext object based on the request parameters, or null if no source parameters are provided
     */
    public static FetchSourceContext parseFromProtoRequest(org.density.protobufs.BulkRequest request) {
        Boolean fetchSource = true;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        // Set up source context if source parameters are provided
        if (request.hasSource()) {
            switch (request.getSource().getSourceConfigParamCase()) {
                case SourceConfigParam.SourceConfigParamCase.BOOL_VALUE:
                    fetchSource = request.getSource().getBoolValue();
                    break;
                case SourceConfigParam.SourceConfigParamCase.STRING_ARRAY:
                    sourceIncludes = request.getSource().getStringArray().getStringArrayList().toArray(new String[0]);
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid sourceConfig provided.");
            }
        }

        if (request.getSourceIncludesList().size() > 0) {
            sourceIncludes = request.getSourceIncludesList().toArray(new String[0]);
        }

        if (request.getSourceExcludesList().size() > 0) {
            sourceExcludes = request.getSourceExcludesList().toArray(new String[0]);
        }
        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#parseFromRestRequest(RestRequest)}
     *
     * @param request The SearchRequest Protocol Buffer containing source configuration
     * @return A FetchSourceContext object based on the request parameters, or null if no source parameters are provided
     */
    public static FetchSourceContext parseFromProtoRequest(org.density.protobufs.SearchRequest request) {
        Boolean fetchSource = null;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        if (request.hasSource()) {
            SourceConfigParam source = request.getSource();

            if (source.hasBoolValue()) {
                fetchSource = source.getBoolValue();
            } else {
                sourceIncludes = source.getStringArray().getStringArrayList().toArray(new String[0]);
            }
        }

        if (request.getSourceIncludesCount() > 0) {
            sourceIncludes = request.getSourceIncludesList().toArray(new String[0]);
        }

        if (request.getSourceExcludesCount() > 0) {
            sourceExcludes = request.getSourceExcludesList().toArray(new String[0]);
        }

        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    /**
     * Converts a SourceConfig Protocol Buffer to a FetchSourceContext object.
     * Similar to {@link FetchSourceContext#fromXContent(XContentParser)}.
     *
     * @param sourceConfig The SourceConfig Protocol Buffer to convert
     * @return A FetchSourceContext object
     */
    public static FetchSourceContext fromProto(SourceConfig sourceConfig) {
        boolean fetchSource = true;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        if (sourceConfig.getSourceConfigCase() == SourceConfig.SourceConfigCase.FETCH) {
            fetchSource = sourceConfig.getFetch();
        } else if (sourceConfig.hasIncludes()) {
            ArrayList<String> list = new ArrayList<>();
            for (String string : sourceConfig.getIncludes().getStringArrayList()) {
                list.add(string);
            }
            includes = list.toArray(new String[0]);
        } else if (sourceConfig.hasFilter()) {
            SourceFilter sourceFilter = sourceConfig.getFilter();
            if (!sourceFilter.getIncludesList().isEmpty()) {
                List<String> includesList = new ArrayList<>();
                for (String s : sourceFilter.getIncludesList()) {
                    includesList.add(s);
                }
                includes = includesList.toArray(new String[0]);
            }
            if (!sourceFilter.getExcludesList().isEmpty()) {
                List<String> excludesList = new ArrayList<>();
                for (String s : sourceFilter.getExcludesList()) {
                    excludesList.add(s);
                }
                excludes = excludesList.toArray(new String[0]);
            }
        }
        return new FetchSourceContext(fetchSource, includes, excludes);
    }
}
