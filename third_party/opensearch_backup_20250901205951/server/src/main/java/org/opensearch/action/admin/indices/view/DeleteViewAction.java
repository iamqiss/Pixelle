/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.view;

import org.density.action.ActionRequestValidationException;
import org.density.action.ActionType;
import org.density.action.ValidateActions;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.ClusterManagerNodeRequest;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.View;
import org.density.cluster.service.ClusterService;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ConstructingObjectParser;
import org.density.core.xcontent.XContentParser;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/** Action to delete a view */
@SuppressWarnings("deprecation")
@ExperimentalApi
public class DeleteViewAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteViewAction INSTANCE = new DeleteViewAction();
    public static final String NAME = "cluster:admin/views/delete";

    public DeleteViewAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /** Request for delete view */
    @ExperimentalApi
    public static class Request extends ClusterManagerNodeRequest<Request> {
        private final String name;

        public Request(final String name) {
            this.name = name;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request that = (Request) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(name)) {
                validationException = ValidateActions.addValidationError("name cannot be empty or null", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "delete_view_request",
            args -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), View.NAME_FIELD);
        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Transport Action for deleting a View
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, AcknowledgedResponse> {

        private final ViewService viewService;

        @Inject
        public TransportAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final ViewService viewService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.viewService = viewService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected AcknowledgedResponse read(final StreamInput in) throws IOException {
            return new AcknowledgedResponse(in);
        }

        @Override
        protected void clusterManagerOperation(
            final Request request,
            final ClusterState state,
            final ActionListener<AcknowledgedResponse> listener
        ) throws Exception {
            viewService.deleteView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
