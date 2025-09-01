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
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.tasks.Task;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import static org.density.action.ValidateActions.addValidationError;

/** Action to create a view */
@ExperimentalApi
public class SearchViewAction extends ActionType<SearchResponse> {

    public static final SearchViewAction INSTANCE = new SearchViewAction();
    public static final String NAME = "views:data/read/search";

    private SearchViewAction() {
        super(NAME, SearchResponse::new);
    }

    /**
     * Wraps the functionality of search requests and tailors for what is available
     * when searching through views
     */
    @ExperimentalApi
    public static class Request extends SearchRequest {

        private final String view;

        public Request(final String view, final SearchRequest searchRequest) {
            super(searchRequest);
            this.view = view;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            view = in.readString();
        }

        public String getView() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return view.equals(that.view) && super.equals(that);
        }

        @Override
        public int hashCode() {
            return Objects.hash(view, super.hashCode());
        }

        @Override
        public ActionRequestValidationException validate() {
            final Function<String, String> unsupported = (String x) -> x + " is not supported when searching views";
            ActionRequestValidationException validationException = super.validate();

            if (scroll() != null) {
                validationException = addValidationError(unsupported.apply("Scroll"), validationException);
            }

            // TODO: Filter out any additional search features that are not supported.
            // Required before removing @ExperimentalApi annotations.

            if (Strings.isNullOrEmpty(view)) {
                validationException = addValidationError("View is required", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(view);
        }

        @Override
        public String toString() {
            return super.toString().replace("SearchRequest{", "SearchViewAction.Request{view=" + view + ",");
        }
    }

    /**
     * Transport Action for searching a View
     */
    public static class TransportAction extends HandledTransportAction<Request, SearchResponse> {

        private final ViewService viewService;

        @Inject
        public TransportAction(final TransportService transportService, final ActionFilters actionFilters, final ViewService viewService) {
            super(NAME, transportService, actionFilters, Request::new);
            this.viewService = viewService;
        }

        @Override
        protected void doExecute(final Task task, final Request request, final ActionListener<SearchResponse> listener) {
            viewService.searchView(request, listener);
        }
    }
}
