/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.list;

import org.density.action.pagination.PageParams;
import org.density.rest.RestRequest;
import org.density.rest.action.cat.AbstractCatAction;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Objects;

import static org.density.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;
import static org.density.action.pagination.PageParams.PARAM_DESC_SORT_VALUE;

/**
 * Base Transport action class for _list API.
 * Serves as a base class for APIs wanting to support pagination.
 * Existing _cat APIs can refer {@link org.density.rest.action.cat.RestIndicesAction}.
 * @density.api
 */
public abstract class AbstractListAction extends AbstractCatAction {

    private static final int DEFAULT_PAGE_SIZE = 100;
    protected PageParams pageParams;

    protected abstract void documentation(StringBuilder sb);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        boolean helpWanted = request.paramAsBoolean("help", false);
        if (helpWanted || isActionPaginated() == false) {
            return super.prepareRequest(request, client);
        }
        this.pageParams = validateAndGetPageParams(request);
        assert Objects.nonNull(pageParams) : "pageParams can not be null for paginated queries";
        return doCatRequest(request, client);
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }

    /**
     *
     * @return Metadata that can be extracted out from the rest request. Query params supported by the action specific
     * to pagination along with any respective validations to be added here.
     */
    protected PageParams validateAndGetPageParams(RestRequest restRequest) {
        PageParams pageParams = restRequest.parsePaginatedQueryParams(defaultSort(), defaultPageSize());
        // validating pageSize
        if (pageParams.getSize() <= 0) {
            throw new IllegalArgumentException("size must be greater than zero");
        }
        // Validating sort order
        if (!(PARAM_ASC_SORT_VALUE.equals(pageParams.getSort()) || PARAM_DESC_SORT_VALUE.equals(pageParams.getSort()))) {
            throw new IllegalArgumentException("value of sort can either be asc or desc");
        }
        return pageParams;
    }

    protected int defaultPageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    protected String defaultSort() {
        return PARAM_ASC_SORT_VALUE;
    }

}
