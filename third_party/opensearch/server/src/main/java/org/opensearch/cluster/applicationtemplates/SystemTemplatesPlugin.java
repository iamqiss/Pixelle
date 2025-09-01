/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.applicationtemplates;

import org.density.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Plugin interface to expose the template maintaining logic.
 */
@ExperimentalApi
public interface SystemTemplatesPlugin {

    /**
     * @return repository implementation from which templates are to be fetched.
     */
    SystemTemplateRepository loadRepository() throws IOException;

    /**
     * @param templateInfo Metadata about the template to load
     * @return Implementation of TemplateLoader which determines how to make the template available at runtime.
     */
    SystemTemplateLoader loaderFor(SystemTemplateMetadata templateInfo);
}
