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
 * Interface to load template into the Density runtime.
 */
@ExperimentalApi
public interface SystemTemplateLoader {

    /**
     * @param template Templated to be loaded
     * @throws IOException If an exceptional situation is encountered while parsing/loading the template
     */
    boolean loadTemplate(SystemTemplate template) throws IOException;
}
