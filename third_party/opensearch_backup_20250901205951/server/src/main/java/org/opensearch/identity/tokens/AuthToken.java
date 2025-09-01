/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.tokens;

import org.density.common.annotation.ExperimentalApi;

/**
 * Interface for all token formats to support to authenticate user such as UserName/Password tokens, Access tokens, and more.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface AuthToken {

    String asAuthHeaderValue();

}
