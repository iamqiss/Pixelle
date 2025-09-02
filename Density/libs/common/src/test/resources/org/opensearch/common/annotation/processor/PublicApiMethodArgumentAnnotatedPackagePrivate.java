/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.annotation.processor;

import org.density.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class PublicApiMethodArgumentAnnotatedPackagePrivate {
    public void method(AnnotatedPackagePrivate arg) {}
}

// The public API exposes this class through public method argument, it should be public
@PublicApi(since = "1.0.0")
class AnnotatedPackagePrivate {}
