/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.common.inject.binder;

import org.density.common.annotation.PublicApi;
import org.density.common.inject.Key;
import org.density.common.inject.Provider;
import org.density.common.inject.TypeLiteral;

/**
 * See the EDSL examples at {@link org.density.common.inject.Binder}.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public interface LinkedBindingBuilder<T> extends ScopedBindingBuilder {

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     */
    ScopedBindingBuilder to(Class<? extends T> implementation);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     */
    ScopedBindingBuilder to(TypeLiteral<? extends T> implementation);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     */
    ScopedBindingBuilder to(Key<? extends T> targetKey);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     *
     * @see org.density.common.inject.Injector#injectMembers
     */
    void toInstance(T instance);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     *
     * @see org.density.common.inject.Injector#injectMembers
     */
    ScopedBindingBuilder toProvider(Provider<? extends T> provider);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     */
    ScopedBindingBuilder toProvider(Class<? extends Provider<? extends T>> providerType);

    /**
     * See the EDSL examples at {@link org.density.common.inject.Binder}.
     */
    ScopedBindingBuilder toProvider(Key<? extends Provider<? extends T>> providerKey);
}
