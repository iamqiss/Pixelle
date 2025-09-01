/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.cluster.metadata.CryptoMetadata;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.crypto.MasterKeyProvider;

/**
 * Crypto plugin to provide support for custom key providers.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface CryptoKeyProviderPlugin {

    /**
     * Every call to this method should return a new key provider.
     * @param cryptoMetadata These are crypto settings needed for creation of a new key provider.
     * @return master key provider.
     */
    MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata);

    /**
     * One crypto plugin extension implementation refers to a unique key provider type.
     * @return key provider type
     */
    String type();
}
