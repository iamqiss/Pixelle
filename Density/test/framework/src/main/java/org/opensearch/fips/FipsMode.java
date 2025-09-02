/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.fips;

import org.bouncycastle.crypto.CryptoServicesRegistrar;

public class FipsMode {

    public static Check CHECK = () -> {
        try {
            return CryptoServicesRegistrar.isInApprovedOnlyMode();
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            return false;
        }
    };

    @FunctionalInterface
    public interface Check {
        boolean isFipsEnabled();
    }
}
