/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.noop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.identity.IdentityService;
import org.density.identity.Subject;
import org.density.identity.tokens.AuthToken;
import org.density.identity.tokens.OnBehalfOfClaims;
import org.density.identity.tokens.TokenManager;

/**
 * This class represents a Noop Token Manager
 */
public class NoopTokenManager implements TokenManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    /**
     * Issue a new Noop Token
     * @return a new Noop Token
     */
    @Override
    public AuthToken issueOnBehalfOfToken(final Subject subject, final OnBehalfOfClaims claims) {
        return new AuthToken() {
            @Override
            public String asAuthHeaderValue() {
                return "noopToken";
            }
        };
    }

    /**
     * Issue a new Noop Token
     * @return a new Noop Token
     */
    @Override
    public AuthToken issueServiceAccountToken(final String audience) {
        return new AuthToken() {
            @Override
            public String asAuthHeaderValue() {
                return "noopToken";
            }
        };
    }
}
