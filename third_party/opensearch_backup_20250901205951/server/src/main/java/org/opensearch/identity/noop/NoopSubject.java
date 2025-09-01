/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.noop;

import org.density.identity.NamedPrincipal;
import org.density.identity.Subject;
import org.density.identity.UserSubject;
import org.density.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.Objects;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @density.internal
 */
public class NoopSubject implements UserSubject {

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Subject that = (Subject) obj;
        return Objects.equals(getPrincipal(), that.getPrincipal());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPrincipal());
    }

    @Override
    public String toString() {
        return "NoopSubject(principal=" + getPrincipal() + ")";
    }

    /**
     * Logs the user in
     */
    @Override
    public void authenticate(AuthToken AuthToken) {
        // Do nothing as noop subject is always logged in
    }
}
