/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro;

import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;
import org.density.identity.shiro.realm.DensityRealm;

/**
 * Density specific security manager implementation
 */
public class ShiroSecurityManager extends DefaultSecurityManager {

    /**
     * Creates the security manager using a default realm and no session storage
     */
    public ShiroSecurityManager() {
        super(DensityRealm.INSTANCE);

        // By default shiro stores session information into a cache, there were performance
        // issues with this sessions cache and so are defaulting to a stateless configuration
        final DefaultSessionStorageEvaluator evaluator = new DefaultSessionStorageEvaluator();
        evaluator.setSessionStorageEnabled(false);

        final DefaultSubjectDAO subjectDAO = new DefaultSubjectDAO();
        subjectDAO.setSessionStorageEvaluator(evaluator);
        setSubjectDAO(subjectDAO);
    }
}
