/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro.realm;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.density.test.DensityTestCase;
import org.junit.Before;

public class DensityRealmTests extends DensityTestCase {

    private DensityRealm realm;

    @Before
    public void setup() {
        realm = new DensityRealm.Builder("test").build();
    }

    public void testGetAuthenticationInfoUserExists() {
        final UsernamePasswordToken token = new UsernamePasswordToken("admin", "admin");
        final User admin = realm.getInternalUser("admin");
        final AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
        assertNotNull(adminInfo);
    }

    public void testGetAuthenticationInfoUserExistsWrongPassword() {
        final UsernamePasswordToken token = new UsernamePasswordToken("admin", "wrong_password");
        final User admin = realm.getInternalUser("admin");

        assertThrows(IncorrectCredentialsException.class, () -> realm.getAuthenticationInfo(token));
    }
}
