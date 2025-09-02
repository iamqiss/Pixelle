/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.secure_sm.policy;

import java.util.List;

public record GrantEntry(String codeBase, List<PermissionEntry> permissionEntries) {
}
