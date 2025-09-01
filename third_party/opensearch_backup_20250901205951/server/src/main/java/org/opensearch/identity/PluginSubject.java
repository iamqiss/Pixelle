/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity;

import org.density.common.annotation.ExperimentalApi;

/**
 * Similar to {@link Subject}, but represents a plugin executing actions
 *
 * @density.experimental
 */
@ExperimentalApi
public interface PluginSubject extends Subject {}
