/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.sort.plugin;

import org.density.plugins.Plugin;
import org.density.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class CustomSortPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SortSpec<?>> getSorts() {
        return Collections.singletonList(new SortSpec<>(CustomSortBuilder.NAME, CustomSortBuilder::new, CustomSortBuilder::fromXContent));
    }
}
