/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.delete;

import org.density.action.ActionRequestValidationException;
import org.density.test.DensityTestCase;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class DeleteRequestTests extends DensityTestCase {

    public void testValidation() {
        {
            final DeleteRequest request = new DeleteRequest("index4", "0");
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, nullValue());
        }

        {
            final DeleteRequest request = new DeleteRequest("index4", null);
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, not(nullValue()));
            assertThat(validate.validationErrors(), hasItems("id is missing"));
        }
    }
}
