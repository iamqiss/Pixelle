/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { typedRoute } from '$lib/types/appRoutes';
import { tokens } from '$lib/utils/constants/tokens.js';
import { redirect, type Actions } from '@sveltejs/kit';

export const actions = {
  default({ cookies }) {
    // eat the cookie
    cookies.set(tokens.accessToken, '', {
      path: '/',
      expires: new Date(0)
    });

    console.log('deleting cookie');

    // redirect the user
    redirect(302, typedRoute('/auth/sign-in'));
  }
} satisfies Actions;
