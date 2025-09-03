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


import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';

describe('e2e -> token', async () => {

  const c = getTestClient();

  const tokenName = 'yolo-token-test';
  
  it('e2e -> token::create', async () => {
    const tk = await c.token.create({ name: tokenName, expiry: 1800n });
    assert.ok(tk.token.length > 1);
  });

  it('e2e -> token::list', async () => {
    const tks = await c.token.list();
    assert.ok(tks.length > 0);
  });

  it('e2e -> token::delete', async () => {
    assert.ok(await c.token.delete({name: tokenName}));
  });
  
  it('e2e -> token::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
