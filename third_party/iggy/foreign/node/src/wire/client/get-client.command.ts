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


import type { CommandResponse } from '../../client/client.type.js';
import { deserializeClient, type Client } from './client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type GetClient = {
  clientId: number
};

// GET CLIENT by id
export const GET_CLIENT = {
  code: COMMAND_CODE.GetClient,

  serialize: ({ clientId }: GetClient): Buffer => {
    const b = Buffer.alloc(4);
    b.writeUInt32LE(clientId);
    return b;
  },

  deserialize: (r: CommandResponse) => {
    if(r.status === 0 && r.length === 0)
      return null;
    return deserializeClient(r.data).data
  }
};

export const getClient = wrapCommand<GetClient, Client | null>(GET_CLIENT);
