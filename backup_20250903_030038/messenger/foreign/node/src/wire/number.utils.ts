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


export const boolToBuf = (v: boolean) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(!v ? 0 : 1);
  return b;
}

export const int8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeInt8(v);
  return b;
}

export const int16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeInt16LE(v);
  return b;
}

export const int32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeInt32LE(v);
  return b;
}

export const int64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigInt64LE(v);
  return b;
}

export const uint8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(v);
  return b;
}

export const uint16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16LE(v);
  return b;
}

export const uint32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32LE(v);
  return b;
}

export const uint64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64LE(v);
  return b;
}

export const floatToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeFloatLE(v);
  return b;
}

export const doubleToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(8);
  b.writeDoubleLE(v);
  return b;
}

// bigint => u128 LE
export function u128ToBuf(num: bigint, width = 16): Buffer {
  const hex = num.toString(16);
  const b = Buffer.from(hex.padStart(width * 2, '0').slice(0, width * 2), 'hex');
  return b.reverse();
}

// u128 LE => Bigint
export function u128LEBufToBigint(b: Buffer): bigint {
  const hex = b.reverse().toString('hex');
  return hex.length === 0 ? BigInt(0) :  BigInt(`0x${hex}`);
}

