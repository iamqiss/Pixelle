/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage.idx.value;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.ReadableChannel;

public class PeekableChannel implements ReadableChannel {
    private byte peekedByte;
    private boolean peekedInUse;
    private final ReadableChannel channel;

    public PeekableChannel(ReadableChannel channel) {
        this.channel = channel;
    }

    @Override
    public byte get() throws IOException {
        if (peekedInUse) {
            peekedInUse = false;
            return peekedByte;
        } else {
            return channel.get();
        }
    }

    @Override
    public short getShort() throws IOException {
        return channel.getShort();
    }

    @Override
    public int getInt() throws IOException {
        return channel.getInt();
    }

    @Override
    public long getLong() throws IOException {
        return channel.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        return channel.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        return channel.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        channel.get(bytes, length);
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    byte peek() throws IOException {
        peekedByte = channel.get();
        peekedInUse = true;

        return peekedByte;
    }

    @Override
    public byte getVersion() throws IOException {
        return channel.getVersion();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return 0;
    }

    @Override
    public void beginChecksum() {}

    @Override
    public int getChecksum() {
        return 0;
    }

    @Override
    public int endChecksumAndValidate() {
        return 0;
    }

    @Override
    public long position() throws IOException {
        return 0;
    }

    @Override
    public void position(long byteOffset) throws IOException {}
}
