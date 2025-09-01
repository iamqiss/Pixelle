/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.dht;

import java.math.BigInteger;

import accord.local.ShardDistributor;
import accord.primitives.Range;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;

import static java.math.BigInteger.ZERO;

public abstract class AccordSplitter implements ShardDistributor.EvenSplit.Splitter<BigInteger>
{
    abstract BigInteger valueForToken(Token token);
    abstract Token tokenForValue(BigInteger value);
    abstract BigInteger minimumValue();
    abstract BigInteger maximumValue();

    @Override
    public BigInteger sizeOf(accord.primitives.Range range)
    {
        // note: minimum value
        TokenKey startBound = (TokenKey)range.start();
        TokenKey endBound = (TokenKey)range.end();
        BigInteger start = startBound.isMin() ? minimumValue() : valueForToken(((TokenKey)range.start()).token());
        BigInteger end = endBound.isMax() ? maximumValue() : valueForToken(((TokenKey)range.end()).token());
        return end.subtract(start);
    }

    @Override
    public TokenRange subRange(accord.primitives.Range range, BigInteger startOffset, BigInteger endOffset)
    {
        TokenKey startBound = (TokenKey)range.start();
        TokenKey endBound = (TokenKey)range.end();

        BigInteger start = startBound.isMin() ? minimumValue() : valueForToken(startBound.token());
        BigInteger end = endBound.isMax() ? maximumValue() : valueForToken(endBound.token());
        BigInteger sizeOfRange = end.subtract(start);

        TableId tableId = startBound.table();
        return TokenRange.create(startOffset.equals(ZERO) ? startBound : new TokenKey(tableId, tokenForValue(start.add(startOffset))),
                                 endOffset.compareTo(sizeOfRange) >= 0 ? endBound : new TokenKey(tableId, tokenForValue(start.add(endOffset))));
    }

    @Override
    public boolean splittable(Range range, int numSplits)
    {
        return sizeOf(range).compareTo(BigInteger.valueOf(numSplits)) >= 0;
    }

    @Override
    public BigInteger zero()
    {
        return ZERO;
    }

    @Override
    public BigInteger valueOf(int v)
    {
        return BigInteger.valueOf(v);
    }


    @Override
    public BigInteger add(BigInteger a, BigInteger b)
    {
        return a.add(b);
    }

    @Override
    public BigInteger subtract(BigInteger a, BigInteger b)
    {
        return a.subtract(b);
    }

    @Override
    public BigInteger divide(BigInteger a, int i)
    {
        return a.divide(BigInteger.valueOf(i));
    }

    @Override
    public BigInteger divide(BigInteger a, BigInteger i)
    {
        return a.divide(i);
    }


    @Override
    public BigInteger multiply(BigInteger a, int i)
    {
        return a.multiply(BigInteger.valueOf(i));
    }

    @Override
    public int min(BigInteger v, int i)
    {
        return v.min(BigInteger.valueOf(i)).intValue();
    }

    @Override
    public int compare(BigInteger a, BigInteger b)
    {
        return a.compareTo(b);
    }
}
