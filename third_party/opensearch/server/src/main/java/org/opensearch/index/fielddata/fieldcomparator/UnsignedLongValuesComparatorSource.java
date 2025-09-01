/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.density.common.Nullable;
import org.density.common.Numbers;
import org.density.common.util.BigArrays;
import org.density.index.fielddata.FieldData;
import org.density.index.fielddata.IndexFieldData;
import org.density.index.fielddata.IndexNumericFieldData;
import org.density.index.fielddata.LeafNumericFieldData;
import org.density.index.fielddata.LongToSortedNumericUnsignedLongValues;
import org.density.index.fielddata.SortedNumericUnsignedLongValues;
import org.density.index.search.comparators.UnsignedLongComparator;
import org.density.search.DocValueFormat;
import org.density.search.MultiValueMode;
import org.density.search.sort.BucketedSort;
import org.density.search.sort.SortOrder;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Comparator source for unsigned long values.
 *
 * @density.internal
 */
public class UnsignedLongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public UnsignedLongValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    private SortedNumericUnsignedLongValues loadDocValues(LeafReaderContext context) {
        final LeafNumericFieldData data = indexFieldData.load(context);
        return new LongToSortedNumericUnsignedLongValues(data.getLongValues());
    }

    private NumericDocValues getNumericDocValues(LeafReaderContext context, BigInteger missingValue) throws IOException {
        final SortedNumericUnsignedLongValues values = loadDocValues(context);
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        }
        final BitSet rootDocs = nested.rootDocs(context);
        final DocIdSetIterator innerDocs = nested.innerDocs(context);
        final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
        return sortMode.select(values, missingValue.longValue(), rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
    }

    @Override
    public Object missingObject(Object missingValue, boolean reversed) {
        if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
            final boolean min = sortMissingFirst(missingValue) ^ reversed;
            return min ? Numbers.MIN_UNSIGNED_LONG_VALUE : Numbers.MAX_UNSIGNED_LONG_VALUE;
        } else {
            if (missingValue instanceof Number) {
                return Numbers.toUnsignedLongExact((Number) missingValue);
            } else {
                BigInteger missing = new BigInteger(missingValue.toString());
                if (missing.signum() < 0) {
                    throw new IllegalArgumentException("Value [" + missingValue + "] is out of range for an unsigned long");
                }
                return missing;
            }
        }
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final BigInteger ulMissingValue = (BigInteger) missingObject(missingValue, reversed);
        return new UnsignedLongComparator(numHits, fieldname, ulMissingValue, reversed, filterPruning(pruning)) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new UnsignedLongLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return UnsignedLongValuesComparatorSource.this.getNumericDocValues(context, ulMissingValue);
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return new BucketedSort.ForUnsignedLongs(bigArrays, sortOrder, format, bucketSize, extra) {
            private final BigInteger ulMissingValue = (BigInteger) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDocValues docValues = getNumericDocValues(ctx, ulMissingValue);
                    private long docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.longValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected long docValue() {
                        return docValue;
                    }
                };
            }
        };
    }

}
