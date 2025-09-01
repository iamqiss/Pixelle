/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.cache.store.builders;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.cache.ICache;
import org.density.common.cache.ICacheKey;
import org.density.common.cache.RemovalListener;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;

import java.util.function.ToLongBiFunction;

/**
 * Builder for store aware cache.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @density.experimental
 */
@ExperimentalApi
public abstract class ICacheBuilder<K, V> {

    private long maxWeightInBytes;

    private ToLongBiFunction<ICacheKey<K>, V> weigher;

    private TimeValue expireAfterAcess;

    private Settings settings;

    private RemovalListener<ICacheKey<K>, V> removalListener;

    private boolean statsTrackingEnabled = true;

    private int numberOfSegments;

    public ICacheBuilder() {}

    public ICacheBuilder<K, V> setMaximumWeightInBytes(long sizeInBytes) {
        this.maxWeightInBytes = sizeInBytes;
        return this;
    }

    public ICacheBuilder<K, V> setWeigher(ToLongBiFunction<ICacheKey<K>, V> weigher) {
        this.weigher = weigher;
        return this;
    }

    public ICacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAcess) {
        this.expireAfterAcess = expireAfterAcess;
        return this;
    }

    public ICacheBuilder<K, V> setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public ICacheBuilder<K, V> setRemovalListener(RemovalListener<ICacheKey<K>, V> removalListener) {
        this.removalListener = removalListener;
        return this;
    }

    public ICacheBuilder<K, V> setStatsTrackingEnabled(boolean statsTrackingEnabled) {
        this.statsTrackingEnabled = statsTrackingEnabled;
        return this;
    }

    public ICacheBuilder<K, V> setNumberOfSegments(int numberOfSegments) {
        this.numberOfSegments = numberOfSegments;
        return this;
    }

    public long getMaxWeightInBytes() {
        return maxWeightInBytes;
    }

    public TimeValue getExpireAfterAcess() {
        return expireAfterAcess;
    }

    public int getNumberOfSegments() {
        return numberOfSegments;
    }

    public ToLongBiFunction<ICacheKey<K>, V> getWeigher() {
        return weigher;
    }

    public RemovalListener<ICacheKey<K>, V> getRemovalListener() {
        return this.removalListener;
    }

    public Settings getSettings() {
        return settings;
    }

    public boolean getStatsTrackingEnabled() {
        return statsTrackingEnabled;
    }

    public abstract ICache<K, V> build();
}
