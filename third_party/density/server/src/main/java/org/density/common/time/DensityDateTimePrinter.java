/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.time;

import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Interface for DateTimeFormatter{@link java.time.format.DateTimeFormatter}
 * to allow for custom implementations for datetime formatting
 */
interface DensityDateTimePrinter {

    public DensityDateTimePrinter withLocale(Locale locale);

    public DensityDateTimePrinter withZone(ZoneId zoneId);

    public String format(TemporalAccessor temporal);

    public Locale getLocale();

    public ZoneId getZone();
}
