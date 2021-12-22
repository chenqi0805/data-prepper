/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.amazon.dataprepper.model.trace.Span;
import com.google.common.collect.Sets;

import java.util.Set;

public class SpanSet {

    private final Set<Span> spans;
    private final long timeSeen;

    public SpanSet() {
        this.spans = Sets.newConcurrentHashSet();
        this.timeSeen = System.currentTimeMillis();
    }

    public Set<Span> getSpans() {
        return spans;
    }

    public long getTimeSeen() {
        return timeSeen;
    }

    public void addSpan(final Span span) {
        spans.add(span);
    }
}
