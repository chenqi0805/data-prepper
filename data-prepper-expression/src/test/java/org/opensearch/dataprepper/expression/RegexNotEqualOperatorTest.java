/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegexNotEqualOperatorTest {
    final RegexNotEqualOperator objectUnderTest = new RegexNotEqualOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is("!~"));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.eval("a", "a*"), is(false));
        assertThat(objectUnderTest.eval("a", "b*"), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval("a"));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval("a", "a", "a*"));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(1, "a*"));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval("a", 1));
    }

    @Test
    void testEvalInValidPattern() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval("a", "*"));
    }
}