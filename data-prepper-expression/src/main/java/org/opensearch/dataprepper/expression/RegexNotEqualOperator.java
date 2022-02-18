/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkArgument;

public class RegexNotEqualOperator implements Operator<Boolean> {
    @Override
    public String getSymbol() {
        return "!~";
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        checkArgument(args[0] instanceof String, "Left operand needs to be String.");
        checkArgument(args[1] instanceof String, "Right Operand needs to be String.");
        try {
            return !((String) args[0]).matches((String) args[1]);
        } catch (final PatternSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
