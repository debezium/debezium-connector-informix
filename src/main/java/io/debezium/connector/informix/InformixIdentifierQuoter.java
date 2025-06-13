/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InformixIdentifierQuoter {
    private static final Pattern UNQUOTED = Pattern.compile("[\\p{Lower}\\p{Digit}_$]*");

    public static String quoteIfNecessary(String identifier) {
        Matcher matcher = UNQUOTED.matcher(identifier);

        if (!matcher.matches() && !(identifier.startsWith("\"") && identifier.endsWith("\""))) {
            return "\"%s\"".formatted(identifier);
        }

        return identifier;
    }
}
