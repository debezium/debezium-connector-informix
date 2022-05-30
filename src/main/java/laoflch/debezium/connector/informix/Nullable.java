/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

public interface Nullable {

    /**
     * @return true if this object has real value, false if it is NULL object
     */
    boolean isAvailable();

}
