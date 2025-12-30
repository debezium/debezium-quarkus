/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.List;

public interface CapturingEvents<V> {

    List<V> records();

    /**
     * @return logical destination for which the event are intended
     */
    String destination();

    /**
     *
     * @return logical source for which the events are intended
     */
    String source();

    /***
     * @return engine for which the events are emitted
     */
    String engine();
}
