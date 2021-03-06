/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.trace.server.jersey.rest.core.tests.stubs;

import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Basic Implementation of the serialized trace model object used by clients.
 * Equality of two stubs is determined by equality of names, paths and
 * {@link UUID}, as the start time, end time and number of events may be unknown
 * due to incomplete indexing.
 *
 * @author Loic Prieur-Drevon
 */
public class TraceModelStub extends AbstractModelStub {

    /**
     * Generated Serial Version UID
     */
    private static final long serialVersionUID = -1030854786688167776L;

    private final String fPath;

    /**
     * {@link JsonCreator} Constructor for final fields
     *
     * @param name
     *            trace name
     * @param path
     *            path to trace on server file system
     * @param uuid
     *            the stub's UUID
     * @param nbEvents
     *            number of current indexed events
     * @param start
     *            start time
     * @param end
     *            end time
     */
    @JsonCreator
    public TraceModelStub(@JsonProperty("name") String name,
            @JsonProperty("path") String path,
            @JsonProperty("UUID") UUID uuid,
            @JsonProperty("nbEvents") long nbEvents,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end) {
        super(name, uuid, nbEvents, start, end);
        fPath = path;
    }

    /**
     * Constructor for comparing equality
     *
     * @param name
     *            trace name
     * @param path
     *            path to trace on server file system
     * @param uuid
     *            the stub's UUID
     */
    public TraceModelStub(String name, String path, UUID uuid) {
        this(name, path, uuid, 0, 0L, 0L);
    }

    /**
     * Getter for the path to the trace on the server's file system
     *
     * @return path
     */
    public String getPath() {
        return fPath;
    }

    @Override
    public String toString() {
        return getName() + ":<path=" + fPath + ", UUID=" + getUUID() + '>'; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fPath);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        } else if (obj instanceof TraceModelStub) {
            TraceModelStub other = (TraceModelStub) obj;
            return Objects.equals(fPath, other.fPath);
        }
        return false;
    }
}
