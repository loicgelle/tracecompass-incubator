/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife;

import java.util.List;
import java.util.Objects;

import org.eclipse.tracecompass.tmf.core.model.timegraph.IElementResolver;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphEntryModel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * {@link TimeGraphEntryModel} for the Span life data provider
 *
 * @author Katherine Nadeau
 *
 */
public class SpanLifeEntryModel extends TimeGraphEntryModel implements IElementResolver {

    public enum EntryType {
        SPAN, KERNEL
    }

    /**
     * Log event
     */
    public static class LogEvent {
        private final long fTime;
        private final String fType;

        /**
         * Constructor
         *
         * @param time
         *            timestamp of the log
         * @param type
         *            type of the log
         */
        public LogEvent(long time, String type) {
            fTime = time;
            fType = type;
        }

        /**
         * Timestamp of the event
         *
         * @return timestamp
         */
        public long getTime() {
            return fTime;
        }

        /**
         * Type of event
         *
         * @return event
         */
        public String getType() {
            return fType;
        }
    }

    private final List<LogEvent> fLogs;

    private final boolean fErrorTag;

    private final String fProcessName;

    private final Integer fTid;

    private final EntryType fType;

    private final String fHostId;

    private final String fSpanUID;

    /**
     * Constructor
     *
     * @param id
     *            Entry ID
     * @param parentId
     *            Parent ID
     * @param name
     *            Entry name to be displayed
     * @param startTime
     *            Start time
     * @param endTime
     *            End time
     * @param logs
     *            Span logs timestamps
     * @param errorTag
     *            true if the span has an error tag
     * @param processName
     *            process name
     * @param tid
     *            process TID
     * @param type
     *            Entry type (kernel or span)
     * @param hostId
     *            Host ID
     */
    public SpanLifeEntryModel(long id, long parentId, String name, long startTime, long endTime,
                              List<LogEvent> logs, boolean errorTag, String processName,
                              Integer tid, EntryType type, String hostId, String spanUID, String spanID) {
        super(id, parentId, "[" + spanID + "] " + name, startTime, endTime);
        fLogs = logs;
        fErrorTag = errorTag;
        fProcessName = processName;
        fTid = tid;
        fType = type;
        fHostId = hostId;
        fSpanUID = spanUID;
    }

    /**
     * Getter for the logs
     *
     * @return the logs timestamps
     */
    public List<LogEvent> getLogs() {
        return fLogs;
    }

    /**
     * Getter for the error tag
     *
     * @return true if the span was tagged with an error
     */
    public boolean getErrorTag() {
        return fErrorTag;
    }

    /**
     * Getter for the process name
     *
     * @return the name of the process of the span
     */
    public String getProcessName() {
        return fProcessName;
    }

    /**
     * Getter for the TID
     *
     * @return the TID of the process of the span
     */
    public Integer getTid() {
        return fTid;
    }

    /**
     * Getter for the type
     *
     * @return the type of the entry
     */
    public EntryType getType() {
        return fType;
    }

    /**
     * Getter for the type
     *
     * @return the host ID
     */
    public String getHostId() {
        return fHostId;
    }

    @Override
    public Multimap<String, String> getMetadata() {
        // TODO Auto-generated method stub
        Multimap<String, String> fAspects = HashMultimap.create();
        fAspects.put(Objects.requireNonNull(org.eclipse.tracecompass.analysis.os.linux.core.event.aspect.Messages.AspectName_Tid),
                String.valueOf(getTid()));
        fAspects.put("SpanUID", fSpanUID);
        return fAspects;
    }

}
