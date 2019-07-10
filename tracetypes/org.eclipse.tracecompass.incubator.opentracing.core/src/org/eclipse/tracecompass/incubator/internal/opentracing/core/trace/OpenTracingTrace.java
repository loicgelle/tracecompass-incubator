/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.core.trace;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.Activator;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.OpenTracingAspects;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.OpenTracingEvent;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.OpenTracingField;
import org.eclipse.tracecompass.internal.provisional.jsontrace.core.trace.JsonTrace;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.event.ITmfLostEvent;
import org.eclipse.tracecompass.tmf.core.event.aspect.ITmfEventAspect;
import org.eclipse.tracecompass.tmf.core.exceptions.TmfTraceException;
import org.eclipse.tracecompass.tmf.core.io.BufferedRandomAccessFile;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfContext;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceUtils;
import org.eclipse.tracecompass.tmf.core.trace.TraceValidationStatus;
import org.eclipse.tracecompass.tmf.core.trace.location.ITmfLocation;
import org.eclipse.tracecompass.tmf.core.trace.location.TmfLongLocation;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;

/**
 * Open Tracing trace. Can read jaeger unsorted or sorted JSON traces.
 *
 * @author Katherine Nadeau
 * @author Loïc Gelle
 *
 */
public class OpenTracingTrace extends JsonTrace {

    private class TraceFile {
        @SerializedName("data")
        public TraceData[] data;
    }

    private class TraceData {
        @SerializedName("traceID")
        public String traceID;

        @SerializedName("spans")
        public JsonObject[] spanData;
    }

    private final @NonNull Iterable<@NonNull ITmfEventAspect<?>> fEventAspects;
    private final Map<String, String> fProcesses;

    private HashMap<Long, String> fIndexToSpanData;

    /**
     * Constructor
     */
    @SuppressWarnings("null")
    public OpenTracingTrace() {
        fEventAspects = Lists.newArrayList(OpenTracingAspects.getAspects());
        fProcesses = new HashMap<>();
        fIndexToSpanData = new HashMap<>();
    }

    @Override
    public void initTrace(IResource resource, String path, Class<? extends ITmfEvent> type) throws TmfTraceException {
        super.initTrace(resource, path, type);
        fProperties.put("Type", "Open-Tracing"); //$NON-NLS-1$ //$NON-NLS-2$
        String dir = TmfTraceManager.getSupplementaryFileDir(this);
        fFile = new File(dir + new File(path).getName());
        if (!fFile.exists()) {
            Job sortJob = new OpenTracingSortingJob(this, path);
            sortJob.schedule();
            while (sortJob.getResult() == null) {
                try {
                    sortJob.join();
                } catch (InterruptedException e) {
                    throw new TmfTraceException(e.getMessage(), e);
                }
            }
            IStatus result = sortJob.getResult();
            if (!result.isOK()) {
                throw new TmfTraceException("Job failed " + result.getMessage()); //$NON-NLS-1$
            }
        }
        try {
            fFileInput = new BufferedRandomAccessFile(fFile, "r"); //$NON-NLS-1$
        } catch (IOException e) {
            throw new TmfTraceException(e.getMessage(), e);
        }

        // First pass through the trace
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new FileReader(path));
            TraceFile tf = gson.fromJson(reader, TraceFile.class);
            long i = 1;
            for (TraceData td : tf.data) {
                for (JsonObject sd : td.spanData) {
                    fIndexToSpanData.put(i, sd.toString());
                    i++;
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * Save the processes list
     *
     * @param path
     *            trace file path
     */
    public void registerProcesses(String path) {

    }

    @Override
    public IStatus validate(IProject project, String path) {
        File file = new File(path);
        if (!file.exists()) {
            return new Status(IStatus.ERROR, Activator.PLUGIN_ID, "File not found: " + path); //$NON-NLS-1$
        }
        if (!file.isFile()) {
            return new Status(IStatus.ERROR, Activator.PLUGIN_ID, "Not a file. It's a directory: " + path); //$NON-NLS-1$
        }
        try {
            if (!TmfTraceUtils.isText(file)) {
                return new TraceValidationStatus(0, Activator.PLUGIN_ID);
            }
        } catch (IOException e) {
            Activator.getInstance().logError("Error validating file: " + path, e); //$NON-NLS-1$
            return new Status(IStatus.ERROR, Activator.PLUGIN_ID, "IOException validating file: " + path, e); //$NON-NLS-1$
        }

        // Validate contents
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new FileReader(file));
            try {
                gson.fromJson(reader, TraceFile.class);
            } catch (JsonSyntaxException e) {
                return new TraceValidationStatus(0, Activator.PLUGIN_ID);
            }

        } catch (FileNotFoundException e) {
            return new Status(IStatus.ERROR, Activator.PLUGIN_ID, "File not found: " + path); //$NON-NLS-1$
        }

        return new TraceValidationStatus(MAX_CONFIDENCE, Activator.PLUGIN_ID);
    }

    @Override
    public Iterable<@NonNull ITmfEventAspect<?>> getEventAspects() {
        return fEventAspects;
    }

    @Override
    public ITmfEvent parseEvent(ITmfContext context) {
        @Nullable
        ITmfLocation location = context.getLocation();
        if (location instanceof TmfLongLocation) {
            TmfLongLocation tmfLongLocation = (TmfLongLocation) location;
            Long locationInfo = tmfLongLocation.getLocationInfo();
            if (location.equals(NULL_LOCATION)) {
                locationInfo = 0L;
            }
            String nextJson = fIndexToSpanData.get(locationInfo);
            if (nextJson != null) {
                String process = fProcesses.get(OpenTracingField.getProcess(nextJson));
                OpenTracingField field = OpenTracingField.parseJson(nextJson, process);
                if (field == null) {
                    return null;
                }
                return new OpenTracingEvent(this, context.getRank(), field);
            }
        }
        return null;
    }

    @Override
    protected synchronized void updateAttributes(final ITmfContext context, final @NonNull ITmfEvent event) {
        ITmfTimestamp timestamp = event.getTimestamp();
        Long duration = event.getContent().getFieldValue(Long.class, IOpenTracingConstants.DURATION);
        ITmfTimestamp endTime = duration != null ? TmfTimestamp.fromNanos(timestamp.toNanos() + duration) : timestamp;
        if (event instanceof ITmfLostEvent) {
            endTime = ((ITmfLostEvent) event).getTimeRange().getEndTime();
        }
        if (getStartTime().equals(TmfTimestamp.BIG_BANG) || (getStartTime().compareTo(timestamp) > 0)) {
            setStartTime(timestamp);
        }
        if (getEndTime().equals(TmfTimestamp.BIG_CRUNCH) || (getEndTime().compareTo(endTime) < 0)) {
            setEndTime(endTime);
        }
        if (context.hasValidRank()) {
            long rank = context.getRank();
            if (getNbEvents() <= rank) {
                setNbEvents(rank + 1);
            }
            if (getIndexer() != null) {
                getIndexer().updateIndex(context, timestamp);
            }
            ITmfLocation loc = context.getLocation();
            TmfLongLocation nextLoc;
            if (loc instanceof TmfLongLocation) {
                TmfLongLocation tmfLongLocation = (TmfLongLocation) loc;
                Long locationInfo = tmfLongLocation.getLocationInfo();
                nextLoc = new TmfLongLocation(locationInfo + 1);
            } else {
                nextLoc = new TmfLongLocation(1);
            }
            context.setLocation(nextLoc);
        }
    }

    @Override
    public synchronized ITmfEvent getNext(final ITmfContext context) {
        // parseEvent() does not update the context
        final ITmfEvent event = parseEvent(context);
        if (event != null) {
            updateAttributes(context, event);
            context.increaseRank();
        }
        return event;
    }
}
