/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.tracecompass.analysis.os.linux.core.event.aspect.LinuxTidAspect;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateValueTypeException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.statesystem.AbstractTmfStateProvider;
import org.eclipse.tracecompass.tmf.core.statesystem.ITmfStateProvider;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceUtils;

import com.google.common.collect.Iterables;

/**
 * Span life state provider
 *
 * @author Katherine Nadeau
 *
 */
public class SpanLifeStateProvider extends AbstractTmfStateProvider {

    /**
     * Quark name for open tracing spans
     */
    public static final String OPEN_TRACING_ATTRIBUTE = "openTracingSpans"; //$NON-NLS-1$
    public static final String CP_TIDS_ATTRIBUTE = "CriticalPathHostIDs"; //$NON-NLS-1$

    /**
     * Quark name for ust spans
     */
    public static final String UST_ATTRIBUTE = "ustSpans"; //$NON-NLS-1$

    private final Map<String, Integer> fSpanMap;

    private final Map<String, Consumer<ITmfEvent>> fHandlers;

    private final List<ITmfEvent> fSpanEventList;

    private final Map<String, ITmfEvent> fStartSpanUSTEvents;

    private final Map<String, ITmfEvent> fFinishSpanUSTEvents;

    private final Map<Integer, Set<Integer>> fTidToQuarks;

    private final Map<Integer, Long> fQuarkToEndTimestamp;

    private final Map<Integer, Long> fCPQuarkToEndTimestamp;

    /**
     * Constructor
     *
     * @param trace
     *            the trace to follow
     */
    public SpanLifeStateProvider(ITmfTrace trace) {
        super(trace, SpanLifeAnalysis.ID);
        fSpanMap = new HashMap<>();
        fHandlers = new HashMap<>();
        fSpanEventList = new LinkedList<>();
        fStartSpanUSTEvents = new HashMap<>();
        fFinishSpanUSTEvents = new HashMap<>();
        fQuarkToEndTimestamp = new HashMap<>();
        fCPQuarkToEndTimestamp = new HashMap<>();
        fTidToQuarks = new HashMap<>();
        fHandlers.put("OpenTracingSpan", this::handleAddSpanToQueue); //$NON-NLS-1$
        fHandlers.put("jaeger_ust:start_span", this::handleStartUSTEvent); //$NON-NLS-1$
        fHandlers.put("jaeger_ust:end_span", this::handleEndUSTEvent); //$NON-NLS-1$
        fHandlers.put("lttng_jul:event", this::handleUSTJulEvent); //$NON-NLS-1$
    }

    @Override
    public int getVersion() {
        return 3;
    }

    @Override
    public @NonNull ITmfStateProvider getNewInstance() {
        return new SpanLifeStateProvider(getTrace());
    }

    @Override
    protected void eventHandle(@NonNull ITmfEvent event) {
        Consumer<ITmfEvent> handler = fHandlers.get(event.getType().getName());
        if (handler != null) {
            handler.accept(event);
        }
    }

    private void handleAddSpanToQueue(ITmfEvent event) {
        fSpanEventList.add(event);
    }

    private void handleUSTJulEvent(ITmfEvent event) {
        ITmfStateSystemBuilder ss = getStateSystemBuilder();
        String loggerName = event.getContent().getFieldValue(String.class, "logger_name"); //$NON-NLS-1$
        if (loggerName == null || !loggerName.equals("io.jaegertracing.internal.JaegerTracer")) { //$NON-NLS-1$
            return;
        }
        String methodName = event.getContent().getFieldValue(String.class, "method_name"); //$NON-NLS-1$
        String msg = event.getContent().getFieldValue(String.class, "msg"); //$NON-NLS-1$
        if (methodName == null || msg == null) {
            return;
        }
        String[] splitMsg = msg.split(" ", 2); //$NON-NLS-1$
        String[] splitSpanID = splitMsg[0].split(":"); //$NON-NLS-1$
        if (splitSpanID.length < 2) {
            return;
        }
        String spanUID = splitSpanID[0] + ':' + splitSpanID[1];
        if (methodName.equals("start")) { //$NON-NLS-1$
            fStartSpanUSTEvents.put(spanUID, event);
        } else if (methodName.equals("finishWithDuration")) { //$NON-NLS-1$
            fFinishSpanUSTEvents.put(spanUID, event);
        } else if (methodName.equals("log")) {
            if (splitMsg.length < 2 || ss == null) {
                return;
            }
            int traceQuark = ss.getQuarkAbsoluteAndAdd(splitSpanID[0]);
            Integer logsQuark = ss.getQuarkRelativeAndAdd(traceQuark, IOpenTracingConstants.LOGS);
            Integer logQuark = ss.getQuarkRelativeAndAdd(logsQuark, splitSpanID[1]);
            ss.modifyAttribute(event.getTimestamp().toNanos(), "event:" + splitMsg[1].replace("\n",  " "), logQuark); //$NON-NLS-1$
            ss.modifyAttribute(event.getTimestamp().toNanos() + 1, (Object) null, logQuark);
        }
    }

    private void handleSpan(ITmfEvent event, ITmfStateSystemBuilder ss) {
        long timestamp = event.getTimestamp().toNanos();
        Integer tid = 0;
        String hostId = "";
        Long duration = event.getContent().getFieldValue(Long.class, IOpenTracingConstants.DURATION);
        if (duration == null) {
            return;
        }

        String traceId = event.getContent().getFieldValue(String.class, IOpenTracingConstants.TRACE_ID);
        int traceQuark = ss.getQuarkAbsoluteAndAdd(traceId);

        int openTracingSpansQuark = ss.getQuarkRelativeAndAdd(traceQuark, OPEN_TRACING_ATTRIBUTE);

        Boolean errorTag = Boolean.parseBoolean(event.getContent().getFieldValue(String.class, IOpenTracingConstants.TAGS + "/error")); //$NON-NLS-1$
        String processName = event.getContent().getFieldValue(String.class, IOpenTracingConstants.PROCESS_NAME);

        int spanQuark;
        Integer cpQuark = null;
        String name = String.valueOf(TmfTraceUtils.resolveAspectOfNameForEvent(event.getTrace(), "Name", event)); //$NON-NLS-1$
        String spanId = event.getContent().getFieldValue(String.class, IOpenTracingConstants.SPAN_ID);

        String spanUID = traceId + ':' + spanId;
        ITmfEvent startEvent = fStartSpanUSTEvents.get(spanUID);
        ITmfEvent finishEvent = fFinishSpanUSTEvents.get(spanUID);
        if (startEvent != null) {
            timestamp = startEvent.getTimestamp().toNanos();
            tid = (Integer) TmfTraceUtils.resolveEventAspectOfClassForEvent(
                    startEvent.getTrace(), LinuxTidAspect.class, startEvent);
            if (tid == null) {
                tid = 0;
            } else {
                hostId = startEvent.getTrace().getHostId();
                cpQuark = ss.getQuarkAbsoluteAndAdd(CP_TIDS_ATTRIBUTE,
                                                    tid + "/" + hostId);
                ss.modifyAttribute(timestamp, 1, cpQuark);
            }
        }
        if (finishEvent != null) {
            duration = finishEvent.getTimestamp().toNanos() - timestamp;
        }
        if (cpQuark != null) {
            final Long ts = timestamp;
            final Long dur = duration;
            fCPQuarkToEndTimestamp.merge(cpQuark, ts + dur,
                    (k, v) -> v > ts + dur ? v : ts + dur);
        }

        String refId = event.getContent().getFieldValue(String.class, IOpenTracingConstants.REFERENCES + "/CHILD_OF"); //$NON-NLS-1$
        if (refId == null) {
            spanQuark = ss.getQuarkRelativeAndAdd(openTracingSpansQuark,
                    name + '/' + spanId + '/' + errorTag + '/' + processName + '/' + tid + '/' + hostId);
        } else {
            Integer parentQuark = fSpanMap.get(refId);
            if (parentQuark == null) {
                return;
            }
            spanQuark = ss.getQuarkRelativeAndAdd(parentQuark,
                    name + '/' + spanId + '/' + errorTag + '/' + processName + '/' + tid + '/' + hostId);
        }

        if (tid > 0) {
            correctSpanRunningStatus(ss, tid, timestamp, duration);
            Set<Integer> quarksSet = fTidToQuarks.get(tid);
            if (quarksSet == null) {
                quarksSet = new HashSet<>();
            }
            quarksSet.add(spanQuark);
            fTidToQuarks.put(tid,  quarksSet);
        }

        ss.modifyAttribute(timestamp, String.valueOf(tid), spanQuark);

        /*Map<Long, Map<String, String>> logs = event.getContent().getFieldValue(Map.class, IOpenTracingConstants.LOGS);
        if (logs != null) {
            // We put all the logs in the state system under the LOGS attribute
            Integer logsQuark = ss.getQuarkRelativeAndAdd(traceQuark, IOpenTracingConstants.LOGS);
            for (Map.Entry<Long, Map<String, String>> log : logs.entrySet()) {
                List<String> logString = new ArrayList<>();
                for (Map.Entry<String, String> entry : log.getValue().entrySet()) {
                    logString.add(entry.getKey() + ':' + entry.getValue());
                }
                // One attribute for each span where each state value is the logs at the
                // timestamp
                // corresponding to the start time of the state
                Integer logQuark = ss.getQuarkRelativeAndAdd(logsQuark, spanId);
                Long logTimestamp = log.getKey();
                ss.modifyAttribute(logTimestamp, String.join("~", logString), logQuark); //$NON-NLS-1$
                ss.modifyAttribute(logTimestamp + 1, (Object) null, logQuark);
            }
        }*/

        fQuarkToEndTimestamp.put(spanQuark, timestamp + duration);
        if (spanId != null) {
            fSpanMap.put(spanId, spanQuark);
        }
    }

    private void correctSpanRunningStatus(ITmfStateSystemBuilder ss, Integer tid,
            long timestamp, long duration) {
        Set<Integer> quarksToCorrect = fTidToQuarks.get(tid);
        if (quarksToCorrect == null) {
            return;
        }
        List<ITmfStateInterval> singleStates = new ArrayList<>();
        for (Integer quark : quarksToCorrect) {
            try {
                singleStates.add(ss.querySingleState(timestamp, quark));
            } catch (IndexOutOfBoundsException | TimeRangeException | StateValueTypeException | StateSystemDisposedException e) {
            }
            try {
                singleStates.add(ss.querySingleState(timestamp + duration, quark));
            } catch (IndexOutOfBoundsException | TimeRangeException | StateValueTypeException | StateSystemDisposedException e) {
            }
        }
        Iterable<ITmfStateInterval> queriedStates = new ArrayList<>();
        try {
            queriedStates = ss.query2D(quarksToCorrect, timestamp, timestamp + duration);
        } catch (IndexOutOfBoundsException | TimeRangeException | StateValueTypeException | StateSystemDisposedException e) {
        }
        for (ITmfStateInterval interval :
            Iterables.concat(queriedStates, singleStates)) {
            Integer quark = interval.getAttribute();
            String quarkValue = (String) interval.getValue();
            if (quarkValue == null) {
                continue;
            }
            Long spanEndTimestamp = fQuarkToEndTimestamp.get(quark);
            if (spanEndTimestamp == null || spanEndTimestamp < timestamp) {
                continue;
            }
            if (quarkValue.equals(String.valueOf(tid))) {
                ss.modifyAttribute(timestamp, "0", quark);
                if (spanEndTimestamp > timestamp + duration) {
                    ss.modifyAttribute(timestamp + duration, quarkValue, quark);
                }
            }
        }
    }

    private void handleStartUSTEvent(ITmfEvent event) {
        ITmfStateSystemBuilder ss = getStateSystemBuilder();
        if (ss == null) {
            return;
        }
        String traceIdHigh = event.getContent().getFieldValue(String.class,
                                                              "trace_id_high");
        String traceIdLow = event.getContent().getFieldValue(String.class,
                                                             "trace_id_low");
        String spanId = event.getContent().getFieldValue(String.class,
                                                         "span_id");
        if (spanId == null || traceIdHigh == null || traceIdLow == null) {
            return;
        }
        Long spanIdLong = Long.valueOf(spanId);
        BigInteger traceId = new BigInteger(traceIdHigh);
        traceId = traceId.shiftLeft(64);
        traceId = traceId.add(new BigInteger(traceIdLow));
        String traceIdStr = traceId.toString(16);
        String spanUID = traceIdStr + ':' + Long.toHexString(spanIdLong);
        fStartSpanUSTEvents.put(spanUID, event);
    }

    private void handleEndUSTEvent(ITmfEvent event) {
        ITmfStateSystemBuilder ss = getStateSystemBuilder();
        if (ss == null) {
            return;
        }
        String traceIdHigh = event.getContent().getFieldValue(String.class,
                "trace_id_high");
        String traceIdLow = event.getContent().getFieldValue(String.class,
                       "trace_id_low");
        String spanId = event.getContent().getFieldValue(String.class,
                   "span_id");
        if (spanId == null || traceIdHigh == null || traceIdLow == null) {
        return;
        }
        Long spanIdLong = Long.valueOf(spanId);
        BigInteger traceId = new BigInteger(traceIdHigh);
        traceId = traceId.shiftLeft(64);
        traceId = traceId.add(new BigInteger(traceIdLow));
        String traceIdStr = traceId.toString(16);
        String spanUID = traceIdStr + ':' + Long.toHexString(spanIdLong);
        fFinishSpanUSTEvents.put(spanUID, event);
    }

    @Override
    public void done() {
        ITmfStateSystemBuilder ss = getStateSystemBuilder();
        if (ss == null) {
            return;
        }
        fSpanEventList.forEach(e -> handleSpan(e, ss));
        for (Entry<Integer, Long> e : fQuarkToEndTimestamp.entrySet()) {
            ss.modifyAttribute(e.getValue(), (Object) null, e.getKey());
        }
        for (Entry<Integer, Long> e : fCPQuarkToEndTimestamp.entrySet()) {
            ss.modifyAttribute(e.getValue(), (Object) null, e.getKey());
        }
    }
}
