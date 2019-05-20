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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.analysis.graph.core.base.IGraphWorker;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfEdge;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfGraph;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfVertex;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfEdge.EdgeType;
import org.eclipse.tracecompass.analysis.os.linux.core.event.aspect.LinuxTidAspect;
import org.eclipse.tracecompass.analysis.os.linux.core.model.HostThread;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.internal.analysis.graph.core.base.TmfGraphVisitor;
import org.eclipse.tracecompass.internal.analysis.graph.ui.criticalpath.view.CriticalPathPresentationProvider.State;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
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

    private final class CriticalPathVisitor extends TmfGraphVisitor {

        private TmfGraph fGraph;
        private HostThread fHostThread;
        private ITmfStateSystemBuilder fSs;
        private int fSpanQuark;
        private int fCPQuark;
        private long fStartTime;
        private long fEndTime;
        private boolean fStatesAdded;
        private String fSpanUID;
        private boolean fNotOnOwnCriticalPath = false;
        private @Nullable String fPreviousSpanUID;
        private @Nullable Long fLastOwnTimestamp;
        private @Nullable Long fLastCPTimestamp;

        public CriticalPathVisitor(ITmfStateSystemBuilder ss, TmfGraph graph, HostThread hostThread, int spanQuark, int cpQuark, String spanUID, long startTime, long endTime) {
            fGraph = graph;
            fHostThread = hostThread;
            fSs = ss;
            fSpanQuark = spanQuark;
            fCPQuark = cpQuark;
            fStartTime = startTime;
            fEndTime = endTime;
            fSpanUID = spanUID;
            fStatesAdded = false;
            fPreviousSpanUID = null;
            fLastOwnTimestamp = null;
            fLastCPTimestamp = null;
        }

        @Override
        public void visit(TmfEdge edge, boolean horizontal) {
            TmfVertex node = edge.getVertexFrom();
            IGraphWorker worker = fGraph.getParentOf(node);
            if (worker == null) {
                return;
            }
            long startTs = node.getTs();
            long duration = edge.getDuration();
            long endTs = startTs + duration;
            if (duration == 0) {
                return;
            }
            Map<String, String> info = worker.getWorkerInformation();
            String tid = info.get(
                    Objects.requireNonNull(org.eclipse.tracecompass.analysis.os.linux.core.event.aspect.Messages.AspectName_Tid));
            if (tid == null) {
                return;
            }
            if (!horizontal) {
                return;
            }

            String cpState = edge.getType().toString();
            StringBuilder infoBuilder = new StringBuilder();
            infoBuilder.append(tid);
            infoBuilder.append("~");
            infoBuilder.append(cpState);
            infoBuilder.append("~");
            if (tid.equals(String.valueOf(fHostThread.getTid()))) {
                infoBuilder.append(String.valueOf(getCPMatchingState(edge.getType())));
            } else {
                infoBuilder.append(State.values().length
                        + getCPMatchingState(edge.getType()));
            }

            if (startTs > fEndTime || startTs + duration < fStartTime) {
                return;
            }
            long correctStartTs = (startTs < fStartTime) ? fStartTime : startTs;
            long correctEndTs = (endTs > fEndTime) ? fEndTime : endTs;

//            if (tid.equals(String.valueOf(fHostThread.getTid()))) {
//                fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
//                fSs.modifyAttribute(correctStartTs, cpState, fCPQuark);
//                if (fNotOnOwnCriticalPath && fPreviousSpanUID != null && fLastCPTimestamp != null) {
//                    long lastCPTimestamp = fLastCPTimestamp;
//                    if (lastCPTimestamp >= correctStartTs) {
//                        lastCPTimestamp = correctStartTs - 1;
//                    }
//                    int arrowQuark = fSs.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE_IN, fSpanUID);
//                    fSs.modifyAttribute(lastCPTimestamp, fPreviousSpanUID, arrowQuark);
//                    fSs.modifyAttribute(correctStartTs, (Object) null, arrowQuark);
//                }
//                fStatesAdded = true;
//                fNotOnOwnCriticalPath = false;
//                fLastOwnTimestamp = correctEndTs;
//                fPreviousSpanUID = fSpanUID;
//            } else {
//                int tidQuark = fSs.optQuarkAbsolute(TID_SPANS_ATTRIBUTE, tid);
//                if (tidQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
//                    try {
//                        Iterable<ITmfStateInterval> intervals = fSs.query2D(Collections.singleton(tidQuark), correctStartTs, correctEndTs);
//                        boolean subStatesAdded = false;
//                        for (ITmfStateInterval interval : intervals) {
//                            long correctSubStartTs = (interval.getStartTime() < correctStartTs) ? correctStartTs : interval.getStartTime();
//                            long correctSubEndTs = (interval.getEndTime() > correctEndTs) ? correctEndTs : interval.getEndTime();
//                            String nextSpanUID = interval.getValueString();
//                            if (nextSpanUID != null) {
//                                fSs.modifyAttribute(correctSubStartTs, String.valueOf(fHostThread.getTid()) + "~Blocked by another span~-1", fSpanQuark);
//                                fSs.modifyAttribute(correctSubStartTs, BLOCKED_BY + nextSpanUID, fCPQuark);
//                                if (!fNotOnOwnCriticalPath && fLastOwnTimestamp != null) {
//                                    long lastOwnTimestamp = fLastOwnTimestamp;
//                                    if (lastOwnTimestamp >= correctSubStartTs) {
//                                        lastOwnTimestamp = correctSubStartTs - 1;
//                                    }
//                                    int arrowQuark = fSs.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE_OUT, fSpanUID);
//                                    fSs.modifyAttribute(lastOwnTimestamp, nextSpanUID, arrowQuark);
//                                    fSs.modifyAttribute(correctSubStartTs, (Object) null, arrowQuark);
//                                }
//                                fNotOnOwnCriticalPath = true;
//                                fLastCPTimestamp = correctSubEndTs;
//                                fPreviousSpanUID = nextSpanUID;
//                            } else {
//                                if (!fNotOnOwnCriticalPath) {
//                                    fSs.modifyAttribute(correctSubStartTs, infoBuilder.toString(), fSpanQuark);
//                                    fSs.modifyAttribute(correctSubStartTs, BLOCKED + cpState, fCPQuark);
//                                    fLastOwnTimestamp = correctSubEndTs;
//                                }
//                            }
//                            subStatesAdded = true;
//                            fStatesAdded = true;
//                        }
//                        if (!subStatesAdded) {
//                            if (!fNotOnOwnCriticalPath) {
//                                fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
//                                fSs.modifyAttribute(correctStartTs, BLOCKED + cpState, fCPQuark);
//                                fLastOwnTimestamp = correctEndTs;
//                                fStatesAdded = true;
//                            }
//                        }
//                    } catch (StateSystemDisposedException e) {
//                        e.printStackTrace();
//                    }
//                } else {
//                    if (!fNotOnOwnCriticalPath) {
//                        fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
//                        fSs.modifyAttribute(correctStartTs, BLOCKED + cpState, fCPQuark);
//                        fNotOnOwnCriticalPath = false;
//                        fLastOwnTimestamp = correctEndTs;
//                        fPreviousSpanUID = fSpanUID;
//                        fStatesAdded = true;
//                    }
//                }
//            }

            if (tid.equals(String.valueOf(fHostThread.getTid()))) {
                int tidQuark = fSs.optQuarkAbsolute(TID_SPANS_ATTRIBUTE, tid);
                if (tidQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
                    try {
                        Iterable<ITmfStateInterval> intervals = fSs.query2D(Collections.singleton(tidQuark), correctStartTs, correctEndTs);
                        boolean subStatesAdded = false;
                        for (ITmfStateInterval interval : intervals) {
                            long correctSubStartTs = (interval.getStartTime() < correctStartTs) ? correctStartTs : interval.getStartTime();
                            long correctSubEndTs = (interval.getEndTime() > correctEndTs) ? correctEndTs : interval.getEndTime();
                            String nextSpanUID = interval.getValueString();
                            if (nextSpanUID != null) {
                                if (fSpanUID.equals(nextSpanUID)) {
                                    fSs.modifyAttribute(correctSubStartTs, infoBuilder.toString(), fSpanQuark);
                                    fSs.modifyAttribute(correctSubStartTs, cpState, fCPQuark);
                                    if (fNotOnOwnCriticalPath && fPreviousSpanUID != null && fLastCPTimestamp != null) {
                                        long lastCPTimestamp = fLastCPTimestamp;
                                        if (lastCPTimestamp >= correctSubStartTs) {
                                            lastCPTimestamp = correctSubStartTs - 1;
                                        }
                                        int arrowQuark = fSs.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE_IN, fSpanUID);
                                        fSs.modifyAttribute(lastCPTimestamp, fPreviousSpanUID, arrowQuark);
                                        fSs.modifyAttribute(correctSubStartTs, (Object) null, arrowQuark);
                                    }
                                    fNotOnOwnCriticalPath = false;
                                    fLastOwnTimestamp = correctSubEndTs;
                                } else {
                                    fSs.modifyAttribute(correctSubStartTs, String.valueOf(fHostThread.getTid()) + "~Blocked by another span~-1", fSpanQuark);
                                    fSs.modifyAttribute(correctSubStartTs, BLOCKED_BY + nextSpanUID, fCPQuark);
                                    if (!fNotOnOwnCriticalPath && fLastOwnTimestamp != null) {
                                        long lastOwnTimestamp = fLastOwnTimestamp;
                                        if (lastOwnTimestamp >= correctSubStartTs) {
                                            lastOwnTimestamp = correctSubStartTs - 1;
                                        }
                                        int arrowQuark = fSs.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE_OUT, fSpanUID);
                                        fSs.modifyAttribute(lastOwnTimestamp, nextSpanUID, arrowQuark);
                                        fSs.modifyAttribute(correctSubStartTs, (Object) null, arrowQuark);
                                    }
                                    fNotOnOwnCriticalPath = true;
                                    fLastCPTimestamp = correctSubEndTs;
                                }
                                fPreviousSpanUID = nextSpanUID;
                                subStatesAdded = true;
                                fStatesAdded = true;
                            } else {
                                if (!fNotOnOwnCriticalPath) {
                                    fSs.modifyAttribute(correctSubStartTs, infoBuilder.toString(), fSpanQuark);
                                    fSs.modifyAttribute(correctSubStartTs, BLOCKED + cpState, fCPQuark);
                                    fLastOwnTimestamp = correctSubEndTs;
                                    subStatesAdded = true;
                                    fStatesAdded = true;
                                }
                            }
                        }
                        if (!subStatesAdded) {
                            if (!fNotOnOwnCriticalPath) {
                                fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
                                fSs.modifyAttribute(correctStartTs, BLOCKED + cpState, fCPQuark);
                                fLastOwnTimestamp = correctEndTs;
                                fStatesAdded = true;
                            }
                        }
                    } catch (StateSystemDisposedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                int tidQuark = fSs.optQuarkAbsolute(TID_SPANS_ATTRIBUTE, tid);
                if (tidQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
                    try {
                        Iterable<ITmfStateInterval> intervals = fSs.query2D(Collections.singleton(tidQuark), correctStartTs, correctEndTs);
                        boolean subStatesAdded = false;
                        for (ITmfStateInterval interval : intervals) {
                            long correctSubStartTs = (interval.getStartTime() < correctStartTs) ? correctStartTs : interval.getStartTime();
                            long correctSubEndTs = (interval.getEndTime() > correctEndTs) ? correctEndTs : interval.getEndTime();
                            String nextSpanUID = interval.getValueString();
                            if (nextSpanUID != null) {
                                fSs.modifyAttribute(correctSubStartTs, String.valueOf(fHostThread.getTid()) + "~Blocked by another span~-1", fSpanQuark);
                                fSs.modifyAttribute(correctSubStartTs, BLOCKED_BY + nextSpanUID, fCPQuark);
                                if (!fNotOnOwnCriticalPath && fLastOwnTimestamp != null) {
                                    long lastOwnTimestamp = fLastOwnTimestamp;
                                    if (lastOwnTimestamp >= correctSubStartTs) {
                                        lastOwnTimestamp = correctSubStartTs - 1;
                                    }
                                    int arrowQuark = fSs.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE_OUT, fSpanUID);
                                    fSs.modifyAttribute(lastOwnTimestamp, nextSpanUID, arrowQuark);
                                    fSs.modifyAttribute(correctSubStartTs, (Object) null, arrowQuark);
                                }
                                fNotOnOwnCriticalPath = true;
                                fLastCPTimestamp = correctSubEndTs;
                                fPreviousSpanUID = nextSpanUID;
                            } else {
                                if (!fNotOnOwnCriticalPath) {
                                    fSs.modifyAttribute(correctSubStartTs, infoBuilder.toString(), fSpanQuark);
                                    fSs.modifyAttribute(correctSubStartTs, BLOCKED + cpState, fCPQuark);
                                    fLastOwnTimestamp = correctSubEndTs;
                                }
                            }
                            subStatesAdded = true;
                            fStatesAdded = true;
                        }
                        if (!subStatesAdded) {
                            if (!fNotOnOwnCriticalPath) {
                                fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
                                fSs.modifyAttribute(correctStartTs, BLOCKED + cpState, fCPQuark);
                                fLastOwnTimestamp = correctEndTs;
                                fStatesAdded = true;
                            }
                        }
                    } catch (StateSystemDisposedException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (!fNotOnOwnCriticalPath) {
                        fSs.modifyAttribute(correctStartTs, infoBuilder.toString(), fSpanQuark);
                        fSs.modifyAttribute(correctStartTs, BLOCKED + cpState, fCPQuark);
                        fNotOnOwnCriticalPath = false;
                        fLastOwnTimestamp = correctEndTs;
                        fPreviousSpanUID = fSpanUID;
                        fStatesAdded = true;
                    }
                }
            }
        }

        public boolean wereStatesAdded() {
            return fStatesAdded;
        }
    }

    /**
     * Quark name for open tracing spans
     */
    public static final String OPEN_TRACING_ATTRIBUTE = "openTracingSpans"; //$NON-NLS-1$
    public static final String TID_SPANS_ATTRIBUTE = "TIDToSpans"; //$NON-NLS-1$
    public static final String CP_AGGREGATED_ATTRIBUTE = "CriticalPathsAggregated"; //$NON-NLS-1$
    public static final String CP_ARROWS_ATTRIBUTE_IN = "CriticalPathsArrowsInBound"; //$NON-NLS-1$
    public static final String CP_ARROWS_ATTRIBUTE_OUT = "CriticalPathsArrowsOutbound"; //$NON-NLS-1$
    public static final String BLOCKED_BY = "Blocked by span "; //$NON-NLS-1$
    public static final String BLOCKED = "[BLOCKED] "; //$NON-NLS-1$

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

    private final Map<HostThread, TmfGraph> fCriticalPaths;

    private @Nullable SpanLifeCriticalPathParameterProvider cpParamProvider;

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
        fTidToQuarks = new HashMap<>();
        fCriticalPaths = new HashMap<>();
        fHandlers.put("OpenTracingSpan", this::handleAddSpanToQueue); //$NON-NLS-1$
        fHandlers.put("jaeger_ust:start_span", this::handleStartUSTEvent); //$NON-NLS-1$
        fHandlers.put("jaeger_ust:end_span", this::handleEndUSTEvent); //$NON-NLS-1$
        fHandlers.put("lttng_jul:event", this::handleUSTJulEvent); //$NON-NLS-1$
        cpParamProvider = SpanLifeCriticalPathParameterProvider.getInstance();
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
        Integer tid = (Integer) TmfTraceUtils.resolveEventAspectOfClassForEvent(
                event.getTrace(), LinuxTidAspect.class, event);
        if (methodName.equals("start")) { //$NON-NLS-1$
            fStartSpanUSTEvents.put(spanUID, event);
            if (tid != null && ss != null) {
                int quark = ss.getQuarkAbsoluteAndAdd(TID_SPANS_ATTRIBUTE, String.valueOf(tid));
                ss.modifyAttribute(event.getTimestamp().toNanos(), spanUID, quark);
            }
        } else if (methodName.equals("finishWithDuration")) { //$NON-NLS-1$
            fFinishSpanUSTEvents.put(spanUID, event);
            if (tid != null && ss != null) {
                int quark = ss.getQuarkAbsoluteAndAdd(TID_SPANS_ATTRIBUTE, String.valueOf(tid));
                SpanLifeStateProvider.this.addFutureEvent(event.getTimestamp().toNanos(), (Object) null, quark);
            }
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
        String hostId = "0";
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
        String name = String.valueOf(TmfTraceUtils.resolveAspectOfNameForEvent(event.getTrace(), "Name", event)); //$NON-NLS-1$
        String spanId = event.getContent().getFieldValue(String.class, IOpenTracingConstants.SPAN_ID);

        String spanUID = traceId + ':' + spanId;
        ITmfEvent startEvent = fStartSpanUSTEvents.get(spanUID);
        ITmfEvent finishEvent = fFinishSpanUSTEvents.get(spanUID);
        HostThread hostThread = null;
        if (startEvent != null) {
            timestamp = startEvent.getTimestamp().toNanos();
            tid = (Integer) TmfTraceUtils.resolveEventAspectOfClassForEvent(
                    startEvent.getTrace(), LinuxTidAspect.class, startEvent);
            if (tid == null) {
                tid = 0;
            } else {
                hostId = startEvent.getTrace().getHostId();
                hostThread = new HostThread(hostId, tid);
            }
        }
        if (finishEvent != null) {
            duration = finishEvent.getTimestamp().toNanos() - timestamp;
        }

        String refId = event.getContent().getFieldValue(String.class, IOpenTracingConstants.REFERENCES + "/CHILD_OF"); //$NON-NLS-1$
        if (refId == null) {
            spanQuark = ss.getQuarkRelativeAndAdd(openTracingSpansQuark,
                    name + '/' + spanUID + '/' + errorTag + '/' + processName + '/' + tid + '/' + hostId);
        } else {
            Integer parentQuark = fSpanMap.get(refId);
            if (parentQuark == null) {
                return;
            }
            spanQuark = ss.getQuarkRelativeAndAdd(parentQuark,
                    name + '/' + spanUID + '/' + errorTag + '/' + processName + '/' + tid + '/' + hostId);
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

        if (hostThread != null) {
            prepareCriticalPath(hostThread);
            if (!addCriticalPathStates(ss, spanQuark, hostThread, spanUID, timestamp, timestamp + duration)) {
                ss.modifyAttribute(timestamp, String.valueOf(tid) + "~0~0", spanQuark);
            }
        } else {
            ss.modifyAttribute(timestamp, "0~0~0", spanQuark);
        }

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

    private boolean addCriticalPathStates(ITmfStateSystemBuilder ss, int spanQuark, HostThread hostThread, String spanUID, long startTime, long endTime) {
        TmfGraph criticalPath = fCriticalPaths.get(hostThread);
        if (criticalPath == null) {
            return false;
        }

        TmfVertex start = criticalPath.getHead();
        if (start == null) {
            return false;
        }
        int cpQuark = ss.getQuarkAbsoluteAndAdd(CP_AGGREGATED_ATTRIBUTE, spanUID);
        CriticalPathVisitor visitor = new CriticalPathVisitor(ss, criticalPath, hostThread, spanQuark,
                                                              cpQuark, spanUID, startTime, endTime);
        criticalPath.scanLineTraverse(start, visitor);
        if (!visitor.wereStatesAdded()) {
            ss.modifyAttribute(startTime, String.valueOf(hostThread.getTid()) + "~0~0", spanQuark);
            ss.modifyAttribute(endTime, "UNKNOWN", cpQuark);
        }
        ss.modifyAttribute(endTime, (Object) null, spanQuark);
        ss.modifyAttribute(endTime, (Object) null, cpQuark);

        return false;
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
        Integer tid = (Integer) TmfTraceUtils.resolveEventAspectOfClassForEvent(
                event.getTrace(), LinuxTidAspect.class, event);
        if (tid != null) {
            int quark = ss.getQuarkAbsoluteAndAdd(TID_SPANS_ATTRIBUTE, String.valueOf(tid));
            ss.modifyAttribute(event.getTimestamp().toNanos(), spanUID, quark);
        }
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
        Integer tid = (Integer) TmfTraceUtils.resolveEventAspectOfClassForEvent(
                event.getTrace(), LinuxTidAspect.class, event);
        if (tid != null) {
            int quark = ss.getQuarkAbsoluteAndAdd(TID_SPANS_ATTRIBUTE, String.valueOf(tid));
            SpanLifeStateProvider.this.addFutureEvent(event.getTimestamp().toNanos(), (Object) null, quark);
        }
    }

    private void prepareCriticalPath(HostThread hostThread) {
        if (!fCriticalPaths.containsKey(hostThread) && cpParamProvider != null) {
            TmfGraph criticalPath = cpParamProvider.getCriticalPath(hostThread);
            fCriticalPaths.put(hostThread, criticalPath);
        }
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

        if (cpParamProvider != null) {
            cpParamProvider.resetParameter();
        }
    }

    private static int getCPMatchingState(EdgeType type) {
        switch (type) {
        case RUNNING:
            return 0;
        case INTERRUPTED:
            return 1;
        case PREEMPTED:
            return 2;
        case TIMER:
            return 3;
        case BLOCK_DEVICE:
            return 4;
        case USER_INPUT:
            return 5;
        case NETWORK:
            return 6;
        case IPI:
            return 7;
        case EPS:
        case UNKNOWN:
        case DEFAULT:
        case BLOCKED:
            break;
        default:
            break;
        }
        return 8;
    }

    public static String getSpanName(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        // The span name could contain a '/' character
        return String.join("/",
                Arrays.copyOfRange(attributeInfo, 0, attributeInfo.length - 5));
    }

    public static String getSpanId(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 5];
    }

    public static String getShortSpanId(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        String spanID = attributeInfo[attributeInfo.length - 5];
        return spanID.split(":")[1];
    }

    public static Boolean getErrorTag(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 4].equals("true"); //$NON-NLS-1$
    }

    public static String getProcessName(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 3];
    }

    public static @Nullable Integer getTid(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        Integer tid = (attributeInfo.length > 5)
                ? Integer.valueOf(attributeInfo[attributeInfo.length - 2])
                : -1;
        return (tid < 1) ? null : tid;
    }

    public static String getHostId(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 1];
    }
}
