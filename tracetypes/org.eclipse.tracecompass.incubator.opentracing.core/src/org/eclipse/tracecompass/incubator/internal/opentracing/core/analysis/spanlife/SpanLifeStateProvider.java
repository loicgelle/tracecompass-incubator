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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.eclipse.tracecompass.analysis.os.linux.core.kernel.KernelAnalysisModule;
import org.eclipse.tracecompass.analysis.os.linux.core.kernel.KernelThreadInformationProvider;
import org.eclipse.tracecompass.analysis.os.linux.core.model.HostThread;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.internal.analysis.graph.core.base.TmfGraphVisitor;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.statesystem.AbstractTmfStateProvider;
import org.eclipse.tracecompass.tmf.core.statesystem.ITmfStateProvider;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceUtils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * Span life state provider
 *
 * @author Katherine Nadeau
 * @author Loïc Gelle
 *
 */
public class SpanLifeStateProvider extends AbstractTmfStateProvider {

    private HashMap<Integer, MultiCriticalPathGraphNode> fCriticalPathsByTid;
    private HashMap<Integer, RangeMap<Long, MultiCriticalPathGraphNode>> fStatesListByTid;
    private HashMap<Integer, RangeMap<Long, String>> fActiveSpanByTid;
    private HashMap<Integer, RangeMap<Long, Set<String>>> fAllSpansByTid;

//    private class MultiCriticalPathGraphArrow {
//        public MultiCriticalPathGraphNode destination;
//        public long startTs;
//        public long endTs;
//
//        public MultiCriticalPathGraphArrow(MultiCriticalPathGraphNode dest,
//                long start, long end) {
//            destination = dest;
//            startTs = start;
//            endTs = end;
//        }
//    }

    private class MultiCriticalPathGraphNode {
        private HashMap<Integer, MultiCriticalPathGraphNode> incomingEdges;
        private HashMap<Integer, MultiCriticalPathGraphNode> outgoingEdges;
//        private HashMap<Integer, MultiCriticalPathGraphArrow> outgoingArrows;
        private @Nullable String state;
        private TmfEdge.EdgeType edgeType;
        private @Nullable Integer stateTid;
        private long startTs;
        private long endTs;
        private boolean visited;
        private boolean arrowsVisited;
        private Map.@Nullable Entry<Range<Long>, String> firstSpanRange;
        private Map.@Nullable Entry<Range<Long>, String> lastSpanRange;

        public MultiCriticalPathGraphNode(long startTs, long endTs, Integer tid) {
            incomingEdges = new HashMap<>();
            outgoingEdges = new HashMap<>();
//            outgoingArrows = new HashMap<>();
            state = null;
            edgeType = TmfEdge.EdgeType.UNKNOWN;
            stateTid = tid;
            this.endTs = endTs;
            this.startTs = startTs;
            this.visited = false;
            this.arrowsVisited = false;
            this.firstSpanRange = null;
            this.lastSpanRange = null;
        }

        public MultiCriticalPathGraphNode() {
            incomingEdges = new HashMap<>();
            outgoingEdges = new HashMap<>();
//            outgoingArrows = new HashMap<>();
            state = null;
            edgeType = TmfEdge.EdgeType.UNKNOWN;
            stateTid = null;
            this.endTs = 0L;
            this.startTs = 0L;
            this.visited = false;
            this.arrowsVisited = false;
            this.firstSpanRange = null;
            this.lastSpanRange = null;
        }

        public void addIncomingEdge(Integer tid, MultiCriticalPathGraphNode node) {
            incomingEdges.put(tid, node);
        }

        public void addOutgoingEdge(Integer tid, MultiCriticalPathGraphNode node) {
            outgoingEdges.put(tid, node);
        }

        public void merge(MultiCriticalPathGraphNode otherNode, RangeMap<Long, MultiCriticalPathGraphNode> tidStates) {
            Integer ownStateTid = stateTid;
            long t1 = startTs;
            long t2 = endTs;
            long otherT1 = otherNode.getStartTs();
            long otherT2 = otherNode.getEndTs();
            if (ownStateTid == null || !ownStateTid.equals(otherNode.getStateTid())) {
                System.out.println("ERROR: Unable to merge critical path states"); //$NON-NLS-1$
                return;
            }
            if (t1 > t2 || otherT1 > otherT2) {
                System.out.println("ERROR: Unable to merge critical path states"); //$NON-NLS-1$
                return;
            }
            if (t2 < otherT1 || otherT2 < t1) {
                System.out.println("ERROR: Unable to merge critical path states"); //$NON-NLS-1$
                return;
            }
            if (t1 < otherT1) {
                MultiCriticalPathGraphNode newState =
                        new MultiCriticalPathGraphNode(t1, otherT1, ownStateTid);
                this.replaceLeft(newState);
                Range<Long> range = Range.openClosed(t1, otherT1);
                tidStates.put(range, newState);
                this.merge(otherNode, tidStates);
                return;
            }
            if (t1 > otherT1) {
                MultiCriticalPathGraphNode newState =
                        new MultiCriticalPathGraphNode(otherT1, t1, ownStateTid);
                otherNode.replaceLeft(newState);
                Range<Long> range = Range.openClosed(otherT1, t1);
                tidStates.put(range, newState);
                this.merge(otherNode, tidStates);
                return;
            }
            // At this point t1 == otherT1
            if (t2 > otherT2) {
                MultiCriticalPathGraphNode newState =
                        new MultiCriticalPathGraphNode(t1, otherT2, ownStateTid);
                this.replaceLeft(newState);
                Range<Long> range = Range.openClosed(t1, otherT2);
                tidStates.put(range, newState);
                newState.merge(otherNode, tidStates);
                return;
            } else if (t2 < otherT2) {
                MultiCriticalPathGraphNode newState =
                        new MultiCriticalPathGraphNode(t1, t2, ownStateTid);
                otherNode.replaceLeft(newState);
                Range<Long> range = Range.openClosed(t1, t2);
                tidStates.put(range, newState);
                this.merge(newState, tidStates);
                return;
            }
            // At this point t2 == otherT2
            for (Map.Entry<Integer, MultiCriticalPathGraphNode> e :
                    otherNode.incomingEdges.entrySet()) {
                e.getValue().outgoingEdges.put(e.getKey(), this);
                this.incomingEdges.put(e.getKey(), e.getValue());
            }
            for (Map.Entry<Integer, MultiCriticalPathGraphNode> e :
                    otherNode.outgoingEdges.entrySet()) {
                this.outgoingEdges.put(e.getKey(), e.getValue());
                e.getValue().incomingEdges.put(e.getKey(), this);
            }
            if (otherNode.getState() != null) {
                this.state = otherNode.getState();
                this.edgeType = otherNode.getEdgeType();
            }
        }

        private void replaceLeft(MultiCriticalPathGraphNode otherNode) {
            this.startTs = otherNode.getEndTs();
            for (Map.Entry<Integer, MultiCriticalPathGraphNode> e : incomingEdges.entrySet()) {
                e.getValue().outgoingEdges.put(e.getKey(), otherNode);
            }
            otherNode.incomingEdges = incomingEdges;
            incomingEdges = new HashMap<>();
            for (Map.Entry<Integer, MultiCriticalPathGraphNode> e : otherNode.incomingEdges.entrySet()) {
                incomingEdges.put(e.getKey(), otherNode);
                otherNode.outgoingEdges.put(e.getKey(), this);
            }
            if (this.state != null) {
                otherNode.state = this.state;
                otherNode.edgeType = this.edgeType;
            }
        }

        public @Nullable String getState() {
            return state;
        }

        public TmfEdge.EdgeType getEdgeType() {
            return edgeType;
        }

        public @Nullable Integer getStateTid() {
            return stateTid;
        }

        public long getEndTs() {
            return endTs;
        }

        public long getStartTs() {
            return startTs;
        }

        public void addState(Integer sourceCPTid, TmfEdge.EdgeType cpState) {
            if (this.state == null) {
                this.state = cpState.toString();
                this.edgeType = cpState;
                return;
            }
            if (sourceCPTid == this.stateTid) {
                this.state = cpState.toString();
                this.edgeType = cpState;
            }
        }

        public void markVisited() {
            this.visited = true;
        }

        public boolean wasVisited() {
            return this.visited;
        }

        public void markArrowsVisited() {
            this.arrowsVisited = true;
        }

        public boolean wasArrowsVisited() {
            return this.arrowsVisited;
        }
    }

    private final class CriticalPathVisitor extends TmfGraphVisitor {

        private TmfGraph fGraph;
        private Integer fCPTid;
        private MultiCriticalPathGraphNode fPreviousNode;

        public CriticalPathVisitor(TmfGraph graph, HostThread hostThread) {
            fGraph = graph;
            fCPTid = hostThread.getTid();
            MultiCriticalPathGraphNode startNode = new MultiCriticalPathGraphNode();
            fPreviousNode = startNode;
            fCriticalPathsByTid.put(fCPTid, startNode);
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
            Range<Long> range = Range.openClosed(startTs, startTs + duration);
            if (duration == 0 || !horizontal) {
                return;
            }
            Map<String, String> info = worker.getWorkerInformation();
            String tidAsStr = info.get(
                    Objects.requireNonNull(org.eclipse.tracecompass.analysis.os.linux.core.event.aspect.Messages.AspectName_Tid));
            if (tidAsStr == null) {
                return;
            }
            Integer tid = Integer.valueOf(tidAsStr);
            TmfEdge.EdgeType cpState = edge.getType();

            if (!fStatesListByTid.containsKey(tid)) {
                fStatesListByTid.put(tid, TreeRangeMap.create());
            }
            RangeMap<Long, MultiCriticalPathGraphNode> tidStates = fStatesListByTid.get(tid);
            Map<Range<Long>, MultiCriticalPathGraphNode> intersectingTidStates =
                    tidStates.subRangeMap(range).asMapOfRanges();
            MultiCriticalPathGraphNode multiNode = new MultiCriticalPathGraphNode(startTs, startTs + duration, tid);
            multiNode.addState(fCPTid, cpState);
            multiNode.addIncomingEdge(fCPTid, fPreviousNode);
            fPreviousNode.addOutgoingEdge(fCPTid, multiNode);
            if (intersectingTidStates.isEmpty()) {
                tidStates.put(range, multiNode);
            } else {
                List<MultiCriticalPathGraphNode> intersectingTidStatesCopy = new LinkedList<>();
                intersectingTidStates.forEach((r, n) -> intersectingTidStatesCopy.add(n));
                tidStates.remove(range);
                for (MultiCriticalPathGraphNode otherState : intersectingTidStatesCopy) {
                    multiNode.merge(otherState, tidStates);
                }
                Range<Long> lastRange = Range.openClosed(multiNode.getStartTs(),
                                                         multiNode.getEndTs());
                tidStates.put(lastRange, multiNode);
            }
            fPreviousNode = multiNode;
        }
    }

    /**
     * Quark name for open tracing spans
     */
    public static final String OPEN_TRACING_ATTRIBUTE = "openTracingSpans"; //$NON-NLS-1$
    public static final String TID_SPANS_ATTRIBUTE = "TIDToSpans"; //$NON-NLS-1$
    public static final String CP_AGGREGATED_ATTRIBUTE = "CriticalPathsAggregated"; //$NON-NLS-1$
    public static final String CP_ARROWS_ATTRIBUTE = "CriticalPathsArrows"; //$NON-NLS-1$
    public static final String BLOCKED_BY = "Blocked by span "; //$NON-NLS-1$
    public static final String BLOCKED = "[BLOCKED] "; //$NON-NLS-1$

    /**
     * Quark name for ust spans
     */
    public static final String UST_ATTRIBUTE = "ustSpans"; //$NON-NLS-1$

    private final Map<String, Integer> fSpanMap;
    private final Map<String, Integer> fCPSpanMap;

    private final Map<String, Consumer<ITmfEvent>> fHandlers;

    private final List<ITmfEvent> fSpanEventList;

    private final Map<String, ITmfEvent> fStartSpanUSTEvents;

    private final Map<String, ITmfEvent> fFinishSpanUSTEvents;

    private final Map<Integer, Set<Integer>> fTidToQuarks;

    private final Set<HostThread> fAllHostThreads;

    private @Nullable SpanLifeCriticalPathParameterProvider cpParamProvider;

    private HashMap<Integer, ArrayDeque<String>> fTidToActiveSpanStack;
    private HashMap<Integer, Long> fLastTidSpanEventTs;
    private HashMap<String, Integer> fSpanUIDToStartTid;
    private HashMap<Integer, RangeMap<Long, String>> fQuarksToStatesMap;
    private HashMap<Integer, RangeMap<Long, String>> fCPQuarksToStatesMap;
    private HashMap<Integer, RangeMap<Long, String>> fQuarksToArrowsMap;
    private HashSet<String> fHandledStartEvents;

    private @Nullable KernelAnalysisModule fKernelAnalysis;
    private Map<Integer, String> fThreadExecNames;

    /**
     * Constructor
     *
     * @param trace
     *            the trace to follow
     */
    public SpanLifeStateProvider(ITmfTrace trace) {
        super(trace, SpanLifeAnalysis.ID);
        fSpanMap = new HashMap<>();
        fCPSpanMap = new HashMap<>();
        fHandlers = new HashMap<>();
        fSpanEventList = new LinkedList<>();
        fStartSpanUSTEvents = new HashMap<>();
        fFinishSpanUSTEvents = new HashMap<>();
        fTidToQuarks = new HashMap<>();
        fAllHostThreads = new HashSet<>();
        fStatesListByTid = new HashMap<>();
        fCriticalPathsByTid = new HashMap<>();
        fTidToActiveSpanStack = new HashMap<>();
        fActiveSpanByTid = new HashMap<>();
        fAllSpansByTid = new HashMap<>();
        fLastTidSpanEventTs = new HashMap<>();
        fSpanUIDToStartTid = new HashMap<>();
        fQuarksToStatesMap = new HashMap<>();
        fCPQuarksToStatesMap = new HashMap<>();
        fQuarksToArrowsMap = new HashMap<>();
        fThreadExecNames = new HashMap<>();
        fHandledStartEvents = new HashSet<>();
        fKernelAnalysis = TmfTraceUtils.getAnalysisModuleOfClass(trace, KernelAnalysisModule.class, KernelAnalysisModule.ID);
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
            if (tid != null) {
                String hostId = event.getTrace().getHostId();
                fAllHostThreads.add(new HostThread(hostId, tid));
                handleTidSpanEvent(tid, event.getTimestamp().toNanos(), spanUID, false);
            }
            fHandledStartEvents.add(spanUID);
        } else if (methodName.equals("finishWithDuration")) { //$NON-NLS-1$
            if (fHandledStartEvents.contains(spanUID)) {
                fFinishSpanUSTEvents.put(spanUID, event);
                if (tid != null) {
                    handleTidSpanEvent(tid, event.getTimestamp().toNanos(), spanUID, true);
                }
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
            Integer parentQuark = fSpanMap.get(traceId + ':' + refId);
            if (parentQuark == null) {
                return;
            }
            spanQuark = ss.getQuarkRelativeAndAdd(parentQuark,
                    name + '/' + spanUID + '/' + errorTag + '/' + processName + '/' + tid + '/' + hostId);
        }

        if (tid > 0) {
            Set<Integer> quarksSet = fTidToQuarks.get(tid);
            if (quarksSet == null) {
                quarksSet = new HashSet<>();
            }
            quarksSet.add(spanQuark);
            fTidToQuarks.put(tid,  quarksSet);
        }

        if (hostThread == null) {
            ss.modifyAttribute(timestamp, "0~0~0", spanQuark);
            ss.modifyAttribute(timestamp + duration, (Object) null, spanQuark);
        }

        fSpanMap.put(spanUID, spanQuark);

        if (hostThread != null) {
            int cpSpanQuark = ss.getQuarkAbsoluteAndAdd(CP_AGGREGATED_ATTRIBUTE, spanUID);
            fCPSpanMap.put(spanUID, cpSpanQuark);
        }
    }

    /**
     * @param ss
     */
    private void visitMultiCriticalPathNode(ITmfStateSystemBuilder ss,
                                            MultiCriticalPathGraphNode node,
                                            ArrayDeque<MultiCriticalPathGraphNode> toVisit) {
        if (node.wasVisited()) {
            return;
        }
        node.markVisited();

        if (node.getState() == null) {
            for (MultiCriticalPathGraphNode next : node.outgoingEdges.values()) {
                toVisit.addFirst(next);
            }
            return;
        }

        RangeMap<Long, String> activeForState = TreeRangeMap.create();
        for (Integer cpTid : node.incomingEdges.keySet()) {
            RangeMap<Long, String> activeSpans = fActiveSpanByTid.get(cpTid);
            for (Map.Entry<Range<Long>, String> tidSpanRange :
                    activeSpans.subRangeMap(Range.openClosed(node.getStartTs(), node.getEndTs())).asMapOfRanges().entrySet()) {
                long correctStart = (tidSpanRange.getKey().lowerEndpoint() < node.getStartTs())
                                    ? node.getStartTs() : tidSpanRange.getKey().lowerEndpoint();
                long correctEnd = (tidSpanRange.getKey().upperEndpoint() > node.getEndTs())
                                  ? node.getEndTs() : tidSpanRange.getKey().upperEndpoint();
                String candidateSpan = tidSpanRange.getValue();
                ITmfEvent candidateStartTs = fStartSpanUSTEvents.get(candidateSpan);
                long lastEmptyTs = correctStart;
                List<Map.Entry<Range<Long>, String>> activeRanges = new LinkedList<>();
                for (Map.Entry<Range<Long>, String> activeRange :
                        activeForState.subRangeMap(Range.openClosed(correctStart, correctEnd)).asMapOfRanges().entrySet()) {
                    activeRanges.add(activeRange);
                }
                for (Map.Entry<Range<Long>, String> activeRange : activeRanges) {
                    long correctSubStart = (activeRange.getKey().lowerEndpoint() < correctStart)
                                           ? correctStart : activeRange.getKey().lowerEndpoint();
                    long correctSubEnd = (activeRange.getKey().upperEndpoint() > correctEnd)
                                         ? correctEnd : activeRange.getKey().upperEndpoint();
                    String activeSpan = activeRange.getValue();
                    ITmfEvent activeStartTs = fStartSpanUSTEvents.get(activeSpan);
                    if (candidateStartTs != null && activeStartTs != null &&
                            candidateStartTs.getTimestamp().toNanos()
                            > activeStartTs.getTimestamp().toNanos()) {
                        activeForState.put(Range.openClosed(correctSubStart, correctSubEnd), candidateSpan);
                    }
                    if (lastEmptyTs < correctSubStart) {
                        activeForState.put(Range.openClosed(lastEmptyTs, correctSubStart), candidateSpan);
                    }
                    lastEmptyTs = correctSubEnd;
                }
                if (!activeRanges.isEmpty() && lastEmptyTs < correctEnd || activeRanges.isEmpty()) {
                    activeForState.put(Range.openClosed(lastEmptyTs, correctEnd), candidateSpan);
                }
            }
        }

        Integer nodeTid = node.getStateTid();
        String state = node.getState();
        EdgeType edgeType = node.getEdgeType();
        Map.Entry<Range<Long>, String> previousSpanRange = null;
        for (Map.Entry<Range<Long>, String> spanRange : activeForState.asMapOfRanges().entrySet()) {
            // Compute spans to consider
            Set<String> spansInRange = new HashSet<>();
            for (Integer cpTid : node.incomingEdges.keySet()) {
                Map<Range<Long>, Set<String>> spanRangesForTid = fAllSpansByTid.get(cpTid)
                        .subRangeMap(
                                   Range.openClosed(spanRange.getKey().lowerEndpoint(), spanRange.getKey().upperEndpoint()))
                        .asMapOfRanges();
                if (spanRangesForTid.isEmpty()) {
                    continue;
                }
                for (Set<String> s : spanRangesForTid.values()) {
                    spansInRange.addAll(s);
                }
            }
            String activeSpanUID = spanRange.getValue();
            for (String spanUID : spansInRange) {
                Integer quark = fSpanMap.get(spanUID);
                Integer cpQuark = fCPSpanMap.get(spanUID);
                if (quark == null || cpQuark == null) {
                    continue;
                }
                if (!fQuarksToStatesMap.containsKey(quark)) {
                    fQuarksToStatesMap.put(quark, TreeRangeMap.create());
                }
                if (!fCPQuarksToStatesMap.containsKey(cpQuark)) {
                    fCPQuarksToStatesMap.put(cpQuark, TreeRangeMap.create());
                }
                RangeMap<Long, String> statesMap = fQuarksToStatesMap.get(quark);
                RangeMap<Long, String> cpStatesMap = fCPQuarksToStatesMap.get(cpQuark);
                String stateStr;
                String cpStateStr;
                if (spanUID.equals(activeSpanUID)) {
                    stateStr = String.valueOf(nodeTid) + "~" + state + "~" + getCPMatchingState(edgeType);
                    cpStateStr = "[" + String.valueOf(nodeTid) + "/" + getExecutableName(nodeTid) + "] " + state;
                } else {
                    stateStr = String.valueOf(nodeTid) + "~Blocked by span " + activeSpanUID + "~-1";
                    cpStateStr = BLOCKED_BY + activeSpanUID;
                }
                statesMap.put(
                        Range.openClosed(spanRange.getKey().lowerEndpoint(), spanRange.getKey().upperEndpoint()),
                        stateStr);
                cpStatesMap.put(
                        Range.openClosed(spanRange.getKey().lowerEndpoint(), spanRange.getKey().upperEndpoint()),
                        cpStateStr);
            }
            if (previousSpanRange != null) {
                if (!previousSpanRange.getValue().equals(spanRange.getValue())) {
                    for (int incomingTid : node.incomingEdges.keySet()) {
                        int arrowQuark = ss.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE,
                                                                   String.valueOf(incomingTid));
                        if (!fQuarksToArrowsMap.containsKey(arrowQuark)) {
                            fQuarksToArrowsMap.put(arrowQuark, TreeRangeMap.create());
                        }
                        RangeMap<Long, String> arrowsMap = fQuarksToArrowsMap.get(arrowQuark);
                        arrowsMap.put(
                                Range.openClosed(previousSpanRange.getKey().upperEndpoint() - 1,
                                                 spanRange.getKey().lowerEndpoint()),
                                                 previousSpanRange.getValue() + "/" + spanRange.getValue());
                    }
                }
            }
            if (node.firstSpanRange == null) {
                node.firstSpanRange = spanRange;
            }
            previousSpanRange = spanRange;
        }
        node.lastSpanRange = previousSpanRange;

        for (MultiCriticalPathGraphNode next : node.outgoingEdges.values()) {
            toVisit.addFirst(next);
        }
    }

    private void visitArrowsMultiCriticalPathNode(ITmfStateSystemBuilder ss,
                                                  MultiCriticalPathGraphNode node,
                                                  ArrayDeque<MultiCriticalPathGraphNode> toVisit) {
        if (node.wasArrowsVisited()) {
            return;
        }
        node.markArrowsVisited();

        Map.Entry<Range<Long>, String> curRange = node.firstSpanRange;
        if (curRange != null) {
            for (Map.Entry<Integer, MultiCriticalPathGraphNode> prev : node.incomingEdges.entrySet()) {
                Map.Entry<Range<Long>, String> prevRange = prev.getValue().lastSpanRange;
                if (prevRange == null) {
                    continue;
                }
                if (prevRange.getValue().equals(curRange.getValue())) {
                    continue;
                }
                int arrowQuark = ss.getQuarkAbsoluteAndAdd(CP_ARROWS_ATTRIBUTE,
                        String.valueOf(prev.getKey()));
                if (!fQuarksToArrowsMap.containsKey(arrowQuark)) {
                    fQuarksToArrowsMap.put(arrowQuark, TreeRangeMap.create());
                }
                RangeMap<Long, String> arrowsMap = fQuarksToArrowsMap.get(arrowQuark);
                arrowsMap.put(
                        Range.openClosed(prevRange.getKey().upperEndpoint() - 1,
                                         curRange.getKey().lowerEndpoint()),
                                         prevRange.getValue() + "/" + curRange.getValue());
            }
        }

        for (MultiCriticalPathGraphNode next : node.outgoingEdges.values()) {
            toVisit.addFirst(next);
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
            String hostId = event.getTrace().getHostId();
            fAllHostThreads.add(new HostThread(hostId, tid));
            handleTidSpanEvent(tid, event.getTimestamp().toNanos(), spanUID, false);
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
            handleTidSpanEvent(tid, event.getTimestamp().toNanos(), spanUID, true);
        }
    }

    private void handleTidSpanEvent(Integer tid, long ts, String spanUID, boolean isEndEvent) {
        Integer spanTid = tid;
        if (isEndEvent) {
            spanTid = fSpanUIDToStartTid.get(spanUID);
        } else {
            fSpanUIDToStartTid.put(spanUID, tid);
        }
        if (!fTidToActiveSpanStack.containsKey(spanTid)) {
            fTidToActiveSpanStack.put(spanTid, new ArrayDeque<>());
        }
        if (!fActiveSpanByTid.containsKey(spanTid)) {
            fActiveSpanByTid.put(spanTid, TreeRangeMap.create());
        }
        if (!fAllSpansByTid.containsKey(spanTid)) {
            fAllSpansByTid.put(spanTid, TreeRangeMap.create());
        }
        ArrayDeque<String> spanStack = fTidToActiveSpanStack.get(spanTid);
        RangeMap<Long, String> activeSpans = fActiveSpanByTid.get(spanTid);
        RangeMap<Long, Set<String>> allSpans = fAllSpansByTid.get(spanTid);
        if (!spanStack.isEmpty()) {
            activeSpans.put(
                    Range.openClosed(fLastTidSpanEventTs.get(spanTid), ts),
                    spanStack.peekLast());
            allSpans.put(
                    Range.openClosed(fLastTidSpanEventTs.get(spanTid), ts),
                    new HashSet<>(spanStack));
        }
        fLastTidSpanEventTs.put(spanTid, ts);
        if (isEndEvent) {
            spanStack.remove(spanUID);
        } else {
            spanStack.addLast(spanUID);
        }
    }

    @Override
    public void done() {
        ITmfStateSystemBuilder ss = getStateSystemBuilder();
        if (ss == null) {
            return;
        }
        SpanLifeCriticalPathParameterProvider paramProvider = cpParamProvider;
        if (paramProvider != null) {
            for (HostThread hostThread : fAllHostThreads) {
                TmfGraph criticalPath = paramProvider.getCriticalPath(hostThread);
                CriticalPathVisitor visitor = new CriticalPathVisitor(criticalPath, hostThread);
                if (criticalPath != null && criticalPath.getHead() != null) {
                    criticalPath.scanLineTraverse(criticalPath.getHead(), visitor);
                }
            }
        }

        for (HostThread hostThread : fAllHostThreads) {
            Integer cpTid = hostThread.getTid();
            MultiCriticalPathGraphNode node =
                    fCriticalPathsByTid.get(cpTid);
            int q = ss.getQuarkAbsoluteAndAdd("TEST", cpTid.toString());
            while (node.outgoingEdges.containsKey(cpTid)) {
                node = node.outgoingEdges.get(cpTid);
                StringBuilder st = new StringBuilder();
                st.append(node.state + ",");
                for (Integer tid : node.incomingEdges.keySet()) {
                    st.append(tid);
                    st.append(",");
                }
                ss.modifyAttribute(node.getStartTs(), st.toString(), q);
                ss.modifyAttribute(node.getEndTs(), (Object) null, q);
            }
        }

        for (Map.Entry<Integer, RangeMap<Long, String>> e : fActiveSpanByTid.entrySet()) {
            int q = ss.getQuarkAbsoluteAndAdd("TEST2", String.valueOf(e.getKey()));
            for (Map.Entry<Range<Long>, String> e2 : e.getValue().asMapOfRanges().entrySet()) {
                ss.modifyAttribute(e2.getKey().lowerEndpoint(), e2.getValue(), q);
                ss.modifyAttribute(e2.getKey().upperEndpoint(), (Object) null, q);
            }
        }

        fSpanEventList.forEach(e -> handleSpan(e, ss));

        ArrayDeque<MultiCriticalPathGraphNode> nodesToVisit = new ArrayDeque<>();
        for (HostThread ht : fAllHostThreads) {
            MultiCriticalPathGraphNode head = fCriticalPathsByTid.get(ht.getTid());
            nodesToVisit.addFirst(head);
        }
        while (!nodesToVisit.isEmpty()) {
            visitMultiCriticalPathNode(ss, nodesToVisit.pollLast(), nodesToVisit);
        }

        nodesToVisit = new ArrayDeque<>();
        for (HostThread ht : fAllHostThreads) {
            MultiCriticalPathGraphNode head = fCriticalPathsByTid.get(ht.getTid());
            nodesToVisit.addFirst(head);
        }
        while (!nodesToVisit.isEmpty()) {
            visitArrowsMultiCriticalPathNode(ss, nodesToVisit.pollLast(), nodesToVisit);
        }

        // Finally, copy span states to the state system
        for (Map.Entry<Integer, RangeMap<Long, String>> spanEntry : fQuarksToStatesMap.entrySet()) {
            Integer quark = spanEntry.getKey();
            RangeMap<Long, String> statesMap = spanEntry.getValue();
            for (Map.Entry<Range<Long>, String> e : statesMap.asMapOfRanges().entrySet()) {
                ss.modifyAttribute(e.getKey().lowerEndpoint(), e.getValue(), quark);
                ss.modifyAttribute(e.getKey().upperEndpoint(), (Object) null, quark);
            }
        }
        for (Map.Entry<Integer, RangeMap<Long, String>> cpSpanEntry : fCPQuarksToStatesMap.entrySet()) {
            Integer quark = cpSpanEntry.getKey();
            RangeMap<Long, String> cpStatesMap = cpSpanEntry.getValue();
            for (Map.Entry<Range<Long>, String> e : cpStatesMap.asMapOfRanges().entrySet()) {
                ss.modifyAttribute(e.getKey().lowerEndpoint(), e.getValue(), quark);
                ss.modifyAttribute(e.getKey().upperEndpoint(), (Object) null, quark);
            }
        }
        for (Map.Entry<Integer, RangeMap<Long, String>> spanEntry : fQuarksToArrowsMap.entrySet()) {
            Integer quark = spanEntry.getKey();
            RangeMap<Long, String> arrowsMap = spanEntry.getValue();
            for (Map.Entry<Range<Long>, String> e : arrowsMap.asMapOfRanges().entrySet()) {
                ss.modifyAttribute(e.getKey().lowerEndpoint(), e.getValue(), quark);
                ss.modifyAttribute(e.getKey().upperEndpoint(), (Object) null, quark);
            }
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

    private String getExecutableName(Integer tid) {
        String existingValue = fThreadExecNames.get(tid);
        if (existingValue != null) {
            return existingValue;
        }
        String newValue;
        KernelAnalysisModule kernelAnalysisModule = fKernelAnalysis;
        if (kernelAnalysisModule == null) {
            newValue = "unknown";
        } else {
            String execName = KernelThreadInformationProvider.getExecutableName(kernelAnalysisModule, tid);
            if (execName == null) {
                newValue = "unknown";
            } else {
                newValue = execName;
            }
        }
        fThreadExecNames.put(tid, newValue);
        return newValue;
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
