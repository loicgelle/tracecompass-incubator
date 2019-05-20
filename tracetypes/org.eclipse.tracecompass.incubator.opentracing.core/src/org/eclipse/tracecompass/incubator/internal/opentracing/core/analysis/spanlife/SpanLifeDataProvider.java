/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeEntryModel.LogEvent;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.internal.tmf.core.model.filters.TimeGraphStateQueryFilter;
import org.eclipse.tracecompass.internal.tmf.core.model.timegraph.AbstractTimeGraphDataProvider;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.exceptions.AttributeNotFoundException;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.model.CommonStatusMessage;
import org.eclipse.tracecompass.tmf.core.model.filters.SelectionTimeQueryFilter;
import org.eclipse.tracecompass.tmf.core.model.filters.TimeQueryFilter;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphArrow;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphRowModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphState;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphArrow;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphEntryModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphRowModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphState;
import org.eclipse.tracecompass.tmf.core.response.ITmfResponse;
import org.eclipse.tracecompass.tmf.core.response.TmfModelResponse;
import org.eclipse.tracecompass.tmf.core.response.ITmfResponse.Status;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;

import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * Data provider that will show the object lifespans.
 *
 * @author Katherine Nadeau
 * @author Loïc Gelle
 *
 */
@SuppressWarnings("restriction")
public class SpanLifeDataProvider extends AbstractTimeGraphDataProvider<@NonNull SpanLifeAnalysis, @NonNull TimeGraphEntryModel> {

    private static final int MARKER_SIZE = 500;

    private static final String ERROR = "error"; //$NON-NLS-1$
    private static final String EVENT = "event"; //$NON-NLS-1$
    private static final String MESSAGE = "message"; //$NON-NLS-1$
    private static final String STACK = "stack"; //$NON-NLS-1$
    private static final String OTHER = "other"; //$NON-NLS-1$

    /**
     * Suffix for dataprovider ID
     */
    public static final String SUFFIX = ".dataprovider"; //$NON-NLS-1$

    private Map<String, Long> fSpanUIDToEntryID;

    /**
     * Constructor
     *
     * @param trace
     *            the trace this provider represents
     * @param analysisModule
     *            the analysis encapsulated by this provider
     */
    public SpanLifeDataProvider(ITmfTrace trace, SpanLifeAnalysis analysisModule) {
        super(trace, analysisModule);
        fSpanUIDToEntryID = new HashMap<>();
    }

    @Override
    public @NonNull TmfModelResponse<@NonNull List<@NonNull ITimeGraphArrow>> fetchArrows(@NonNull TimeQueryFilter filter, @Nullable IProgressMonitor monitor) {
        List<ITimeGraphArrow> arrows = new ArrayList<>();
        ITmfStateSystem ss = getAnalysisModule().getStateSystem();
        if (ss == null) {
            return new TmfModelResponse<>(null, Status.COMPLETED, CommonStatusMessage.COMPLETED);
        }

        Collection<@NonNull Integer> arrowOutQuarks = ss.getQuarks(SpanLifeStateProvider.CP_ARROWS_ATTRIBUTE_OUT, "*");
        try {
            for (ITmfStateInterval arrowState : ss.query2D(arrowOutQuarks, filter.getStart(), filter.getEnd())) {
                String spanToUID = arrowState.getValueString();
                if (spanToUID == null) {
                    continue;
                }
                String spanFromUID = ss.getAttributeName(arrowState.getAttribute());
                Long entryFrom = fSpanUIDToEntryID.get(spanFromUID);
                Long entryTo = fSpanUIDToEntryID.get(spanToUID);
                if (entryFrom == null || entryTo == null) {
                    continue;
                }
                arrows.add(new TimeGraphArrow(entryFrom, entryTo, arrowState.getStartTime(), arrowState.getEndTime() - arrowState.getStartTime(), -2));
            }
        } catch (IndexOutOfBoundsException | TimeRangeException | StateSystemDisposedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Collection<@NonNull Integer> arrowInQuarks = ss.getQuarks(SpanLifeStateProvider.CP_ARROWS_ATTRIBUTE_IN, "*");
        try {
            for (ITmfStateInterval arrowState : ss.query2D(arrowInQuarks, filter.getStart(), filter.getEnd())) {
                String spanFromUID = arrowState.getValueString();
                if (spanFromUID == null) {
                    continue;
                }
                String spanToUID = ss.getAttributeName(arrowState.getAttribute());
                Long entryFrom = fSpanUIDToEntryID.get(spanFromUID);
                Long entryTo = fSpanUIDToEntryID.get(spanToUID);
                if (entryFrom == null || entryTo == null) {
                    continue;
                }
                arrows.add(new TimeGraphArrow(entryFrom, entryTo, arrowState.getStartTime(), arrowState.getEndTime() - arrowState.getStartTime(), -2));
            }
        } catch (IndexOutOfBoundsException | TimeRangeException | StateSystemDisposedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new TmfModelResponse<>(arrows, Status.COMPLETED, CommonStatusMessage.COMPLETED);
    }

    @Override
    public @NonNull TmfModelResponse<@NonNull Map<@NonNull String, @NonNull String>> fetchTooltip(@NonNull SelectionTimeQueryFilter filter, @Nullable IProgressMonitor monitor) {
        ITmfStateSystem ss = getAnalysisModule().getStateSystem();
        Map<@NonNull Long, @NonNull Integer> entries = getSelectedEntries(filter);
        Collection<@NonNull Integer> quarks = entries.values();
        long startTime = filter.getStart();
        long hoverTime = filter.getTimesRequested()[1];
        long endTime = filter.getEnd();
        if (ss == null || quarks.size() != 1 || !getAnalysisModule().isQueryable(hoverTime)) {
            return new TmfModelResponse<>(null, ITmfResponse.Status.COMPLETED, CommonStatusMessage.COMPLETED);
        }

        int traceLogsQuark = ITmfStateSystem.INVALID_ATTRIBUTE;
        try {
            String traceId = ss.getFullAttributePathArray(quarks.iterator().next())[0];
            traceLogsQuark = ss.getQuarkRelative(ss.getQuarkAbsolute(traceId), IOpenTracingConstants.LOGS);
        } catch (AttributeNotFoundException e) {
            return new TmfModelResponse<>(null, ITmfResponse.Status.CANCELLED, CommonStatusMessage.TASK_CANCELLED);
        }

        int spanLogQuark = getLogQuark(ss, ss.getAttributeName(quarks.iterator().next()), ss.getSubAttributes(traceLogsQuark, false));

        try {
            Map<@NonNull String, @NonNull String> retMap = new HashMap<>();

            int quark = quarks.iterator().next();
            String attrName = ss.getAttributeName(quark);
            Integer tid = SpanLifeStateProvider.getTid(attrName);
            if (tid != null) {
                ITmfStateInterval interval = ss.querySingleState(hoverTime, quark);
                String info = (String) interval.getValue();
                if (info == null) {
                    return new TmfModelResponse<>(null, ITmfResponse.Status.CANCELLED, CommonStatusMessage.TASK_CANCELLED);
                }
                String[] infoSpl = info.split("~");
                Map<String, String> infoMap = new HashMap<>();
                infoMap.put("TID", infoSpl[0]);
                infoMap.put("State", infoSpl[1]);
                return new TmfModelResponse<>(infoMap, Status.COMPLETED, CommonStatusMessage.COMPLETED);
            }

            if (spanLogQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
                Long ssStartTime = startTime == Long.MIN_VALUE ? ss.getStartTime() : startTime;
                Long ssEndTime = endTime == Long.MIN_VALUE ? ss.getCurrentEndTime() : endTime;
                Long deviationAccepted = (ssEndTime - ssStartTime) / MARKER_SIZE;
                for (ITmfStateInterval state : ss.query2D(Collections.singletonList(spanLogQuark), Math.max(hoverTime - deviationAccepted, ssStartTime), Math.min(hoverTime + deviationAccepted, ssEndTime))) {
                    Object object = state.getValue();
                    if (object instanceof String) {
                        String logs = (String) object;
                        String timestamp = TmfTimestamp.fromNanos(state.getStartTime()).toString();
                        if (timestamp != null) {
                            retMap.put("log timestamp", timestamp); //$NON-NLS-1$
                        }
                        String[] fields = logs.split("~"); //$NON-NLS-1$
                        for (String field : fields) {
                            retMap.put(field.substring(0, field.indexOf(':')), field.substring(field.indexOf(':') + 1));
                        }
                        return new TmfModelResponse<>(retMap, ITmfResponse.Status.COMPLETED, CommonStatusMessage.COMPLETED);
                    }
                }
            }
            return new TmfModelResponse<>(retMap, ITmfResponse.Status.COMPLETED, CommonStatusMessage.COMPLETED);
        } catch (StateSystemDisposedException e) {
            return new TmfModelResponse<>(null, ITmfResponse.Status.CANCELLED, CommonStatusMessage.TASK_CANCELLED);
        }
    }

    @Override
    public @NonNull String getId() {
        return getAnalysisModule().getId() + SUFFIX;
    }

    @Override
    protected @Nullable List<@NonNull ITimeGraphRowModel> getRowModel(@NonNull ITmfStateSystem ss, @NonNull SelectionTimeQueryFilter filter, @Nullable IProgressMonitor monitor) throws StateSystemDisposedException {
        TreeMultimap<Integer, ITmfStateInterval> intervals = TreeMultimap.create(Comparator.naturalOrder(),
                Comparator.comparing(ITmfStateInterval::getStartTime));
        Map<@NonNull Long, @NonNull Integer> entries = getSelectedEntries(filter);
        /* Do the actual query */
        for (ITmfStateInterval interval : ss.query2D(entries.values(), filter.getStart(), filter.getEnd())) {
            if (monitor != null && monitor.isCanceled()) {
                return Collections.emptyList();
            }
            intervals.put(interval.getAttribute(), interval);
        }
        Map<@NonNull Integer, @NonNull Predicate<@NonNull Multimap<@NonNull String, @NonNull String>>> predicates = new HashMap<>();
        if (filter instanceof TimeGraphStateQueryFilter) {
            TimeGraphStateQueryFilter timeEventFilter = (TimeGraphStateQueryFilter) filter;
            predicates.putAll(computeRegexPredicate(timeEventFilter));
        }
        List<@NonNull ITimeGraphRowModel> rows = new ArrayList<>();
        Collection<Long> times = new ArrayList<>();
        for (Long t : filter.getTimesRequested()) {
            times.add(t);
        }
        for (Map.Entry<@NonNull Long, @NonNull Integer> entry : entries.entrySet()) {
            if (monitor != null && monitor.isCanceled()) {
                return Collections.emptyList();
            }

            int quark = entry.getValue();

            List<ITimeGraphState> eventList = new ArrayList<>();
            for (ITmfStateInterval interval : intervals.get(quark)) {
                long startTime = interval.getStartTime();
                long endTime = interval.getEndTime();
                long duration = endTime - startTime - 1;
                String intervalState = interval.getValueString();
                if (intervalState == null) {
                    continue;
                }
                String[] splitState = intervalState.split("~");
                TimeGraphState state = new TimeGraphState(startTime, duration, Integer.valueOf(splitState[2]));
                applyFilterAndAddState(eventList, state, entry.getKey(), predicates, monitor);
            }
            rows.add(new TimeGraphRowModel(entry.getKey(), eventList));

        }
        return rows;
    }

    @Override
    protected boolean isCacheable() {
        return true;
    }

    @Override
    protected @NonNull List<@NonNull TimeGraphEntryModel> getTree(@NonNull ITmfStateSystem ss, @NonNull TimeQueryFilter filter, @Nullable IProgressMonitor monitor) throws StateSystemDisposedException {
        Builder<@NonNull TimeGraphEntryModel> builder = new Builder<>();
        long rootId = getId(ITmfStateSystem.ROOT_ATTRIBUTE);
        builder.add(new TimeGraphEntryModel(rootId, -1, String.valueOf(getTrace().getName()), ss.getStartTime(), ss.getCurrentEndTime()));

        for (int traceQuark : ss.getSubAttributes(ITmfStateSystem.ROOT_ATTRIBUTE, false)) {
            addTrace(ss, builder, traceQuark, rootId);
        }

        return builder.build();
    }

    private void addTrace(ITmfStateSystem ss, Builder<@NonNull TimeGraphEntryModel> builder, int quark, long parentId) {
        List<@NonNull Integer> logsQuarks;
        try {
            int logsQuark = ss.getQuarkRelative(quark, IOpenTracingConstants.LOGS);
            logsQuarks = ss.getSubAttributes(logsQuark, false);
        } catch (AttributeNotFoundException e) {
            logsQuarks = new ArrayList<>();
        }

        int openTracingSpansQuark;
        try {
            openTracingSpansQuark = ss.getQuarkRelative(quark, SpanLifeStateProvider.OPEN_TRACING_ATTRIBUTE);
        } catch (AttributeNotFoundException e) {
            return;
        }

        long traceQuarkId = getId(quark);
        builder.add(new TimeGraphEntryModel(traceQuarkId, parentId, ss.getAttributeName(quark), ss.getStartTime(), ss.getCurrentEndTime()));

        int ustSpansQuark;
        try {
            ustSpansQuark = ss.getQuarkRelative(quark, SpanLifeStateProvider.UST_ATTRIBUTE);
        } catch (AttributeNotFoundException e) {
            addChildren(ss, builder, openTracingSpansQuark, traceQuarkId, logsQuarks);
            return;
        }
        addUstChildren(ss, builder, openTracingSpansQuark, ustSpansQuark, traceQuarkId, logsQuarks);
    }

    private void addChildren(ITmfStateSystem ss, Builder<@NonNull TimeGraphEntryModel> builder, int quark, long parentId, List<Integer> logsQuarks) {
        for (Integer child : ss.getSubAttributes(quark, false)) {
            long childId = getId(child);
            String childName = ss.getAttributeName(child);
            if (!childName.equals(IOpenTracingConstants.LOGS)) {
                List<LogEvent> logs = new ArrayList<>();
                int logQuark = getLogQuark(ss, childName, logsQuarks);
                try {
                    for (ITmfStateInterval interval : ss.query2D(Collections.singletonList(logQuark), ss.getStartTime(), ss.getCurrentEndTime())) {
                        if (!interval.getStateValue().isNull()) {
                            logs.add(new LogEvent(interval.getStartTime(), getLogType(String.valueOf(interval.getValue()))));
                        }
                    }
                } catch (IndexOutOfBoundsException | TimeRangeException | StateSystemDisposedException e) {
                }
                Integer tid = SpanLifeStateProvider.getTid(childName);
                if (tid == null) {
                    builder.add(new SpanLifeEntryModel(childId, parentId, SpanLifeStateProvider.getSpanName(childName),
                            ss.getStartTime(), ss.getCurrentEndTime(), logs, SpanLifeStateProvider.getErrorTag(childName),
                            SpanLifeStateProvider.getProcessName(childName), 0,
                            SpanLifeEntryModel.EntryType.SPAN, "",
                            SpanLifeStateProvider.getSpanId(childName),
                            SpanLifeStateProvider.getShortSpanId(childName)));
                } else {
                    builder.add(new SpanLifeEntryModel(childId, parentId, SpanLifeStateProvider.getSpanName(childName),
                            ss.getStartTime(), ss.getCurrentEndTime(), logs, SpanLifeStateProvider.getErrorTag(childName),
                            SpanLifeStateProvider.getProcessName(childName), tid, SpanLifeEntryModel.EntryType.KERNEL,
                            SpanLifeStateProvider.getHostId(childName),
                            SpanLifeStateProvider.getSpanId(childName),
                            SpanLifeStateProvider.getShortSpanId(childName)));
                    fSpanUIDToEntryID.put(SpanLifeStateProvider.getSpanId(childName), childId);
                }
                addChildren(ss, builder, child, childId, logsQuarks);
            }
        }
    }

    private void addUstChildren(ITmfStateSystem ss, Builder<@NonNull TimeGraphEntryModel> builder, int openTracingQuark, int ustQuark, long parentId, List<Integer> logsQuarks) {
        for (Integer child : ss.getSubAttributes(openTracingQuark, false)) {
            String childName = ss.getAttributeName(child);

            List<LogEvent> logs = new ArrayList<>();
            int logQuark = getLogQuark(ss, childName, logsQuarks);
            try {
                for (ITmfStateInterval interval : ss.query2D(Collections.singletonList(logQuark), ss.getStartTime(), ss.getCurrentEndTime())) {
                    if (!interval.getStateValue().isNull()) {
                        logs.add(new LogEvent(interval.getStartTime(), getLogType(String.valueOf(interval.getValue()))));
                    }
                }
            } catch (IndexOutOfBoundsException | TimeRangeException | StateSystemDisposedException e) {
            }

            String spanId = SpanLifeStateProvider.getSpanId(childName);

            int ustSpan;
            try {
                ustSpan = ss.getQuarkRelative(ustQuark, spanId);
            } catch (AttributeNotFoundException e) {
                return;
            }
            Integer tid = SpanLifeStateProvider.getTid(childName);
            long childId = getId(ustSpan);
            if (tid == null) {
                builder.add(new SpanLifeEntryModel(childId, parentId, SpanLifeStateProvider.getSpanName(childName),
                        ss.getStartTime(), ss.getCurrentEndTime(), logs,
                        SpanLifeStateProvider.getErrorTag(childName), SpanLifeStateProvider.getProcessName(childName),
                        0, SpanLifeEntryModel.EntryType.SPAN, "",
                        SpanLifeStateProvider.getSpanId(childName),
                        SpanLifeStateProvider.getShortSpanId(childName)));
            } else {
                builder.add(new SpanLifeEntryModel(childId, parentId, SpanLifeStateProvider.getSpanName(childName),
                        ss.getStartTime(), ss.getCurrentEndTime(), logs,
                        SpanLifeStateProvider.getErrorTag(childName), SpanLifeStateProvider.getProcessName(childName),
                        tid, SpanLifeEntryModel.EntryType.KERNEL, SpanLifeStateProvider.getHostId(childName),
                        SpanLifeStateProvider.getSpanId(childName),
                        SpanLifeStateProvider.getShortSpanId(childName)));
                fSpanUIDToEntryID.put(SpanLifeStateProvider.getSpanId(childName), childId);
            }
            addUstChildren(ss, builder, child, ustQuark, childId, logsQuarks);
        }
    }

    private static int getLogQuark(ITmfStateSystem ss, String spanName, List<Integer> logsQuarks) {
        for (int logsQuark : logsQuarks) {
            if (ss.getAttributeName(logsQuark).equals(SpanLifeStateProvider.getSpanId(spanName))) {
                return logsQuark;
            }
        }
        return ITmfStateSystem.INVALID_ATTRIBUTE;
    }

    private static String getLogType(String logs) {
        String[] logsArray = logs.split("~"); //$NON-NLS-1$
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < logsArray.length; i++) {
            builder.append(logsArray[i].substring(0, logsArray[i].indexOf(':')));
        }
        String types = builder.toString();

        if (types.contains(ERROR)) {
            return ERROR;
        } else if (types.contains(EVENT)) {
            return EVENT;
        } else if (types.contains(MESSAGE)) {
            return MESSAGE;
        } else if (types.contains(STACK)) {
            return STACK;
        } else {
            return OTHER;
        }
    }

}
