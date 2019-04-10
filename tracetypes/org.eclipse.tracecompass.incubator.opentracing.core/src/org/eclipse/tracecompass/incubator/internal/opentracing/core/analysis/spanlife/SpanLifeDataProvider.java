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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeEntryModel.LogEvent;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.IOpenTracingConstants;
import org.eclipse.tracecompass.internal.analysis.os.linux.core.threadstatus.ThreadEntryModel;
import org.eclipse.tracecompass.internal.analysis.os.linux.core.threadstatus.ThreadStatusDataProvider;
import org.eclipse.tracecompass.internal.tmf.core.model.filters.TimeGraphStateQueryFilter;
import org.eclipse.tracecompass.internal.tmf.core.model.timegraph.AbstractTimeGraphDataProvider;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.exceptions.AttributeNotFoundException;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.dataprovider.DataProviderManager;
import org.eclipse.tracecompass.tmf.core.model.CommonStatusMessage;
import org.eclipse.tracecompass.tmf.core.model.filters.SelectionTimeQueryFilter;
import org.eclipse.tracecompass.tmf.core.model.filters.TimeQueryFilter;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphArrow;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphRowModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphState;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphEntryModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphRowModel;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphState;
import org.eclipse.tracecompass.tmf.core.response.ITmfResponse;
import org.eclipse.tracecompass.tmf.core.response.TmfModelResponse;
import org.eclipse.tracecompass.tmf.core.response.ITmfResponse.Status;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;

import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.TreeMultimap;

/**
 * Data provider that will show the object lifespans.
 *
 * @author Katherine Nadeau
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

    private static class ThreadInfoRequest {
        private final long fStart;
        private final long fEnd;

        public ThreadInfoRequest(long start, long end) {
            fStart = start;
            fEnd = end;
        }

        public boolean intersects(ITimeGraphState state) {
            return !(state.getStartTime() > fEnd || (state.getStartTime() + state.getDuration()) < fStart);
        }

        public boolean precedes(ITimeGraphState state) {
            return (state.getStartTime() + state.getDuration() < fEnd);
        }

        public ITimeGraphState sanitize(ITimeGraphState state) {
            if (state.getStartTime() < fStart || state.getStartTime() + state.getDuration() > fEnd) {
                long start = Math.max(state.getStartTime(), fStart);
                long end = Math.min(state.getStartTime() + state.getDuration(), fEnd);
                String label = state.getLabel();
                if (label != null) {
                    return new TimeGraphState(start, end - start, state.getValue(), label);
                }
                return new TimeGraphState(start, end - start, state.getValue());
            }
            return state;
        }
    }

    private static class ThreadData {

        private final ThreadStatusDataProvider fThreadDataProvider;
        private final List<ThreadEntryModel> fThreadTree;
        private final Status fStatus;

        public ThreadData(ThreadStatusDataProvider dataProvider, List<ThreadEntryModel> threadTree, Status status) {
            fThreadDataProvider = dataProvider;
            fThreadTree = threadTree;
            fStatus = status;
        }

        public @Nullable Map<String, String> fetchTooltip(ThreadEntryModel threadEntry, long time, @Nullable IProgressMonitor monitor) {
            if (threadEntry.getStartTime() <= time && threadEntry.getEndTime() >= time) {
                TmfModelResponse<Map<String, String>> tooltip = fThreadDataProvider.fetchTooltip(new SelectionTimeQueryFilter(Collections.singletonList(time), Collections.singleton(threadEntry.getId())), monitor);
                return tooltip.getModel();
            }
            return null;
        }

    }

    private @Nullable ThreadData fThreadData = null;
    private Map<Integer, ThreadEntryModel> fTidToThreadEntry = new HashMap<>();

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
    }

    @Override
    public @NonNull TmfModelResponse<@NonNull List<@NonNull ITimeGraphArrow>> fetchArrows(@NonNull TimeQueryFilter filter, @Nullable IProgressMonitor monitor) {
        return new TmfModelResponse<>(null, ITmfResponse.Status.COMPLETED, CommonStatusMessage.COMPLETED);
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

            /* Adds tooltip info for kernel states */
            Integer tid = getTid(ss.getAttributeName(quarks.iterator().next()));
            ThreadData threadData = fThreadData;
            if (threadData != null && tid != null) {
                ThreadEntryModel threadEntry = fTidToThreadEntry.get(tid);
                if (threadEntry != null) {
                    Map<String, String> tooltipInfo = threadData.fetchTooltip(threadEntry, filter.getStart(), monitor);
                    if (tooltipInfo != null) {
                        retMap.putAll(tooltipInfo);
                    }
                }
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
        Collection<Long> times = getTimes(filter, ss.getStartTime(), ss.getCurrentEndTime());
        /* Do the actual query */
        for (ITmfStateInterval interval : ss.query2D(entries.values(), ss.getStartTime(), ss.getCurrentEndTime())) {
            if (monitor != null && monitor.isCanceled()) {
                return Collections.emptyList();
            }
            intervals.put(interval.getAttribute(), interval);
        }
        Map<@NonNull Integer, @NonNull Predicate<@NonNull Map<@NonNull String, @NonNull String>>> predicates = new HashMap<>();
        if (filter instanceof TimeGraphStateQueryFilter) {
            TimeGraphStateQueryFilter timeEventFilter = (TimeGraphStateQueryFilter) filter;
            predicates.putAll(computeRegexPredicate(timeEventFilter));
        }
        prepareKernelData(ss.getStartTime());
        List<@NonNull ITimeGraphRowModel> rows = new ArrayList<>();
        for (Map.Entry<@NonNull Long, @NonNull Integer> entry : entries.entrySet()) {
            if (monitor != null && monitor.isCanceled()) {
                return Collections.emptyList();
            }

            int quark = entry.getValue();
            String entryName = ss.getAttributeName(quark);
            Integer tid = getTid(entryName);

            List<ITimeGraphState> eventList = new ArrayList<>();
            for (ITmfStateInterval interval : intervals.get(quark)) {
                long startTime = interval.getStartTime();
                long endTime = interval.getEndTime();
                long duration = endTime - startTime + 1;
                Object state = interval.getValue();
                Builder<@NonNull ITimeGraphState> builder = new Builder<>();
                if (state == null) {
                    continue;
                }
                if (tid == null || state.equals("0")) {
                    builder.add(new TimeGraphState(startTime, duration, 0));
                } else {
                    if (!addKernelStates(builder, tid, startTime, endTime, times)) {
                        // If kernel states are empty, replace with a span state
                        builder.add(new TimeGraphState(startTime, duration, 0));
                    }
                }
                for (ITimeGraphState value : builder.build()) {
                    addToStateList(eventList, value, entry.getKey(), predicates, monitor);
                }
            }
            rows.add(new TimeGraphRowModel(entry.getKey(), eventList));

        }
        return rows;
    }

    private boolean addKernelStates(Builder<ITimeGraphState> builder, Integer tid, long startTime, long endTime, Collection<Long> times) {
        boolean statesAdded = false;
        ThreadData threadData = fThreadData;
        if (threadData == null) {
            return statesAdded;
        }
        List<ThreadEntryModel> tree = threadData.fThreadTree;

        // FIXME: A callstack analysis may be for an experiment that span many hosts,
        // the thread data provider will be a composite and the models may be for
        // different host IDs. But for now, suppose the callstack is a composite also
        // and the trace filtered the right host.
        List<Long> threadModelIDs = new ArrayList<>();
        for (ThreadEntryModel entryModel : tree) {
            if (tid == entryModel.getThreadId()) {
                fTidToThreadEntry.put(tid, entryModel);
                threadModelIDs.add(entryModel.getId());
                break;
            }
        }

        List<Long> filteredTimes = new ArrayList<>();
        for (long t : times) {
            if (t >= startTime && t <= endTime) {
                filteredTimes.add(t);
            }
        }
        SelectionTimeQueryFilter tidFilter = new SelectionTimeQueryFilter(filteredTimes, threadModelIDs);
        TmfModelResponse<List<ITimeGraphRowModel>> rowModel = threadData.fThreadDataProvider.fetchRowModel(tidFilter, null);
        List<ITimeGraphRowModel> rowModels = rowModel.getModel();
        if (rowModel.getStatus().equals(Status.CANCELLED) || rowModel.getStatus().equals(Status.FAILED) || rowModels == null) {
            return statesAdded;
        }

        ThreadInfoRequest tInfo = new ThreadInfoRequest(startTime, endTime);
        rowModels.get(0).getStates();
        for (ITimeGraphRowModel m : rowModels) {
            for (ITimeGraphState state : m.getStates()) {
                if (tInfo.intersects(state)) {
                    ITimeGraphState newState = tInfo.sanitize(state);
                    statesAdded = statesAdded || true;
                    builder.add(newState);
                }
                if (!tInfo.precedes(state)) {
                    break;
                }
            }
        }
        return statesAdded;
    }

    private void prepareKernelData(long start) {
        ThreadData data = fThreadData;
        if (data != null && data.fStatus.equals(Status.COMPLETED)) {
            return;
        }
        // FIXME: Wouldn't work correctly if trace is an experiment as it would cover
        // many hosts
        Set<ITmfTrace> tracesForHost = TmfTraceManager.getInstance().getOpenedTraces();
        for (ITmfTrace trace : tracesForHost) {
            ThreadStatusDataProvider dataProvider = DataProviderManager.getInstance().getDataProvider(trace, ThreadStatusDataProvider.ID, ThreadStatusDataProvider.class);
            if (dataProvider != null) {
                // Get the tree for the trace's current range
                TmfModelResponse<List<ThreadEntryModel>> threadTreeResp = dataProvider.fetchTree(new TimeQueryFilter(start, Long.MAX_VALUE, 2), null);
                List<ThreadEntryModel> threadTree = threadTreeResp.getModel();
                if (threadTree != null) {
                    fThreadData = new ThreadData(dataProvider, threadTree, threadTreeResp.getStatus());
                    break;
                }
            }
        }
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
                Integer tid = getTid(childName);
                if (tid == null) {
                    builder.add(new SpanLifeEntryModel(childId, parentId, getSpanName(childName),
                            ss.getStartTime(), ss.getCurrentEndTime(), logs, getErrorTag(childName),
                            getProcessName(childName), 0, SpanLifeEntryModel.EntryType.SPAN, ""));
                } else {
                    builder.add(new SpanLifeEntryModel(childId, parentId, getSpanName(childName),
                            ss.getStartTime(), ss.getCurrentEndTime(), logs, getErrorTag(childName),
                            getProcessName(childName), tid, SpanLifeEntryModel.EntryType.KERNEL, getHostId(childName)));
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

            String spanId = getSpanId(childName);

            int ustSpan;
            try {
                ustSpan = ss.getQuarkRelative(ustQuark, spanId);
            } catch (AttributeNotFoundException e) {
                return;
            }
            Integer tid = getTid(childName);
            long childId = getId(ustSpan);
            if (tid == null) {
                builder.add(new SpanLifeEntryModel(childId, parentId, getSpanName(childName),
                        ss.getStartTime(), ss.getCurrentEndTime(), logs,
                        getErrorTag(childName), getProcessName(childName), 0, SpanLifeEntryModel.EntryType.SPAN, ""));
            } else {
                builder.add(new SpanLifeEntryModel(childId, parentId, getSpanName(childName),
                        ss.getStartTime(), ss.getCurrentEndTime(), logs, getErrorTag(childName),
                        getProcessName(childName), tid, SpanLifeEntryModel.EntryType.KERNEL, getHostId(childName)));
            }
            addUstChildren(ss, builder, child, ustQuark, childId, logsQuarks);
        }
    }

    private static int getLogQuark(ITmfStateSystem ss, String spanName, List<Integer> logsQuarks) {
        for (int logsQuark : logsQuarks) {
            if (ss.getAttributeName(logsQuark).equals(getSpanId(spanName))) {
                return logsQuark;
            }
        }
        return ITmfStateSystem.INVALID_ATTRIBUTE;
    }

    private static String getSpanName(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        // The span name could contain a '/' character
        return String.join("/",
                Arrays.copyOfRange(attributeInfo, 0, attributeInfo.length - 5));
    }

    private static String getSpanId(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 5];
    }

    private static Boolean getErrorTag(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 4].equals("true"); //$NON-NLS-1$
    }

    private static String getProcessName(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 3];
    }

    private static @Nullable Integer getTid(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        Integer tid = (attributeInfo.length > 5)
                ? Integer.valueOf(attributeInfo[attributeInfo.length - 2])
                : -1;
        return (tid < 1) ? null : tid;
    }

    private static String getHostId(String attributeName) {
        String[] attributeInfo = attributeName.split("/");  //$NON-NLS-1$
        return attributeInfo[attributeInfo.length - 1];
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
