/*******************************************************************************
 * Copyright (c) 2018 École Polytechnique de Montréal
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.IWeightedTreeProvider;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.WeightedTree;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.analysis.TmfAbstractAnalysisModule;
import org.eclipse.tracecompass.tmf.core.exceptions.TmfAnalysisException;
import org.eclipse.tracecompass.tmf.core.signal.TmfSignal;
import org.eclipse.tracecompass.tmf.core.signal.TmfSignalHandler;
import org.eclipse.tracecompass.tmf.core.signal.TmfStartAnalysisSignal;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

/**
 *
 *
 * @author Geneviève Bastien
 */
public class SpanLifeCriticalPathAggregated extends TmfAbstractAnalysisModule implements IWeightedTreeProvider<Object, String, WeightedTree<Object>> {

    public static final String ID = "org.eclipse.tracecompass.incubator.callstack.core.criticalpath.aggregated"; //$NON-NLS-1$
    private static final MetricType DURATION_METRIC = new MetricType(Objects.requireNonNull("Duration"), DataType.NANOSECONDS);

    private @Nullable SpanLifeAnalysis fModule = null;

    private @Nullable String fOTTrace = null;
    private Collection<WeightedTree<Object>> fTrees = new ArrayList<>();

    @Override
    protected boolean executeAnalysis(IProgressMonitor monitor) throws TmfAnalysisException {
        SpanLifeAnalysis module = fModule;
        if (module == null) {
            return false;
        }
        if (!module.waitForCompletion(Objects.requireNonNull(monitor))) {
            return false;
        }

        ITmfStateSystem ss = module.getStateSystem();
        if (ss == null || fOTTrace == null) {
            return false;
        }

        int traceQuark = ss.optQuarkAbsolute(fOTTrace);
        if (traceQuark == ITmfStateSystem.INVALID_ATTRIBUTE) {
            return false;
        }
        int spansQuark = ss.optQuarkRelative(traceQuark, SpanLifeStateProvider.OPEN_TRACING_ATTRIBUTE);
        if (spansQuark == ITmfStateSystem.INVALID_ATTRIBUTE) {
            return false;
        }
        for (int q : ss.getSubAttributes(spansQuark, true)) {
            computeStatisticsForSpan(ss, q);
        }

        return true;
    }

    private void computeStatisticsForSpan(ITmfStateSystem ss, int spanQuark) {
        Map<String, WeightedTree<Object>> stateTrees = new HashMap<>();
        String attributeName = ss.getAttributeName(spanQuark);
        String spanUID = SpanLifeStateProvider.getSpanId(attributeName);
        String entryName = "[" + SpanLifeStateProvider.getShortSpanId(attributeName)
                + "] " + SpanLifeStateProvider.getSpanName(attributeName);
        Multimap<String, String> metadata = LinkedHashMultimap.create();
        metadata.put("SpanUID", spanUID);
        WeightedTree<Object> tree = new WeightedTree<>(entryName, metadata, metadata);
        int cpQuark = ss.optQuarkAbsolute(SpanLifeStateProvider.CP_AGGREGATED_ATTRIBUTE, spanUID);
        if (cpQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
            try {
                for (ITmfStateInterval interval : ss.query2D(Collections.singleton(cpQuark),
                                                             ss.getStartTime(), ss.getCurrentEndTime())) {
                    String value = interval.getValueString();
                    if (value == null) {
                        continue;
                    }
                    WeightedTree<Object> stateTree = stateTrees.get(value);
                    if (stateTree == null) {
                        if (value.startsWith(SpanLifeStateProvider.BLOCKED_BY)) {
                            String blockingSpanUID = value.substring(SpanLifeStateProvider.BLOCKED_BY.length());
                            System.out.println(blockingSpanUID);
                            Multimap<String, String> blockingMetadata = LinkedHashMultimap.create();
                            Multimap<String, String> blockingCaptureMetadata = LinkedHashMultimap.create();
                            blockingMetadata.put("SpanUID", blockingSpanUID);
                            stateTree = new WeightedTree<>(value, blockingMetadata, blockingCaptureMetadata);
                        } else {
                            stateTree = new WeightedTree<>(value);
                        }
                        tree.addChild(stateTree);
                    }
                    stateTree.addToWeight(interval.getEndTime() - interval.getStartTime());
                    tree.addToWeight(interval.getEndTime() - interval.getStartTime());
                    stateTrees.put(value, stateTree);
                }
            } catch (IndexOutOfBoundsException | TimeRangeException | StateSystemDisposedException e) {
                e.printStackTrace();
            }
        }
        if (tree.getWeight() > 0) {
            fTrees.add(tree);
        }
    }

    /**
     * Signal handler for analysis started, we need to rebuilt the entry list with
     * updated statistics values for the current graph worker of the critical path
     * module.
     *
     * @param signal
     *            The signal
     */
    @TmfSignalHandler
    public void analysisStarted(TmfStartAnalysisSignal signal) {
        IAnalysisModule analysis = signal.getAnalysisModule();
        if (analysis instanceof SpanLifeAnalysis) {
            SpanLifeAnalysis spanLifeAnalysis = (SpanLifeAnalysis) analysis;
            Collection<ITmfTrace> traces = TmfTraceManager.getTraceSetWithExperiment(getTrace());
            if (traces.contains(spanLifeAnalysis.getTrace())) {
                cancel();
                fModule = spanLifeAnalysis;
                fOTTrace = null;
                fTrees = new ArrayList<>();
                resetAnalysis();
                schedule();
            }
        }
    }

    @TmfSignalHandler
    public void traceSelected(TmfSignal signal) {
        if (signal instanceof OTTraceSelectedSignal) {
            cancel();
            OTTraceSelectedSignal traceSignal = (OTTraceSelectedSignal) signal;
            fOTTrace = traceSignal.getTraceID();
            fTrees = new ArrayList<>();
            resetAnalysis();
            schedule();
        }
    }

    @Override
    protected void canceling() {
    }

//    public void addListener(ICriticalPathListener listener) {
//        fListeners.add(listener);
//    }
//
//    public void removeListener(ICriticalPathListener listener) {
//        fListeners.remove(listener);
//    }

//    @Override
//    public @Nullable ITmfStatistics getStatistics() {
//        return fCritPathCg;
//    }

    @Override
    public Collection<WeightedTree<Object>> getTreesFor(String element) {
        if (fOTTrace != null) {
            return fTrees;
        }
        return Collections.EMPTY_SET;
    }

    @Override
    public Collection<WeightedTree<Object>> getTrees(String element, ITmfTimestamp start, ITmfTimestamp end) {
        return Collections.EMPTY_SET;
    }

    @Override
    public Collection<String> getElements() {
        if (fOTTrace != null) {
            return Collections.singleton(fOTTrace);
        }
        return Collections.EMPTY_SET;
    }

    @Override
    public MetricType getWeightType() {
        return DURATION_METRIC;
    }

    @Override
    public String getTitle() {
        // TODO
        return "State of the request"; //$NON-NLS-1$
    }

}
