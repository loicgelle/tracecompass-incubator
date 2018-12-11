/*******************************************************************************
 * Copyright (c) 2018 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.kernel.core.criticalpath;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.analysis.graph.core.criticalpath.CriticalPathModule;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.IWeightedTreeProvider;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.WeightedTree;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.analysis.TmfAbstractAnalysisModule;
import org.eclipse.tracecompass.tmf.core.exceptions.TmfAnalysisException;
import org.eclipse.tracecompass.tmf.core.signal.TmfSignalHandler;
import org.eclipse.tracecompass.tmf.core.signal.TmfStartAnalysisSignal;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;

/**
 *
 *
 * @author Geneviève Bastien
 */
public class CriticalPathAggregated extends TmfAbstractAnalysisModule implements IWeightedTreeProvider<Object, String, WeightedTree<Object>> {

    public static final String ID = "org.eclipse.tracecompass.incubator.callstack.core.criticalpath.aggregated"; //$NON-NLS-1$
    private static final MetricType DURATION_METRIC = new MetricType(Objects.requireNonNull("Duration"), DataType.NANOSECONDS);

    private List<ICriticalPathListener> fListeners = new ArrayList<>();
    private @Nullable CriticalPathModule fModule = null;
    private @Nullable CriticalPathToCallGraph fCritPathCg = null;

    @Override
    protected boolean executeAnalysis(IProgressMonitor monitor) throws TmfAnalysisException {
        CriticalPathModule module = fModule;
        if (module == null) {
            return false;
        }
        if (!module.waitForCompletion(Objects.requireNonNull(monitor))) {
            return false;
        }
        CriticalPathToCallGraph critPathCallGraph = new CriticalPathToCallGraph(module.getCriticalPath());
        fCritPathCg = critPathCallGraph;
        for (ICriticalPathListener listener : fListeners) {
            listener.update(critPathCallGraph);
        }
        return true;
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
        if (analysis instanceof CriticalPathModule) {
            CriticalPathModule criticalPath = (CriticalPathModule) analysis;
            Collection<ITmfTrace> traces = TmfTraceManager.getTraceSetWithExperiment(getTrace());
            if (traces.contains(criticalPath.getTrace())) {
                cancel();
                fModule = criticalPath;
                fCritPathCg = null;
                resetAnalysis();
                schedule();
            }
        }
    }

    @Override
    protected void canceling() {
        // TODO Auto-generated method stub

    }

    public void addListener(ICriticalPathListener listener) {
        fListeners.add(listener);
    }

    public void removeListener(ICriticalPathListener listener) {
        fListeners.remove(listener);
    }

//    @Override
//    public @Nullable ITmfStatistics getStatistics() {
//        return fCritPathCg;
//    }

    @Override
    public Collection<WeightedTree<Object>> getTreesFor(String element) {
        CriticalPathToCallGraph critPathCg = fCritPathCg;
        if (critPathCg == null) {
            return Collections.emptyList();
        }
        return critPathCg.getTreesFor(element);
    }

    @Override
    public Collection<WeightedTree<Object>> getTrees(String element, ITmfTimestamp start, ITmfTimestamp end) {
        CriticalPathToCallGraph critPathCg = fCritPathCg;
        if (critPathCg == null) {
            return Collections.emptyList();
        }
        return critPathCg.getTrees(element, start, end);
    }

    @Override
    public Collection<String> getElements() {
        CriticalPathToCallGraph critPathCg = fCritPathCg;
        if (critPathCg == null) {
            return Collections.emptyList();
        }
        return critPathCg.getElements();
    }

    @Override
    public MetricType getWeightType() {
        return DURATION_METRIC;
    }

    @Override
    public String getTitle() {
        return "What the process is waiting for"; //$NON-NLS-1$
    }

}
