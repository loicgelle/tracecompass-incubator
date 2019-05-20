/*******************************************************************************
 * Copyright (c) 2015 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfGraph;
import org.eclipse.tracecompass.analysis.graph.core.criticalpath.CriticalPathModule;
import org.eclipse.tracecompass.analysis.os.linux.core.execution.graph.OsWorker;
import org.eclipse.tracecompass.analysis.os.linux.core.model.HostThread;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.analysis.TmfAbstractAnalysisParamProvider;
import org.eclipse.tracecompass.tmf.core.signal.TmfSignalManager;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;

/**
 * Class that provides parameters to the critical path analysis for lttng kernel
 * traces
 *
 * @author Geneviève Bastien
 */
public class SpanLifeCriticalPathParameterProvider extends TmfAbstractAnalysisParamProvider {

    private static final String NAME = "Critical Path Lttng kernel parameter provider"; //$NON-NLS-1$

    private static @Nullable SpanLifeCriticalPathParameterProvider fInstance = null;
    private @Nullable HostThread fCurrentHostThread = null;

    /**
     * Constructor
     */
    public SpanLifeCriticalPathParameterProvider() {
        super();
        fInstance = this;
        TmfSignalManager.register(this);
    }

    @Override
    public void dispose() {
        super.dispose();
        TmfSignalManager.deregister(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean appliesToTrace(@Nullable ITmfTrace trace) {
        return true;
    }

    @Override
    public @Nullable Object getParameter(@Nullable String name) {
        if (name == null) {
            return null;
        }
        if (name.equals(CriticalPathModule.PARAM_WORKER)) {
            final HostThread currentHostThread = fCurrentHostThread;
            if (currentHostThread == null) {
                return null;
            }
            /* Try to find the worker for the critical path */
            IAnalysisModule mod = getModule();
            if ((mod != null) && (mod instanceof CriticalPathModule)) {
                OsWorker worker = new OsWorker(currentHostThread, "", 0); //$NON-NLS-1$
                return worker;
            }
        }
        return null;
    }

    public @Nullable TmfGraph getCriticalPath(HostThread hostThread) {
        IAnalysisModule mod = getModule();
        if ((mod == null) || !(mod instanceof CriticalPathModule)) {
              return null;
        }
        CriticalPathModule module = (CriticalPathModule) mod;
        fCurrentHostThread = hostThread;
        notifyParameterChanged(CriticalPathModule.PARAM_WORKER);
        module.waitForCompletion();
        return module.getCriticalPath();
    }

    public static @Nullable SpanLifeCriticalPathParameterProvider getInstance() {
        return fInstance;
    }

    public void resetParameter() {
        fCurrentHostThread = null;
    }

}
