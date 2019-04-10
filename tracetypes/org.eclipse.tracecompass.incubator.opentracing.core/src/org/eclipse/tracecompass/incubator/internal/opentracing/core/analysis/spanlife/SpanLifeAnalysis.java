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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfGraph;
import org.eclipse.tracecompass.analysis.os.linux.core.model.HostThread;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.statesystem.ITmfStateProvider;
import org.eclipse.tracecompass.tmf.core.statesystem.TmfStateSystemAnalysisModule;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;

/**
 * Spans life tracker
 *
 * @author Katherine Nadeau
 */
public class SpanLifeAnalysis extends TmfStateSystemAnalysisModule {

    /**
     * ID
     */
    public static final String ID = "org.eclipse.tracecompass.incubator.opentracing.analysis.spanlife"; //$NON-NLS-1$
    private Map<HostThread, TmfGraph> fCriticalPaths;

    /**
     * Constructor
     */
    public SpanLifeAnalysis() {
        setId(ID);
        fCriticalPaths = new HashMap<>();
    }

    @Override
    protected @NonNull ITmfStateProvider createStateProvider() {
        ITmfTrace trace = getTrace();
        return new SpanLifeStateProvider(Objects.requireNonNull(trace));
    }

    @Override
    protected boolean executeAnalysis(@Nullable IProgressMonitor monitor) {
        if (!super.executeAnalysis(monitor)) {
            return false;
        }

        SpanLifeCriticalPathParameterProvider cpParamProvider = SpanLifeCriticalPathParameterProvider.getInstance();
        if (cpParamProvider == null) {
            return true;
        }
        ITmfStateSystem ss = this.getStateSystem();
        if (ss == null) {
            return true;
        }

        Collection<Integer> queryQuarks = new ArrayList<>();
        Integer cpQuark = ss.optQuarkAbsolute(SpanLifeStateProvider.CP_TIDS_ATTRIBUTE);
        if (cpQuark != ITmfStateSystem.INVALID_ATTRIBUTE) {
            queryQuarks.addAll(ss.getSubAttributes(cpQuark, false));
        }
        try {
            for (ITmfStateInterval interval :ss.query2D(queryQuarks, ss.getStartTime(), ss.getCurrentEndTime())) {
                if (interval.getValue() != null) {
                    if ((Integer) interval.getValue() == 1) {
                        String quarkName = ss.getAttributeName(interval.getAttribute());
                        String[] spl = quarkName.split("/");
                        if (spl.length != 2) {
                            continue;
                        }
                        int tid = Integer.valueOf(spl[0]);
                        String hostId = spl[1];
                        HostThread hostThread = new HostThread(hostId, tid);
                        TmfGraph criticalPath = cpParamProvider.getCriticalPath(hostThread);
                        if (criticalPath != null) {
                            fCriticalPaths.put(hostThread, criticalPath);
                        }
                    }
                }
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        } catch (TimeRangeException e) {
            e.printStackTrace();
        } catch (StateSystemDisposedException e) {
            e.printStackTrace();
        }

        cpParamProvider.dispose();

        return true;
    }

    public @Nullable TmfGraph getCriticalPath(HostThread hostThread) {
        return fCriticalPaths.get(hostThread);
    }

}
