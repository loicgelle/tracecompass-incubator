/*******************************************************************************
 * Copyright (c) 2018 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.kernel.ui.listener;

import org.eclipse.tracecompass.incubator.internal.callstack.ui.views.weightedtree.WeightedTreeView;
import org.eclipse.tracecompass.incubator.internal.kernel.core.criticalpath.CriticalPathAggregated;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.analysis.ITmfNewAnalysisModuleListener;
import org.eclipse.tracecompass.tmf.ui.analysis.TmfAnalysisViewOutput;

/**
 * new module listener to add weighted critical path
 *
 * @author Geneviève Bastien
 */
public class CriticalPathListener implements ITmfNewAnalysisModuleListener {

    @Override
    public void moduleCreated(IAnalysisModule module) {
        if (module instanceof CriticalPathAggregated) {
            module.registerOutput(new TmfAnalysisViewOutput(WeightedTreeView.ID, module.getId()));
        }

    }

}
