/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.opentracing.ui.view.spanlife;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.RGBA;
import org.eclipse.tracecompass.analysis.os.linux.core.model.HostThread;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.OTTraceSelectedSignal;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeAnalysis;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeDataProvider;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeEntryModel;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeEntryModel.LogEvent;
import org.eclipse.tracecompass.incubator.internal.opentracing.ui.Activator;
import org.eclipse.tracecompass.internal.analysis.os.linux.ui.actions.FollowThreadAction;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphEntryModel;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.experiment.TmfExperiment;
import org.eclipse.tracecompass.tmf.ui.views.TmfView;
import org.eclipse.tracecompass.tmf.ui.views.timegraph.BaseDataProviderTimeGraphView;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.TimeGraphPresentationProvider;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.IMarkerEvent;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.ITimeGraphEntry;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.TimeGraphEntry;

/**
 * Simple gantt chart to see the life of the spans
 *
 * @author Katherine Nadeau
 */
public class SpanLifeView extends BaseDataProviderTimeGraphView {

    private class FollowTraceAction extends Action {

        private final TmfView fView;
        private String fTraceID;

        public FollowTraceAction(TmfView source, String traceId) {
            fView = source;
            fTraceID = traceId;
        }

        @Override
        public String getText() {
            return "Compute aggregated critical path statistics for " + fTraceID;
        }

        @Override
        public void run() {
            fView.broadcast(new OTTraceSelectedSignal(fView, fTraceID));
            super.run();
        }

    }

//    private class FollowSpanCPAction extends Action {
//
//        private final SpanLifeView fView;
//        private final String fSpanUID;
//
//        public FollowSpanCPAction(SpanLifeView source, String spanUID) {
//            fView = source;
//            fSpanUID = spanUID;
//        }
//
//        @Override
//        public String getText() {
//            return "Follow critical path for span " + fSpanUID;
//        }
//
//        @Override
//        public void run() {
//            // TODO
//            TraceCompassFilter filter = TraceCompassFilter.fromRegex(
//                    Collections.singleton("arrows/" + fSpanUID), fView.getTrace());
//            TmfFilterAppliedSignal signal = new TmfFilterAppliedSignal(fView, fView.getTrace(), filter);
//            fView.regexFilterApplied(signal);
//            super.run();
//        }
//
//    }

    /**
     * Span life view Id
     */
    public static final String ID = "org.eclipse.tracecompass.incubator.opentracing.ui.view.life.spanlife.view"; //$NON-NLS-1$
    private static final RGBA MARKER_COLOR = new RGBA(200, 0, 0, 150);

    private static final Image ERROR_IMAGE = Objects.requireNonNull(Activator.getDefault()).getImageFromPath("icons/delete_button.gif"); //$NON-NLS-1$


    private static class SpanTreeLabelProvider extends TreeLabelProvider {

        @Override
        public @Nullable Image getColumnImage(@Nullable Object element, int columnIndex) {
            if (columnIndex == 0 && element instanceof TimeGraphEntry) {
                TimeGraphEntry entry = (TimeGraphEntry) element;
                ITimeGraphEntryModel entryModel = entry.getModel();
                if ((entryModel instanceof SpanLifeEntryModel) && ((SpanLifeEntryModel) entryModel).getErrorTag()) {
                    return ERROR_IMAGE;
                }
            }
            return null;
        }
    }

    /**
     * Constructor
     */
    public SpanLifeView() {
        this(ID, new SpanLifePresentationProvider(), SpanLifeAnalysis.ID + SpanLifeDataProvider.SUFFIX);
        setTreeLabelProvider(new SpanTreeLabelProvider());
    }

    /**
     * Extendable constructor
     *
     * @param id
     *            the view ID
     * @param pres
     *            the presentation provider
     * @param dpID
     *            the dataprovider ID
     */
    public SpanLifeView(String id, TimeGraphPresentationProvider pres, String dpID) {
        super(id, pres, dpID);
    }

    @Override
    protected @NonNull List<IMarkerEvent> getViewMarkerList(long startTime, long endTime,
            long resolution, @NonNull IProgressMonitor monitor) {
        ITimeGraphEntry[] expandedElements = getTimeGraphViewer().getExpandedElements();
        List<IMarkerEvent> markers = new ArrayList<>();
        for (ITimeGraphEntry element : expandedElements) {
            if (((TimeGraphEntry) element).getModel() instanceof SpanLifeEntryModel) {
                SpanLifeEntryModel model = (SpanLifeEntryModel) ((TimeGraphEntry) element).getModel();
                for (LogEvent log : model.getLogs()) {
                    markers.add(new SpanMarkerEvent(element, log.getTime(), MARKER_COLOR, log.getType()));
                }
            }
        }
        return markers;
    }

    @Override
    protected void buildEntryList(@NonNull ITmfTrace trace, @NonNull ITmfTrace parentTrace, @NonNull IProgressMonitor monitor) {
        super.buildEntryList((parentTrace instanceof TmfExperiment) ? parentTrace : trace, parentTrace, monitor);
    }

    @Override
    protected void fillTimeGraphEntryContextMenu(@NonNull IMenuManager menuManager) {
        // TODO Auto-generated method stub
        ISelection selection = getSite().getSelectionProvider().getSelection();
        if (selection instanceof StructuredSelection) {
            StructuredSelection sSel = (StructuredSelection) selection;
            if (sSel.getFirstElement() instanceof TimeGraphEntry) {
                TimeGraphEntry entry = (TimeGraphEntry) sSel.getFirstElement();
                if (entry.getModel() instanceof SpanLifeEntryModel) {
                    SpanLifeEntryModel entryModel = (SpanLifeEntryModel) entry.getModel();
                    if (entryModel.getTid() > 0) {
                        HostThread threadId = new HostThread(entryModel.getHostId(), entryModel.getTid());
                        @SuppressWarnings("restriction")
                        FollowThreadAction action = new FollowThreadAction(SpanLifeView.this,
                                                                           String.valueOf(threadId.getTid()),
                                                                           threadId);
                        menuManager.add(action);
//                        String spanUID = SpanLifeStateProvider.getSpanId(entry.getName());
//                        FollowSpanCPAction actionCP = new FollowSpanCPAction(SpanLifeView.this, spanUID);
//                        menuManager.add(actionCP);
                    }
                } else {
                    menuManager.add(new FollowTraceAction(SpanLifeView.this, entry.getName()));
                }
            }
        }
    }

}
