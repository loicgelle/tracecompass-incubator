/*******************************************************************************
 * Copyright (c) 2015, 2016 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Alexis Cabana-Loriaux - Initial API and implementation
 *
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.callstack.ui.views.weightedtree;

import java.text.Format;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.ListenerList;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.linuxtools.dataviewers.piechart.PieChart;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.IWeightedTreeProvider;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.WeightedTree;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.ui.viewers.TmfTimeViewer;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.widgets.TimeGraphColorScheme;

/**
 * Creates a viewer containing 2 pie charts, one for showing information about
 * the current selection, and the second one for showing information about the
 * current time-range selection. It follows the MVC pattern, being a view.
 *
 * This class is closely related with the IPieChartViewerState interface that
 * acts as a state machine for the general layout of the charts.
 *
 * @author Geneviève Bastien
 * @since 2.0
 *
 */
public class WeightedTreePieChartViewer extends TmfTimeViewer {

    /**
     * Represents the minimum percentage a slice of pie must have in order to be
     * shown
     */
    private static final float MIN_PRECENTAGE_TO_SHOW_SLICE = 0.01F;// 1%

    /**
     * The name of the slice containing the too little slices
     */
    private static String OTHER_SLICE_NAME = "Other";

    /**
     * The pie chart containing global information about the trace
     */
    private @Nullable PieChart fGlobalPC = null;
    private @Nullable PieChart fSecondaryPc = null;

    /**
     * The listener for the mouse movement event.
     */
    private final Listener fMouseMoveListener;

    /**
     * The listener for the mouse right click event.
     */
    private final MouseListener fMouseClickListener;

    /**
     * The list of listener to notify when an event type is selected
     */
    private ListenerList fSelectedListeners = new ListenerList(ListenerList.IDENTITY);

    private Format fWeightFormatter = WeightedTreeView.DECIMAL_FORMATTER;


    /**
     * Represents the maximum number of slices of the pie charts. WE don't want
     * to pollute the viewer with too much slice entries.
     */
    // private static final int NB_MAX_SLICES = 10;

    /** The color scheme for the chart */
    private TimeGraphColorScheme fColorScheme = new TimeGraphColorScheme();
    private final WeightedTreeView fView;



    /**
     * @param parent
     *            The parent composite that will hold the viewer
     * @param view
     *            The parent weighted tree view
     */
    public WeightedTreePieChartViewer(@Nullable Composite parent, WeightedTreeView view) {
        super(parent);
        if (parent != null) {
            parent.addDisposeListener(e -> {
                fColorScheme.dispose();
            });
        }
        fView = view;
     // Setup listeners for the tooltips
        fMouseMoveListener = new Listener() {
            @Override
            public void handleEvent(@Nullable Event event) {
                if (event == null) {
                    return;
                }
                PieChart pc = (PieChart) event.widget;
                switch (event.type) {
                /* Get tooltip information on the slice */
                case SWT.MouseMove:
                    int sliceIndex = pc.getSliceIndexFromPosition(0, event.x, event.y);
                    if (sliceIndex < 0) {
                        // mouse is outside the chart
                        pc.setToolTipText(null);
                        break;
                    }
                    float percOfSlice = (float) pc.getSlicePercent(0, sliceIndex);
                    String percent = String.format("%.1f", percOfSlice); //$NON-NLS-1$
                    Long nbEvents = Long.valueOf((long) pc.getSeriesSet().getSeries()[sliceIndex].getXSeries()[0]);

                    String text = "slide name = " + //$NON-NLS-1$
                            pc.getSeriesSet().getSeries()[sliceIndex].getId() + "\n"; //$NON-NLS-1$

                    text += "Slice value = "//$NON-NLS-1$
                            + fWeightFormatter.format(nbEvents) + " (" + percent + "%)"; //$NON-NLS-1$ //$NON-NLS-2$
                    pc.setToolTipText(text);
                    return;
                default:
                }
            }
        };

        fMouseClickListener = new MouseListener() {

            @Override
            public void mouseUp(@Nullable MouseEvent e) {
            }

            @Override
            public void mouseDown(@Nullable MouseEvent e) {
                if (e == null) {
                    return;
                }
                PieChart pc = (PieChart) e.widget;
                int slicenb = pc.getSliceIndexFromPosition(0, e.x, e.y);
                if (slicenb < 0 || slicenb >= pc.getSeriesSet().getSeries().length) {
                    // mouse is outside the chart
                    return;
                }
                Event selectionEvent = new Event();
                selectionEvent.text = pc.getSeriesSet().getSeries()[slicenb].getId();
                notifySelectionListener(selectionEvent);
            }

            @Override
            public void mouseDoubleClick(@Nullable MouseEvent e) {
            }
        };
    }

    // ------------------------------------------------------------------------
    // Class methods
    // ------------------------------------------------------------------------

    @Override
    public void loadTrace(@Nullable ITmfTrace trace) {
        super.loadTrace(trace);
        if (trace == null) {
            return;
        }
        Thread thread = new Thread() {
            @Override
            public void run() {
                initializeDataSource(trace);
                Display.getDefault().asyncExec(new Runnable() {
                    @Override
                    public void run() {
                        if (!trace.equals(getTrace())) {
                            return;
                        }
                        // clearContent();
                        updateContent(getWindowStartTime(), getWindowEndTime(), false);
                    }
                });
            }
        };
        thread.start();
    }

    /**
     * Called by this class' constructor. Constructs the basic viewer containing
     * the charts, as well as their listeners
     */
    private synchronized void initializeDataSource(ITmfTrace trace) {
        Set<IWeightedTreeProvider<?, ?, WeightedTree<?>>> modules = fView.getWeightedTrees(trace);

        modules.forEach(m -> {
            if (m instanceof IAnalysisModule) {
                ((IAnalysisModule) m).schedule();
            }
        });

        // setLayout(new FillLayout());
        //
        // fGlobalPC = null;
        // fTimeRangePC = null;


    }

    /**
     * Requests an update of the viewer's content in a given time range or
     * selection time range. An extra parameter defines whether these times
     * correspond to the selection or the visible range, as the viewer may
     * update differently in those cases.
     *
     * @param start
     *            The start time of the requested content
     * @param end
     *            The end time of the requested content
     * @param isSelection
     *            <code>true</code> if this time range is for a selection,
     *            <code>false</code> for the visible time range
     */
    protected void updateContent(final long start, final long end, final boolean isSelection) {
        ITmfTrace trace = getTrace();
        if (trace == null) {
            return;
        }
        Job thread = new Job("") { //$NON-NLS-1$
            @Override
            public IStatus run(@Nullable IProgressMonitor monitor) {
                // final ITmfTreeViewerEntry rootEntry = updateElements(trace,
                // start, end, isSelection);
                // /* Set the input in main thread only if it didn't change */
                // if (rootEntry != null) {
                // Display.getDefault().asyncExec(new Runnable() {
                // @Override
                // public void run() {
                // if (fTreeViewer.getControl().isDisposed()) {
                // return;
                // }
                //
                // if (rootEntry != fTreeViewer.getInput()) {
                // fTreeViewer.setInput(rootEntry);
                // contentChanged(rootEntry);
                // } else {
                // fTreeViewer.refresh();
                // }
                // // FIXME should add a bit of padding
                // for (TreeColumn column : fTreeViewer.getTree().getColumns())
                // {
                // column.pack();
                // }
                // }
                // });
                // }
                return Status.OK_STATUS;
            }
        };
        thread.setSystem(true);
        thread.schedule();
    }

    /**
     * Updates the data contained in the Global PieChart by using a Map.
     * Normally, this method is only called by the state machine.
     *
     * @param treeProvider
     */
    synchronized void updateGlobalPieChart(Set<WeightedTree<?>> trees, IWeightedTreeProvider<?, ?, WeightedTree<?>> treeProvider) {
        PieChart pie = getGlobalPC();
        if (pie == null) {
            pie = new PieChart(getParent(), SWT.NONE);
            Color backgroundColor = fColorScheme.getColor(TimeGraphColorScheme.TOOL_BACKGROUND);
            Color foregroundColor = fColorScheme.getColor(TimeGraphColorScheme.TOOL_FOREGROUND);
            pie.getTitle().setText(treeProvider.getTitle());
            pie.getTitle().setForeground(foregroundColor);
            pie.setBackground(backgroundColor);
            pie.setForeground(foregroundColor);
            pie.getAxisSet().getXAxis(0).getTitle().setText(""); // Hide //$NON-NLS-1$
                                                                 // the
                                                                 // title
                                                                 // over
                                                                 // the
                                                                 // legend
            pie.getAxisSet().getXAxis(0).getTitle().setForeground(foregroundColor);
            pie.getLegend().setVisible(true);
            pie.getLegend().setPosition(SWT.RIGHT);
            pie.getLegend().setBackground(backgroundColor);
            pie.getLegend().setForeground(foregroundColor);
            pie.addListener(SWT.MouseMove, fMouseMoveListener);
            pie.addMouseListener(fMouseClickListener);
            fGlobalPC = pie;
            fWeightFormatter = WeightedTreeView.getFormatterForType(treeProvider.getWeightType().getDataType());
        }

        updatePieChartWithData(pie, trees, treeProvider, MIN_PRECENTAGE_TO_SHOW_SLICE, OTHER_SLICE_NAME);
        pie.redraw();
    }

    synchronized void updateSecondaryPieChart(Collection<WeightedTree<?>> trees, IWeightedTreeProvider<?, ?, WeightedTree<?>> treeProvider) {
        PieChart pie = getSecondaryPc();
        if (pie == null) {
            pie = new PieChart(getParent(), SWT.NONE);
            Color backgroundColor = fColorScheme.getColor(TimeGraphColorScheme.TOOL_BACKGROUND);
            Color foregroundColor = fColorScheme.getColor(TimeGraphColorScheme.TOOL_FOREGROUND);
            pie.getTitle().setText(treeProvider.getTitle());
            pie.getTitle().setForeground(foregroundColor);
            pie.setBackground(backgroundColor);
            pie.setForeground(foregroundColor);
            pie.getAxisSet().getXAxis(0).getTitle().setText(""); // Hide //$NON-NLS-1$
                                                                 // the
                                                                 // title
                                                                 // over
                                                                 // the
                                                                 // legend
            pie.getAxisSet().getXAxis(0).getTitle().setForeground(foregroundColor);
            pie.getLegend().setVisible(true);
            pie.getLegend().setPosition(SWT.RIGHT);
            pie.getLegend().setBackground(backgroundColor);
            pie.getLegend().setForeground(foregroundColor);
            pie.addListener(SWT.MouseMove, fMouseMoveListener);
            pie.addMouseListener(fMouseClickListener);
            fGlobalPC = pie;
            fWeightFormatter = WeightedTreeView.getFormatterForType(treeProvider.getWeightType().getDataType());
        }

        updatePieChartWithData(pie, trees, treeProvider, MIN_PRECENTAGE_TO_SHOW_SLICE, OTHER_SLICE_NAME);
        pie.redraw();
    }

    /**
     * Updates the data contained in the Time-Range PieChart by using a Map.
     * Normally, this method is only called by the state machine.
     */
    synchronized void updateTimeRangeSelectionPieChart() {
        // if (getTimeRangePC() == null) {
        // Color backgroundColor =
        // fColorScheme.getColor(TimeGraphColorScheme.TOOL_BACKGROUND);
        // Color foregroundColor =
        // fColorScheme.getColor(TimeGraphColorScheme.TOOL_FOREGROUND);
        // fTimeRangePC = new PieChart(this, SWT.NONE);
        // fTimeRangePC.setBackground(backgroundColor);
        // fTimeRangePC.setForeground(foregroundColor);
        // getTimeRangePC().getTitle().setText(fTimeRangePCname);
        // getTimeRangePC().getTitle().setForeground(foregroundColor);
        // getTimeRangePC().getAxisSet().getXAxis(0).getTitle().setText(""); //
        // Hide //$NON-NLS-1$
        // // the
        // // title
        // // over
        // // the
        // // legend
        // getTimeRangePC().getAxisSet().getXAxis(0).getTitle().setForeground(foregroundColor);
        // getTimeRangePC().getLegend().setPosition(SWT.BOTTOM);
        // getTimeRangePC().getLegend().setVisible(true);
        // getTimeRangePC().getLegend().setBackground(backgroundColor);
        // getTimeRangePC().getLegend().setForeground(foregroundColor);
        // getTimeRangePC().addListener(SWT.MouseMove, fMouseMoveListener);
        // getTimeRangePC().addMouseListener(fMouseClickListener);
        // } else if (getTimeRangePC().isDisposed()) {
        // return;
        // }
        //
        // Map<String, Long> totalEventCountForChart =
        // getTotalEventCountForChart(false);
        //
        // if (totalEventCountForChart == null) {
        // return;
        // }
        //
        // updatePieChartWithData(fTimeRangePC, totalEventCountForChart,
        // MIN_PRECENTAGE_TO_SHOW_SLICE, fOthersSliceName);
    }

    /**
     * Reinitializes the charts to their initial state, without any data
     */
    public synchronized void reinitializeCharts() {
        // if (isDisposed()) {
        // return;
        // }
        //
        // if (getGlobalPC() != null && !getGlobalPC().isDisposed()) {
        // getGlobalPC().dispose();
        // }
        // fGlobalPC = new PieChart(this, SWT.NONE);
        // getGlobalPC().getTitle().setText(fGlobalPCname);
        // getGlobalPC().getAxisSet().getXAxis(0).getTitle().setText(""); //
        // Hide //$NON-NLS-1$
        // // the
        // // title
        // // over
        // // the
        // // legend
        // if (getTimeRangePC() != null && !getTimeRangePC().isDisposed()) {
        // getTimeRangePC().dispose();
        // fTimeRangePC = null;
        // }
        // layout();
        // setCurrentState(new PieChartViewerStateNoContentSelected(this));
    }

    /**
     * Function used to update or create the slices of a PieChart to match the
     * content of a Map passed in parameter. It also provides a facade to use
     * the PieChart API
     *
     * @param treeProvider
     * @param minimumSizeOfSlice
     * @param nameOfOthers
     */
    private static void updatePieChartWithData(
            final PieChart chart,
            final Collection<WeightedTree<?>> trees,
            IWeightedTreeProvider<?, ?, WeightedTree<?>> treeProvider, final float minimumSizeOfSlice,
            final String nameOfOthers) {

        if (trees.isEmpty()) {
            return;
        }
        // Get the total weights
        long totalWeight = 0;
        for (WeightedTree<?> tree : trees) {
            totalWeight += tree.getWeight();
        }

        long otherWeight = 0;
        // Add to the list only the trees that would be visible (> threshold),
        // add the rest to an "other" element
        List<WeightedTree<?>> list = new ArrayList<>();
        for (WeightedTree<?> tree : trees) {
            if ((float) tree.getWeight() / (float) totalWeight > MIN_PRECENTAGE_TO_SHOW_SLICE) {
                list.add(tree);
            } else {
                otherWeight += tree.getWeight();
            }
        }
        Collections.sort(list);

        double[][] tempValues = new double[list.size() + 1][1];
        String[] tempNames = new String[list.size() + 1];
        int i = 0;
        for (WeightedTree<?> tree : list) {
            tempNames[i] = treeProvider.toDisplayString(tree);
            tempValues[i][0] = tree.getWeight();
            i++;
        }
        tempNames[list.size()] = "OTHERS"; //$NON-NLS-1$
        tempValues[list.size()][0] = otherWeight;

        chart.addPieChartSeries(tempNames, tempValues);
    }

    /**
     * Refresh this viewer
     *
     * @param refreshGlobal
     *            if we have to refresh the global piechart
     * @param refreshSelection
     *            if we have to refresh the selection piechart
     */
    public synchronized void refresh(boolean refreshGlobal, boolean refreshSelection) {
        // if (fModel == null) {
        // reinitializeCharts();
        // } else {
        // if (refreshGlobal) {
        // /* will update the global pc */
        // getCurrentState().newGlobalEntries(this);
        // }
        //
        // if (refreshSelection) {
        // // Check if the selection is empty
        // int nbEventsType = 0;
        // Map<String, Long> selectionModel = getTotalEventCountForChart(false);
        // for (Long l : selectionModel.values()) {
        // if (l != 0) {
        // nbEventsType++;
        // }
        // }
        //
        // // Check if the selection is empty or if
        // // there is enough event types to show in the piecharts
        // if (nbEventsType < 2) {
        // getCurrentState().newEmptySelection(this);
        // } else {
        // getCurrentState().newSelection(this);
        // }
        // }
        // }
    }

    /**
     * @param l
     *            the listener to add
     */
    public void addSelectionListener(Listener l) {
        fSelectedListeners.add(l);
    }

    /**
     * @param l
     *            the listener to remove
     */
    public void removeSelectionListener(Listener l) {
        fSelectedListeners.remove(l);
    }

    /* Notify all listeners that an event type has been selected */
    private void notifySelectionListener(Event e) {
        for (Object o : fSelectedListeners.getListeners()) {
            ((Listener) o).handleEvent(e);
        }
    }

    // ------------------------------------------------------------------------
    // Getters
    // ------------------------------------------------------------------------

    /**
     * @return the global piechart
     */
    synchronized @Nullable PieChart getGlobalPC() {
        return fGlobalPC;
    }

    /**
     * @return the time-range selection piechart
     */
     synchronized @Nullable PieChart getSecondaryPc() {
     return fSecondaryPc;
     }

    // ------------------------------------------------------------------------
    // Setters
    // ------------------------------------------------------------------------

    /**
     * An element has been selected
     *
     * @param trees
     *            The selected elements
     * @param treeProvider
     *            The tree provider for the selected trees
     */
    public void elementSelected(Set<WeightedTree<?>> trees, IWeightedTreeProvider<?, ?, WeightedTree<?>> treeProvider) {
        updateGlobalPieChart(trees, treeProvider);
    }

    @Override
    public @Nullable Control getControl() {
        return getParent();
    }

    @Override
    public void refresh() {

    }

    /**
     * An element has been selected
     *
     * @param collection
     *            The selected elements
     * @param treeProvider
     *            The tree provider for the selected trees
     */
    public void secondarySelection(Collection<WeightedTree<?>> collection, IWeightedTreeProvider<?, ?, WeightedTree<?>> treeProvider) {
        updateSecondaryPieChart(collection, treeProvider);
    }

}
