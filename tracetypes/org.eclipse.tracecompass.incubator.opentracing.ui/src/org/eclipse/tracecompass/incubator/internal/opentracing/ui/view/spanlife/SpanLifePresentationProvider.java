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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.RGBA;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeEntryModel;
import org.eclipse.tracecompass.internal.analysis.graph.ui.criticalpath.view.CriticalPathPresentationProvider;
import org.eclipse.tracecompass.internal.analysis.graph.ui.criticalpath.view.CriticalPathPresentationProvider.State;
import org.eclipse.tracecompass.tmf.core.model.filters.SelectionTimeQueryFilter;
import org.eclipse.tracecompass.tmf.core.model.timegraph.ITimeGraphDataProvider;
import org.eclipse.tracecompass.tmf.core.model.timegraph.TimeGraphEntryModel;
import org.eclipse.tracecompass.tmf.core.presentation.IYAppearance;
import org.eclipse.tracecompass.tmf.core.presentation.RGBAColor;
import org.eclipse.tracecompass.tmf.core.response.TmfModelResponse;
import org.eclipse.tracecompass.tmf.ui.views.timegraph.BaseDataProviderTimeGraphView;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.StateItem;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.TimeGraphPresentationProvider;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.ILinkEvent;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.ITimeEvent;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.ITimeEventStyleStrings;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.ITimeGraphEntry;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.TimeEvent;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.model.TimeGraphEntry;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.widgets.ITmfTimeGraphDrawingHelper;
import org.eclipse.tracecompass.tmf.ui.widgets.timegraph.widgets.TimeGraphControl;

import com.google.common.collect.ImmutableMap;

//import com.google.common.collect.ImmutableMap;

/**
 * Span life presentation provider
 *
 * @author Katherine Nadeau
 */
public class SpanLifePresentationProvider extends TimeGraphPresentationProvider {

    private static final @NonNull String ERROR = "error"; //$NON-NLS-1$
    private static final @NonNull String EVENT = "event"; //$NON-NLS-1$
    private static final @NonNull String MESSAGE = "message"; //$NON-NLS-1$
    private static final @NonNull String STACK = "stack"; //$NON-NLS-1$
    private static final @NonNull String OTHER = "other"; //$NON-NLS-1$
    private static final @NonNull String FLAG_EMOJI = "🏳️"; //$NON-NLS-1$

    private static final @NonNull RGBA MARKER_COLOR = new RGBA(200, 0, 0, 150);
    private static final int MARKER_COLOR_INT = MARKER_COLOR.hashCode();
    /**
     * Only states available
     */
    private static final StateItem[] STATE_TABLE = { new StateItem(new RGB(179,205,224), "Fist Service Class"), //$NON-NLS-1$
            new StateItem(new RGB(100, 151, 177), "Second Service Class"), //$NON-NLS-1$
            new StateItem(new RGB(0,91,150), "Third Service Class"), //$NON-NLS-1$
            new StateItem(new RGB(3, 57, 108), "Forth Service Class"), //$NON-NLS-1$
            new StateItem(new RGB(1, 31, 75), "Fifth Service Class"), //$NON-NLS-1$
            new StateItem(ImmutableMap.of(ITimeEventStyleStrings.label(), ERROR, ITimeEventStyleStrings.fillColor(), MARKER_COLOR_INT, ITimeEventStyleStrings.symbolStyle(), IYAppearance.SymbolStyle.CROSS, ITimeEventStyleStrings.heightFactor(),
                    0.4f)),
            new StateItem(
                    ImmutableMap.of(ITimeEventStyleStrings.label(), EVENT, ITimeEventStyleStrings.fillColor(), MARKER_COLOR_INT, ITimeEventStyleStrings.symbolStyle(), IYAppearance.SymbolStyle.DIAMOND, ITimeEventStyleStrings.heightFactor(), 0.3f)),
            new StateItem(
                    ImmutableMap.of(ITimeEventStyleStrings.label(), MESSAGE, ITimeEventStyleStrings.fillColor(), MARKER_COLOR_INT, ITimeEventStyleStrings.symbolStyle(), IYAppearance.SymbolStyle.CIRCLE, ITimeEventStyleStrings.heightFactor(), 0.3f)),
            new StateItem(ImmutableMap.of(ITimeEventStyleStrings.label(), STACK, ITimeEventStyleStrings.fillColor(), MARKER_COLOR_INT, ITimeEventStyleStrings.symbolStyle(), IYAppearance.SymbolStyle.SQUARE,
                    ITimeEventStyleStrings.heightFactor(), 0.3f)),
            new StateItem(ImmutableMap.of(ITimeEventStyleStrings.label(), OTHER, ITimeEventStyleStrings.fillColor(), MARKER_COLOR_INT, ITimeEventStyleStrings.symbolStyle(), FLAG_EMOJI, ITimeEventStyleStrings.heightFactor(), 0.3f)),
            new StateItem(new RGB(175, 175, 175), "Blocked in critical path"), //$NON-NLS-1$
            new StateItem(
                    ImmutableMap.of(ITimeEventStyleStrings.heightFactor(), 0.1f,
                            ITimeEventStyleStrings.itemTypeProperty(), ITimeEventStyleStrings.linkType(),
                            ITimeEventStyleStrings.fillStyle(), ITimeEventStyleStrings.solidColorFillStyle(),
                            ITimeEventStyleStrings.fillColor(), new RGBAColor(0, 0, 0).toInt()))
    };

    public static final int NETWORK_ARROW_INDEX_1;
    public static final int UNKNOWN_ARROW_INDEX_1;
    public static final StateItem[] CP_STATE_TABLE_1;
    static {
        int networkArrowIndex = -1;
        int unknownNetworkIndex = -1;
        CP_STATE_TABLE_1 = new StateItem[State.values().length];
        for (int i = 0; i < CP_STATE_TABLE_1.length; i++) {
            State state = State.values()[i];

            float heightFactor = 1.0f;
            if (state.equals(State.NETWORK_ARROW)) {
                networkArrowIndex = i;
                heightFactor = 0.1f;
            } else if (state.equals(State.UNKNOWN_ARROW)) {
                unknownNetworkIndex = i;
                heightFactor = 0.1f;
            }

            RGB stateColor = state.rgb;
            String stateType = state.equals(State.NETWORK_ARROW) || state.equals(State.UNKNOWN_ARROW) ? ITimeEventStyleStrings.linkType() : ITimeEventStyleStrings.stateType();
            ImmutableMap<String, Object> styleMap = ImmutableMap.of(
                    ITimeEventStyleStrings.fillStyle(), ITimeEventStyleStrings.solidColorFillStyle(),
                    ITimeEventStyleStrings.fillColor(), new RGBAColor(stateColor.red, stateColor.green, stateColor.blue).toInt(),
                    ITimeEventStyleStrings.label(), String.valueOf(state.toString()),
                    ITimeEventStyleStrings.heightFactor(), heightFactor,
                    ITimeEventStyleStrings.itemTypeProperty(), stateType
                    );
            CP_STATE_TABLE_1[i] = new StateItem(styleMap);
        }

        NETWORK_ARROW_INDEX_1 = networkArrowIndex;
        UNKNOWN_ARROW_INDEX_1 = unknownNetworkIndex;
    }

    public static final int NETWORK_ARROW_INDEX_2;
    public static final int UNKNOWN_ARROW_INDEX_2;
    public static final StateItem[] CP_STATE_TABLE_2;
    static {
        int networkArrowIndex = -1;
        int unknownNetworkIndex = -1;
        CP_STATE_TABLE_2 = new StateItem[State.values().length];
        for (int i = 0; i < CP_STATE_TABLE_2.length; i++) {
            State state = State.values()[i];

            float heightFactor = 1.0f;
            if (state.equals(State.NETWORK_ARROW)) {
                networkArrowIndex = i;
                heightFactor = 0.1f;
            } else if (state.equals(State.UNKNOWN_ARROW)) {
                unknownNetworkIndex = i;
                heightFactor = 0.1f;
            }

            RGB stateColor = state.rgb;
            String stateType = state.equals(State.NETWORK_ARROW) || state.equals(State.UNKNOWN_ARROW) ? ITimeEventStyleStrings.linkType() : ITimeEventStyleStrings.stateType();
            ImmutableMap<String, Object> styleMap = ImmutableMap.of(
                    ITimeEventStyleStrings.fillStyle(), ITimeEventStyleStrings.solidColorFillStyle(),
                    ITimeEventStyleStrings.fillColor(), new RGBAColor(stateColor.red, stateColor.green, stateColor.blue, 150).toInt(),
                    ITimeEventStyleStrings.label(), String.valueOf(state.toString()),
                    ITimeEventStyleStrings.heightFactor(), heightFactor,
                    ITimeEventStyleStrings.itemTypeProperty(), stateType
                    );
            CP_STATE_TABLE_2[i] = new StateItem(ImmutableMap.copyOf(styleMap));
        }

        NETWORK_ARROW_INDEX_2 = networkArrowIndex;
        UNKNOWN_ARROW_INDEX_2 = unknownNetworkIndex;
    }

    /**
     * Constructor
     */
    public SpanLifePresentationProvider() {
        super("Span"); //$NON-NLS-1$
    }

    @Override
    public StateItem[] getStateTable() {
        return ArrayUtils.addAll(STATE_TABLE,
                ArrayUtils.addAll(CP_STATE_TABLE_1, CP_STATE_TABLE_2));
    }

    public int getCPStateTableIndex(@Nullable ITimeEvent event) {
        if (event instanceof TimeEvent && ((TimeEvent) event).hasValue()) {
            int value = ((TimeEvent) event).getValue();
            if (value < getCPStateTableLength()) {
                if (event instanceof ILinkEvent) {
                    //return the right arrow item index
                    return CP_STATE_TABLE_1[value]
                                .getStateString()
                                .equals(CriticalPathPresentationProvider.State.NETWORK.toString())
                            ? NETWORK_ARROW_INDEX_1
                                    : UNKNOWN_ARROW_INDEX_1;
                }
                return value;
            }
            if (event instanceof ILinkEvent) {
                //return the right arrow item index
                return CP_STATE_TABLE_2[value]
                            .getStateString()
                            .equals(CriticalPathPresentationProvider.State.NETWORK.toString())
                        ? getCPStateTableLength() + NETWORK_ARROW_INDEX_2
                                : getCPStateTableLength() + UNKNOWN_ARROW_INDEX_2;
            }
            return value;
        }
        return TRANSPARENT;
    }

    @Override
    public Map<String, String> getEventHoverToolTipInfo(ITimeEvent event, long hoverTime) {
        Map<String, String> eventHoverToolTipInfo = super.getEventHoverToolTipInfo(event, hoverTime);
        if (eventHoverToolTipInfo == null) {
            eventHoverToolTipInfo = new LinkedHashMap<>();
        }
        ITimeGraphEntry entry = event.getEntry();
        if (entry instanceof TimeGraphEntry) {
            long id = ((TimeGraphEntry) entry).getModel().getId();
            ITimeGraphDataProvider<? extends TimeGraphEntryModel> provider = BaseDataProviderTimeGraphView.getProvider((TimeGraphEntry) entry);

            long windowStartTime = Long.MIN_VALUE;
            long windowEndTime = Long.MIN_VALUE;
            ITmfTimeGraphDrawingHelper drawingHelper = getDrawingHelper();
            if (drawingHelper instanceof TimeGraphControl) {
                TimeGraphControl timeGraphControl = (TimeGraphControl) drawingHelper;
                windowStartTime = timeGraphControl.getTimeDataProvider().getTime0();
                windowEndTime = timeGraphControl.getTimeDataProvider().getTime1();
            }

            List<@NonNull Long> times = new ArrayList<>();
            times.add(windowStartTime);
            times.add(hoverTime);
            times.add(windowEndTime);

            SelectionTimeQueryFilter filter = new SelectionTimeQueryFilter(times, Collections.singleton(id));
            TmfModelResponse<@NonNull Map<@NonNull String, @NonNull String>> tooltipResponse = provider.fetchTooltip(filter, new NullProgressMonitor());
            Map<@NonNull String, @NonNull String> tooltipModel = tooltipResponse.getModel();
            if (tooltipModel != null) {
                eventHoverToolTipInfo.putAll(tooltipModel);
            }
        }
        return eventHoverToolTipInfo;
    }

    @Override
    public int getStateTableIndex(@Nullable ITimeEvent event) {
        if (event instanceof SpanMarkerEvent) {
            SpanMarkerEvent markerEvent = (SpanMarkerEvent) event;
            String type = markerEvent.getType();
            switch (type) {
            case ERROR:
                return 5;
            case EVENT:
                return 6;
            case MESSAGE:
                return 7;
            case STACK:
                return 8;
            default:
                return 9;
            }
        }
        if ((event instanceof TimeEvent) && ((TimeEvent) event).getValue() != Integer.MIN_VALUE) {
            if ((event.getEntry() instanceof TimeGraphEntry) && (((TimeGraphEntry) event.getEntry()).getModel() instanceof SpanLifeEntryModel)) {
                SpanLifeEntryModel entryModel = ((SpanLifeEntryModel) ((TimeGraphEntry) event.getEntry()).getModel());
                if (event instanceof ILinkEvent) {
                    return 11;
                }
                if (entryModel.getType() == SpanLifeEntryModel.EntryType.SPAN) {
                    String processName = entryModel.getProcessName();
                    // We want a random color but that is the same for 2 spans of the same service
                    return Math.abs(Objects.hash(processName)) % 5;
                }
                if (entryModel.getType() == SpanLifeEntryModel.EntryType.KERNEL) {
                    if (event instanceof TimeEvent && ((TimeEvent) event).hasValue()) {
                        int value = ((TimeEvent) event).getValue();
                        if (value == -1) {
                            return 10;
                        }
                    }
                    return STATE_TABLE.length + getCPStateTableIndex(event);
                }
            }
            return 0;
        }
        return -1;
    }

    public static int getCPStateTableLength() {
        return State.values().length;
    }
}
