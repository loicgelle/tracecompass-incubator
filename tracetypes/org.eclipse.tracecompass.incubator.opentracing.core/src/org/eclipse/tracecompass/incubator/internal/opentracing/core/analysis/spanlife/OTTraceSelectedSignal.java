package org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife;

import org.eclipse.tracecompass.tmf.core.signal.TmfSignal;

public class OTTraceSelectedSignal extends TmfSignal {

    private String fTraceID;

    public OTTraceSelectedSignal(Object source, String traceID) {
        super(source);
        fTraceID = traceID;
    }

    public String getTraceID() {
        return fTraceID;
    }

}
