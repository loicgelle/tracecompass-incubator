/*******************************************************************************
 * Copyright (c) 2014 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Geneviève Bastien - Initial API and implementation
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.opentracing.core.tests;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.eclipse.test.performance.Dimension;
import org.eclipse.test.performance.Performance;
import org.eclipse.test.performance.PerformanceMeter;
import org.eclipse.tracecompass.analysis.os.linux.core.kernel.KernelAnalysisModule;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.analysis.spanlife.SpanLifeAnalysis;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.event.OpenTracingEvent;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.trace.OpenTracingExperiment;
import org.eclipse.tracecompass.incubator.internal.opentracing.core.trace.OpenTracingTrace;
import org.eclipse.tracecompass.lttng2.kernel.core.trace.LttngKernelTrace;
import org.eclipse.tracecompass.lttng2.ust.core.trace.LttngUstTrace;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.tests.shared.TmfTestHelper;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;
import org.eclipse.tracecompass.tmf.core.trace.experiment.TmfExperiment;
import org.junit.Test;

/**
 * Benchmark trace synchronization
 *
 * @author Geneviève Bastien
 */
public class ScalabilityBenchmark {

    private static final String TEST_ID = "scalability#";
    private static final String TIME = "-time";
    private static final String TEST_SUMMARY = "Trace synchronization";

    private static final String[] EXPERIMENTS = {
            "1-1",
            "1-5",
            "1-10",
            "1-50",
            "1-100",
            "1-1000",
            "1-5000",
            "2-2",
            "2-10",
            "2-20",
            "2-100",
            "2-200",
            "2-2000",
            "2-10000",
            "5-5",
            "5-25",
            "5-50",
            "5-250",
            "5-500",
            "5-5000",
            "5-25000",
            "10-10",
            "10-50",
            "10-100",
            "10-500",
            "10-1000",
            "10-10000",
            "10-50000",
            "20-20",
            "20-100",
            "20-200",
            "20-1000",
            "20-2000",
            "20-20000",
            "20-100000"
    };

    /**
     * Trace opening
     */
    /*@Test
    public void testTraceOpening() {
        for (String expName : EXPERIMENTS) {
            runCpuTest(expName, 60);
        }
    }*/

    /**
     * Control flow view
     */
    /*@Test
    public void testKernelAnalysis() {
        for (String expName : EXPERIMENTS) {
            runControlFlowTest(expName, 20);
        }
    }*/

    /**
     * Span analysis view
     */
    @Test
    public void testSpanLifeAnalysis() {
        for (String expName : EXPERIMENTS) {
            runSpanLifeAnalysisTest(expName, 15);
        }
    }

    /*private static void runCpuTest(String expName, int loop_count) {
        String testName = expName + "#open";
        Performance perf = Performance.getDefault();
        PerformanceMeter pm = perf.createPerformanceMeter(TEST_ID + testName + TIME);
        perf.tagAsSummary(pm, TEST_SUMMARY + ':' + testName + TIME, Dimension.CPU_TIME);

        for (int i = 0; i < loop_count; i++) {
            pm.start();
            TmfTrace ustTrace = new LttngUstTrace();
            TmfTrace kernelTrace = new LttngKernelTrace();
            TmfTrace openTracingTrace = new OpenTracingTrace();
            try {
                ustTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/ust/uid/1000/64-bit",
                        ITmfEvent.class);
                kernelTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/kernel",
                        ITmfEvent.class);
                openTracingTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/traces.json",
                        OpenTracingEvent.class);
                ITmfTrace traces[] = { ustTrace, kernelTrace, openTracingTrace };
                TmfExperiment experiment = new OpenTracingExperiment(
                        ITmfEvent.class,
                        "exp-" + expName, traces,
                        TmfExperiment.DEFAULT_INDEX_PAGE_SIZE, null);
                try {
                    for (ITmfTrace t : traces) {
                        deleteSupplementaryFiles(t);
                        t.dispose();
                    }
                    deleteSupplementaryFiles(experiment);
                    experiment.dispose();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            pm.stop();
        }
        pm.commit();

    }*/

    private static void runControlFlowTest(String expName, int loop_count) {
        String testName = expName + "#cfanalysis";
        Performance perf = Performance.getDefault();
        PerformanceMeter pm = perf.createPerformanceMeter(TEST_ID + testName + TIME);
        perf.tagAsSummary(pm, TEST_SUMMARY + ':' + testName + TIME, Dimension.CPU_TIME);

        try {
            for (int i = 0; i < loop_count; i++) {
                TmfTrace ustTrace = new LttngUstTrace();
                TmfTrace kernelTrace = new LttngKernelTrace();
                TmfTrace openTracingTrace = new OpenTracingTrace();
                ustTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/ust/uid/1000/64-bit",
                        ITmfEvent.class);
                kernelTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/kernel",
                        ITmfEvent.class);
                openTracingTrace.initTrace(
                        null,
                        "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/traces.json",
                        OpenTracingEvent.class);
                ITmfTrace traces[] = { ustTrace, kernelTrace, openTracingTrace };
                TmfExperiment experiment = new OpenTracingExperiment(
                        ITmfEvent.class,
                        "exp-" + expName, traces,
                        TmfExperiment.DEFAULT_INDEX_PAGE_SIZE, null);
                IAnalysisModule module = new KernelAnalysisModule();
                module.setTrace(experiment);
                pm.start();
                TmfTestHelper.executeAnalysis(module);
                pm.stop();
                module.dispose();

                // Cleanup
                for (ITmfTrace t : traces) {
                    deleteSupplementaryFiles(t);
                    t.dispose();
                }
                deleteSupplementaryFiles(experiment);
                experiment.dispose();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        pm.commit();

    }

    private static void runSpanLifeAnalysisTest(String expName, int loop_count) {
        String testName = expName + "#spanlifeanalysis";
        Performance perf = Performance.getDefault();
        PerformanceMeter pm = perf.createPerformanceMeter(TEST_ID + testName + TIME);
        perf.tagAsSummary(pm, TEST_SUMMARY + ':' + testName + TIME, Dimension.CPU_TIME);

        try {
            TmfTrace ustTrace = new LttngUstTrace();
            TmfTrace kernelTrace = new LttngKernelTrace();
            TmfTrace openTracingTrace = new OpenTracingTrace();
            ustTrace.initTrace(
                    null,
                    "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/ust/uid/1000/64-bit",
                    ITmfEvent.class);
            kernelTrace.initTrace(
                    null,
                    "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/kernel",
                    ITmfEvent.class);
            openTracingTrace.initTrace(
                    null,
                    "/home/loicgelle/Data_Drive/Thesis/Bench/AnalysisScalability/" + expName + "/traces.json",
                    OpenTracingEvent.class);
            ITmfTrace traces[] = { ustTrace, kernelTrace, openTracingTrace };
            TmfExperiment experiment = new OpenTracingExperiment(
                    ITmfEvent.class,
                    "exp-" + expName, traces,
                    TmfExperiment.DEFAULT_INDEX_PAGE_SIZE, null);
            IAnalysisModule kernelModule = new KernelAnalysisModule();
            kernelModule.setTrace(experiment);
            TmfTestHelper.executeAnalysis(kernelModule);

            for (int i = 0; i < loop_count; i++) {
                IAnalysisModule module = new SpanLifeAnalysis();
                module.setTrace(experiment);
                pm.start();
                TmfTestHelper.executeAnalysis(module);
                pm.stop();
                module.dispose();
                deleteSpanLifeFile(experiment);
            }
            kernelModule.dispose();
            // Cleanup
            for (ITmfTrace t : traces) {
                deleteSupplementaryFiles(t);
                t.dispose();
            }
            deleteSupplementaryFiles(experiment);
            experiment.dispose();
        } catch (Exception e) {
            e.printStackTrace();
        }
        pm.commit();

    }

    private static void deleteSupplementaryFiles(ITmfTrace trace) throws IOException {
        // Delete supplementary files
        File suppDir = new File(TmfTraceManager.getSupplementaryFileDir(trace));
        FileUtils.deleteDirectory(suppDir);
    }

    private static void deleteSpanLifeFile(ITmfTrace trace) throws IOException {
        // Delete supplementary files
        File suppDir = new File(TmfTraceManager.getSupplementaryFileDir(trace));
        for (File f : FileUtils.listFiles(suppDir, FileFilterUtils.trueFileFilter(), FileFilterUtils.trueFileFilter())) {
            if (f.getName().equals("org.eclipse.tracecompass.incubator.opentracing.analysis.spanlife.ht")) {
                FileUtils.deleteQuietly(f);
            }
        }
    }
}
