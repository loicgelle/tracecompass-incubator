/*******************************************************************************
 * Copyright (c) 2018 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.internal.kernel.core.criticalpath;

import java.util.Collection;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.analysis.graph.core.base.IGraphWorker;
import org.eclipse.tracecompass.analysis.graph.core.base.ITmfGraphVisitor;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfEdge;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfEdge.EdgeType;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfGraph;
import org.eclipse.tracecompass.analysis.graph.core.base.TmfVertex;
import org.eclipse.tracecompass.analysis.os.linux.core.execution.graph.OsWorker;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.IWeightedTreeProvider;
import org.eclipse.tracecompass.incubator.analysis.core.concepts.WeightedTree;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;

import com.google.common.collect.ImmutableList;

/**
 *
 *
 * @author Geneviève Bastien
 */
public class CriticalPathToCallGraph implements IWeightedTreeProvider<Object, String, WeightedTree<Object>> {

    private static final String ALL_SUFFIX = "_all"; //$NON-NLS-1$
    private static final String PROCESS_SUFFIX = "_proc"; //$NON-NLS-1$

    private final List<String> fElements;
    private WeightedTree<Object> fAggregatedTree;
    private WeightedTree<Object> fTree;
    private WeightedTree<Object> fProcessTree;

    private class graphToCallGraphConverter implements ITmfGraphVisitor {

        private final TmfGraph fGraph;
        private final IGraphWorker fMainWorker;

        public graphToCallGraphConverter(IGraphWorker mainWorker, TmfGraph graph) {
            fGraph = graph;
            fMainWorker = mainWorker;
        }

        @Override
        public void visitHead(TmfVertex vertex) {
            // Nothing to do

        }

        @Override
        public void visit(TmfVertex vertex) {
            // Nothing to do
        }

        @Override
        public void visit(TmfEdge edge, boolean horizontal) {
            addEdgeToElement(edge);
            addEdgeToAggregatedElement(edge);
            addEdgeToProcessElement(edge);
        }

        private void addEdgeToAggregatedElement(TmfEdge edge) {
            // Get the worker to which to attribute this edge, whether vertical
            // or horizontal
            IGraphWorker worker = fGraph.getParentOf(edge.getVertexTo());
            if (worker == null) {
                return;
            }

            // If it's another worker that is running, add a other process running state
            if (worker != fMainWorker && edge.getType().equals(EdgeType.RUNNING)) {
                WeightedTree<Object> callSite = new WeightedTree<>("Other process"); //$NON-NLS-1$
                callSite.addToWeight(edge.getDuration());
                fAggregatedTree.addChild(callSite);
                return;
            }

            // Otherwise, add a first level call that corresponds to the worker
            WeightedTree<Object> callSite = new WeightedTree<>(edge.getType());
            callSite.addToWeight(edge.getDuration());
            fAggregatedTree.addChild(callSite);

        }

        private void addEdgeToElement(TmfEdge edge) {
            // Get the worker to which to attribute this edge, whether vertical
            // or horizontal
            IGraphWorker worker = fGraph.getParentOf(edge.getVertexTo());
            if (worker == null) {
                return;
            }

            // If it is the main worker, just add a 1st level call of the edge
            // type
            if (worker == fMainWorker) {
                WeightedTree<Object> callSite = new WeightedTree<>(edge.getType());
                callSite.addToWeight(edge.getDuration());
                fTree.addChild(callSite);
                return;
            }

            // Otherwise, add a first level call that corresponds to the worker
            WeightedTree<Object> callSite = new WeightedTree<>(String.valueOf(worker));
            callSite.addToWeight(edge.getDuration());

            // Then, add a second level for the edge type if it is not running
            if (!edge.getType().equals(EdgeType.RUNNING)) {
                WeightedTree<Object> childType = new WeightedTree<>(edge.getType());
                childType.addToWeight(edge.getDuration());
                callSite.addChild(childType);
            }
            fTree.addChild(callSite);
        }

        private void addEdgeToProcessElement(TmfEdge edge) {
            // Get the worker to which to attribute this edge, whether vertical
            // or horizontal
            IGraphWorker worker = fGraph.getParentOf(edge.getVertexTo());
            if (worker == null) {
                return;
            }

            // If it's another worker that is running, add a other process running state
            if (worker != fMainWorker && edge.getType().equals(EdgeType.RUNNING)) {
                WeightedTree<Object> callSite = new WeightedTree<>(((OsWorker) worker).getName());
                callSite.addToWeight(edge.getDuration());
                fProcessTree.addChild(callSite);
                return;
            }

            // Otherwise, add a first level call that corresponds to the worker
            WeightedTree<Object> callSite = new WeightedTree<>(edge.getType());
            callSite.addToWeight(edge.getDuration());
            fProcessTree.addChild(callSite);
        }

    }

    /**
     * @param graph
     */
    public CriticalPathToCallGraph(@Nullable TmfGraph graph) {
        if (graph == null) {
            throw new NullPointerException("Bad graph"); //$NON-NLS-1$
        }
        TmfVertex head = graph.getHead();
        if (head == null) {
            throw new NullPointerException("Empty graph"); //$NON-NLS-1$
        }

        IGraphWorker worker = graph.getParentOf(head);
        if (worker == null) {
            throw new NullPointerException("head vertex has no parent"); //$NON-NLS-1$
        }
        fElements = ImmutableList.of(String.valueOf(worker), String.valueOf(worker) + ALL_SUFFIX, String.valueOf(worker) + PROCESS_SUFFIX);
        fTree = new WeightedTree<>(String.valueOf(worker));
        fAggregatedTree = new WeightedTree<>(String.valueOf(worker) + ALL_SUFFIX);
        fProcessTree = new WeightedTree<>(String.valueOf(worker) + PROCESS_SUFFIX);
        graphToCallGraphConverter converter = new graphToCallGraphConverter(worker, graph);
        graph.scanLineTraverse(worker, converter);
    }

    @Override
    public Collection<WeightedTree<Object>> getTrees(String elements, ITmfTimestamp fromNanos, ITmfTimestamp fromNanos2) {
        return ImmutableList.of(fTree, fAggregatedTree, fProcessTree);
    }

    @Override
    public Collection<WeightedTree<Object>> getTreesFor(String element) {
        if (element.endsWith(ALL_SUFFIX)) {
            return fAggregatedTree.getChildren();
        }
        if (element.endsWith(PROCESS_SUFFIX)) {
            return fProcessTree.getChildren();
        }
        return fTree.getChildren();
    }

    @Override
    public Collection<String> getElements() {
        return fElements;
    }

    @Override
    public String getTitle() {
        return ""; //$NON-NLS-1$
    }

}
