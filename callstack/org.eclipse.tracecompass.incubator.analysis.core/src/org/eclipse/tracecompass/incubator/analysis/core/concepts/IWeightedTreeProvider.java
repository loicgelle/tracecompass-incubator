/*******************************************************************************
 * Copyright (c) 2018 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.incubator.analysis.core.concepts;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.tracecompass.tmf.core.timestamp.ITmfTimestamp;

/**
 * An interface that classes and analyses providing weighted trees can
 * implement. This interface allows to add extra information about the specific
 * trees that it provides.
 *
 * The trees are associated with elements that are used to group them. The
 * elements can implement the {@link ITree} class if there is a hierarchy in the
 * groupings.
 *
 * @author Geneviève Bastien
 *
 * @param <N>
 *            The type of objects represented by each node in the tree
 * @param <E>
 *            The type of elements used to group the trees
 * @param <T>
 *            The type of the tree provided
 */
public interface IWeightedTreeProvider<@NonNull N, E, @NonNull T extends WeightedTree<N>> {

    /**
     * The type of data that a value represents. Mostly for numeric value, as
     * the data type will help decide how to format the data to be displayed to
     * the user
     */
    public enum DataType {
        /**
         * Data represent a decimal number
         */
        NUMBER,
        /**
         * Data represent a time in nanoseconds, can be negative
         */
        NANOSECONDS,
        /**
         * Data represent a binary size, in bytes
         */
        BYTES,
        /**
         * Data represent a binary speed, in bytes/second
         */
        BINARY_SPEED,
        /**
         * Any other type of data
         */
        OTHER
    }

    /**
     * This class associate a title to a data type for tree metrics
     */
    public static class MetricType {
        private String fTitle;
        private DataType fDataType;

        /**
         * Constructor
         *
         * @param title
         *            The title of this metric (a string meant for end users)
         * @param dataType
         *            The type of data this metric represent
         */
        public MetricType(String title, DataType dataType) {
            fTitle = title;
            fDataType = dataType;
        }

        /**
         * Get the title of this metric, for the user
         *
         * @return The title
         */
        public String getTitle() {
            return fTitle;
        }

        /**
         * Get the type of data of this metric
         *
         * @return The data type of the metric
         */
        public DataType getDataType() {
            return fDataType;
        }

    }

    /**
     * The default metric type for the tree's weight
     */
    MetricType WEIGHT_TYPE = new MetricType("Weight", DataType.NUMBER); //$NON-NLS-1$

    /**
     * Get the trees provided by this analysis. This should return all the trees
     * for the whole trace
     *
     * @param element
     *            The element for which to get the trees
     * @return A collection of trees provided by this class
     */
    Collection<T> getTreesFor(E element);

    /**
     * Get the trees for a certain time range. If a provider does not support
     * ranged trees, this method can just return an empty collection
     *
     * @param element
     *            The element for which to get the trees
     * @param start
     *            The timestamp of the start of the range
     * @param end
     *            The timestamp of the end of the range
     * @return A collection of trees whose values span from start to end
     */
    default Collection<T> getTrees(E element, ITmfTimestamp start, ITmfTimestamp end) {
        return Collections.emptySet();
    }

    /**
     * Get the elements under which are the trees. It can be a single constant
     * element if this provider does not have the concept of grouping the trees.
     *
     * @return The elements used to group the trees
     */
    Collection<E> getElements();

    /**
     * Get the metric type for the weight value. The default metric is called
     * "Weight" and is a number
     *
     * @return The metric type for the weight value.
     */
    default MetricType getWeightType() {
        return WEIGHT_TYPE;
    }

    /**
     * Get a list of additional metrics that are provided by this tree.
     *
     * @return A list of metrics provided by the trees, in addition to the
     *         weight
     */
    default List<MetricType> getAdditionalMetrics() {
        return Collections.emptyList();
    }

    /**
     * Get an additional metric for a tree. The metric index corresponds to the
     * position of the desired metric in the list of metric returned by the
     * {@link #getAdditionalMetrics()} method and the return value should be of
     * the proper data type
     *
     * @param object
     *            The tree object for which to get the metric
     * @param metricIndex
     *            The index in the list of the metric metric to get
     * @return The value of the metric for the tree in parameter
     */
    default Object getAdditionalMetric(T object, int metricIndex) {
        throw new UnsupportedOperationException("If the tree provider has metric, it should implement this method, or it should not be called"); //$NON-NLS-1$
    }

    /**
     * Get a user-facing text to identify a tree object. By default, it is the
     * string representation of the object.
     *
     * @param tree
     *            The tree whose value to display
     * @return A user-facing string to identify this node
     */
    default String toDisplayString(T tree) {
        return String.valueOf(tree.getObject());
    }

    /**
     * A title for this tree provider. This title will be visible by users and
     * should describe what this tree provider's data represent.
     *
     * @return The title of this provider
     */
    String getTitle();

}
