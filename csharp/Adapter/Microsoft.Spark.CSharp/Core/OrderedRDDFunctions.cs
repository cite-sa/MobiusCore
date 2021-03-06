﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using SerializationHelpers.Data;
using SerializationHelpers.Extensions;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Extra functions available on RDDs of (key, value) pairs where the key is sortable through
    /// a function to sort the key.
    /// </summary>
    public static class OrderedRDDFunctions
    {

        /// <summary>
        /// Sorts this RDD, which is assumed to consist of Tuple pairs.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="ascending"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static RDD<Tuple<K, V>> SortByKey<K, V>(this RDD<Tuple<K, V>> self,
            bool ascending = true, int? numPartitions = null)
        {
            return SortByKey<K, V, K>(self, ascending, numPartitions, key => new DefaultSortKeyFuncHelper<K>().Execute(key));
        }
        /// <summary>
        /// Sorts this RDD, which is assumed to consist of Tuples. If Item1 is type of string, case is sensitive.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="self"></param>
        /// <param name="ascending"></param>
        /// <param name="numPartitions">Number of partitions. Each partition of the sorted RDD contains a sorted range of the elements.</param>
        /// <param name="keyFunc">RDD will sort by keyFunc(key) for every Item1 in Tuple. Must not be null.</param>
        /// <returns></returns>
        public static RDD<Tuple<K, V>> SortByKey<K, V, U>(this RDD<Tuple<K, V>> self,
            bool ascending, int? numPartitions, Expression<Func<K, U>> keyFunc)
        {
            if (keyFunc == null)
            {
                throw new ArgumentNullException("keyFunc cannot be null.");
            }

            if (numPartitions == null)
            {
                numPartitions = self.GetDefaultPartitionNum();
            }

            if (numPartitions == 1)
            {
                if (self.GetNumPartitions() > 1)
                {
                    self = self.Coalesce(1);
                }
                return self.MapPartitionsWithIndex((sortByKeyX, sortByKeyY) => new SortByKeyHelper<K, V, U>(keyFunc, ascending).Execute(sortByKeyX, sortByKeyY), true);
            }

            var rddSize = self.Count();
            if (rddSize == 0) return self; // empty RDD

            var maxSampleSize = numPartitions.Value * 20; // constant from Spark's RangePartitioner
            double fraction = Math.Min((double)maxSampleSize / Math.Max(rddSize, 1), 1.0);

            /* first compute the boundary of each part via sampling: we want to partition
             * the key-space into bins such that the bins have roughly the same
             * number of (key, value) pairs falling into them */
            U[] samples = self.Sample(false, fraction, 1).Map(kv => kv.Item1).Collect().Select(k => keyFunc.Compile()(k)).ToArray();
            Array.Sort(samples, StringComparer.Ordinal); // case sensitive if key type is string

            List<U> bounds = new List<U>();
            for (int i = 0; i < numPartitions - 1; i++)
            {
                bounds.Add(samples[(int)(samples.Length * (i + 1) / numPartitions)]);
            }

            return self.PartitionBy(numPartitions.Value, (partionDynamicX) =>
                 new PairRDDFunctions.PartitionFuncDynamicTypeHelper<K>(
                     (rangePartionsX) => new RangePartitionerHelper<K, U>(numPartitions.Value, keyFunc, bounds, ascending).Execute(rangePartionsX))
                     .Execute((object)partionDynamicX))
                        .MapPartitionsWithIndex((sortX, sortY) => new SortByKeyHelper<K, V, U>(keyFunc, ascending).Execute(sortX, sortY), true);
        }

        /// <summary>
        /// Repartition the RDD according to the given partitioner and, within each resulting partition,
        /// sort records by their keys.
        ///
        /// This is more efficient than calling `repartition` and then sorting within each partition
        /// because it can push the sorting down into the shuffle machinery.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="numPartitions"></param>
        /// <param name="partitionFunc"></param>
        /// <param name="ascending"></param>
        /// <returns></returns>
        public static RDD<Tuple<K, V>> repartitionAndSortWithinPartitions<K, V>(
            this RDD<Tuple<K, V>> self,
            int? numPartitions = null,
            Func<K, int> partitionFunc = null,
            bool ascending = true)
        {
            return self.MapPartitionsWithIndex<Tuple<K, V>>((pid, iter) => ascending ? iter.OrderBy(kv => kv.Item1) : iter.OrderByDescending(kv => kv.Item1));
        }

        [Serializable]
        internal class SortByKeyHelper<K, V, U>
        {
            [DataMember]
            private readonly LinqExpressionData expressionData;
            //private readonly Func<K, U> func;
            [DataMember]
            private readonly bool ascending;
            public SortByKeyHelper(Expression<Func<K, U>> f, bool ascending = true)
            {
                expressionData = f.ToExpressionData();
                this.ascending = ascending;
            }

            public IEnumerable<Tuple<K, V>> Execute(int pid, IEnumerable<Tuple<K, V>> kvs)
            {
                IEnumerable<Tuple<K, V>> ordered;
                var func = this.expressionData.ToFunc<Func<K, U>>();
                if (ascending)
                {
                    if (typeof(K) == typeof(string))
                        ordered = kvs.OrderBy(k => func(k.Item1).ToString(), StringComparer.Ordinal);
                    else
                        ordered = kvs.OrderBy(k => func(k.Item1));
                }
                else
                {
                    if (typeof(K) == typeof(string))
                        ordered = kvs.OrderByDescending(k => func(k.Item1).ToString(), StringComparer.Ordinal);
                    else
                        ordered = kvs.OrderByDescending(k => func(k.Item1));
                }
                return ordered;
            }
        }

        [Serializable]
        internal class DefaultSortKeyFuncHelper<K>
        {
            public K Execute(K key) { return key; }
        }

        [Serializable]
        internal class RangePartitionerHelper<K, U>
        {
            [DataMember]
            private readonly LinqExpressionData expressionData;
            [DataMember]
            private readonly int numPartitions;
            //private readonly Func<K, U> keyFunc;
            [DataMember]
            private readonly List<U> bounds;
            [DataMember]
            private readonly bool ascending;
            public RangePartitionerHelper(int numPartitions, Expression<Func<K, U>> keyFunc, List<U> bounds, bool ascending)
            {
                this.numPartitions = numPartitions;
                this.bounds = bounds;
                this.expressionData = keyFunc.ToExpressionData();
                this.ascending = ascending;
            }

            public int Execute(K key)
            {
                // Binary search the insert position in the bounds. If key found, return the insert position; if not, a negative
                // number that is the bitwise complement of insert position is returned, so bitwise inversing it.
                var keyFunc = this.expressionData.ToFunc<Func<K, U>>();
                var pos = bounds.BinarySearch(keyFunc(key));
                if (pos < 0) pos = ~pos;

                return ascending ? pos : numPartitions - 1 - pos;
            }
        }
    }
}
