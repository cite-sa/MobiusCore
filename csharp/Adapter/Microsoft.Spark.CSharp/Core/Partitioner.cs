﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SerializationHelpers.Data;
using SerializationHelpers.Extensions;
using System;
using System.Linq.Expressions;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// An object that defines how the elements in a key-value pair RDD are partitioned by key.
    /// Maps each key to a partition ID, from 0 to "numPartitions - 1".
    /// </summary>
    [Serializable]
    public class Partitioner
    {
        private readonly int numPartitions;
        //private readonly Func<dynamic, int> partitionFunc;
        private readonly LinqExpressionData expressionData;
        /// <summary>
        /// Create a <seealso cref="Partitioner"/> instance.
        /// </summary>
        /// <param name="numPartitions">Number of partitions.</param>
        /// <param name="partitionFunc">Defines how the elements in a key-value pair RDD are partitioned by key. Input of Func is key, output is partition index.
        /// Warning: diffrent Func instances are considered as different partitions which will cause repartition.</param>
        public Partitioner(int numPartitions, Expression<Func<dynamic, int>> partitionFunc)
        {
            this.numPartitions = numPartitions;
            this.expressionData = partitionFunc?.ToExpressionData();
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <returns>
        /// true if the specified object  is equal to the current object; otherwise, false.
        /// </returns>
        /// <param name="obj">The object to compare with the current object. </param>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            var otherPartitioner = obj as Partitioner;
            if (otherPartitioner != null && otherPartitioner.expressionData != null && otherPartitioner.expressionData.Exists())
            {
                var otherPartiotionExpression = otherPartitioner.expressionData.ToExpression<Func<dynamic, int>>();
                var thisPartitionExpression = expressionData.ToExpression<Func<dynamic, int>>();
                return otherPartitioner.numPartitions == numPartitions &&
                    (otherPartiotionExpression == thisPartitionExpression || otherPartiotionExpression.ToString() == thisPartitionExpression.ToString());


            }

            return base.Equals(obj);
        }

        /// <summary>
        /// Serves as the default hash function. 
        /// </summary>
        /// <returns>
        /// A hash code for the current object.
        /// </returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
