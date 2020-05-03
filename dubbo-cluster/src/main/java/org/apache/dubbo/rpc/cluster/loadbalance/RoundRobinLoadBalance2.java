/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于加权的轮询算法，优化版本一
 */
public class RoundRobinLoadBalance2 extends AbstractLoadBalance {

    public static final String NAME = "roundrobin";

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    private final ConcurrentMap<String, AtomicPositiveInteger> indexSeqs = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        int length = invokers.size();
        int maxWeight = 0;
        int minWeight = Integer.MAX_VALUE;
        final List<Invoker<T>> invokerToWeightList = new ArrayList<>();

        // 查找最大和最小权重
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            maxWeight = Math.max(maxWeight, weight);
            minWeight = Math.min(minWeight, weight);
            if (weight > 0) {
                invokerToWeightList.add(invokers.get(i));
            }
        }

        // 获取当前服务对应的调用序列对象 AtomicPositiveInteger
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            // 创建 AtomicPositiveInteger，默认值为0
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }

        // 获取下标序列对象 AtomicPositiveInteger
        AtomicPositiveInteger indexSeq = indexSeqs.get(key);
        if (indexSeq == null) {
            // 创建 AtomicPositiveInteger，默认值为 -1
            indexSeqs.putIfAbsent(key, new AtomicPositiveInteger(-1));
            indexSeq = indexSeqs.get(key);
        }

        if (maxWeight > 0 && minWeight < maxWeight) {

            length = invokerToWeightList.size();

            while (true) {

                int index = indexSeq.incrementAndGet() % length;

                int currentWeight = sequence.get() % maxWeight;

                // 每循环一轮（index = 0），重新计算 currentWeight
                if (index == 0) {
                    currentWeight = sequence.incrementAndGet() % maxWeight;
                }

                // 检测 Invoker 的权重是否大于 currentWeight，大于则返回
                if (getWeight(invokerToWeightList.get(index), invocation) > currentWeight) {
                    return invokerToWeightList.get(index);
                }
            }
        }

        // 所有 Invoker 权重相等，此时进行普通的轮询即可
        return invokers.get(sequence.incrementAndGet() % length);
    }
}