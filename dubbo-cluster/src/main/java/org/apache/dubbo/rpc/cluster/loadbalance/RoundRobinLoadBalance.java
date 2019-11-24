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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 轮循 负载均衡算法
 * Round robin load balance.
 *
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "roundrobin";

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        // 获取接口名称
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();

        // 获取invoker的个数
        int length = invokers.size();

        // 记录最大权重
        int maxWeight = 0;
        // 记录最小的权重
        int minWeight = Integer.MAX_VALUE;

        // invoker作为key，权重为value
        final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Invoker<T>, IntegerWrapper>();

        // 权重总和
        int weightSum = 0;

        for (int i = 0; i < length; i++) {

            // 获取invoker的权重
            int weight = getWeight(invokers.get(i), invocation);

            // 获取最大的权重
            maxWeight = Math.max(maxWeight, weight);
            // 获取最小的权重
            minWeight = Math.min(minWeight, weight);

            if (weight > 0) {
                invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
                weightSum += weight;
            }
        }

        // 获取一个自动增长的序号
        AtomicPositiveInteger sequence = sequences.get(key);

        if (sequence == null) {
            // 没有则新建
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }

        // 有请求过来，则sequence增加1
        int currentSequence = sequence.getAndIncrement();

        // 如果最大权重大于0，最小权重小于最大权重（权重不相等）
        if (maxWeight > 0 && minWeight < maxWeight) {

            // currentSequence与总权重取模的结果
            int mod = currentSequence % weightSum;

            for (int i = 0; i < maxWeight; i++) {

                for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {

                    final Invoker<T> k = each.getKey();

                    final IntegerWrapper v = each.getValue();

                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }

                    if (v.getValue() > 0) {
                        // 权重值减1
                        v.decrement();
                        // 减少mod值
                        mod--;
                    }
                }
            }
        }

        // 取模，获取一个invoker
        return invokers.get(currentSequence % length);
    }

    private static final class IntegerWrapper {
        private int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }

}
