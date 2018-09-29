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
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.Random;

/**
 * random load balance.
 *
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 获取invokers个数
        int length = invokers.size();
        // 获取总权重
        int totalWeight = 0;
        // 每一个invokers的权重是否都一样
        boolean sameWeight = true;


        for (int i = 0; i < length; i++) {
            // 计算该invoker的权重
            int weight = getWeight(invokers.get(i), invocation);

            // 累加权重值
            totalWeight += weight;

            // 判断与前一个invoker的权重是否相同，不同则修改标志位sameWeight
            if (sameWeight && i > 0
                    && weight != getWeight(invokers.get(i - 1), invocation)) {
                sameWeight = false;
            }
        }


        if (totalWeight > 0 && !sameWeight) {
            // 至少有一个invoker不一样，并且至少一个invoker的权重大于0，则在总权重的基础上 随机 选择一个invoker

            // 从[0，totalWeight]这个区间 随机选择一个值
            int offset = random.nextInt(totalWeight);

            // 根据偏移选出一个invoker
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }

        // 如果所有invoker权重一样，则随机选择一个
        return invokers.get(random.nextInt(length));
    }

}
