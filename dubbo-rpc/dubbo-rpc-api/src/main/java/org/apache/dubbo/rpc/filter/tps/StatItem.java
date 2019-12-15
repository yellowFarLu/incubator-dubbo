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
package org.apache.dubbo.rpc.filter.tps;

import java.util.concurrent.atomic.AtomicInteger;

class StatItem {

    private String name;

    private long lastResetTime;

    private long interval;

    private AtomicInteger token;

    private int rate;

    StatItem(String name, int rate, long interval) {
        this.name = name;
        this.rate = rate;
        this.interval = interval;
        this.lastResetTime = System.currentTimeMillis();
        this.token = new AtomicInteger(rate);
    }

    public boolean isAllowable() {

        // 获取现在的时间
        long now = System.currentTimeMillis();

        // 当经过了interval时间间隔
        if (now > lastResetTime + interval) {

            // 重新设置token令牌的个数
            token.set(rate);

            // 从现在开始，经过interval的时间
            lastResetTime = now;
        }

        // 获取令牌的值
        int value = token.get();

        boolean flag = false;

        // 使用CAS实现乐观锁
        while (value > 0 && !flag) {

            // 能够执行请求，则令牌减一
            flag = token.compareAndSet(value, value - 1);

            value = token.get();
        }

        return flag;
    }

    long getLastResetTime() {
        return lastResetTime;
    }

    int getToken() {
        return token.get();
    }

    @Override
    public String toString() {
        return new StringBuilder(32).append("StatItem ")
                .append("[name=").append(name).append(", ")
                .append("rate = ").append(rate).append(", ")
                .append("interval = ").append(interval).append("]")
                .toString();
    }

}
