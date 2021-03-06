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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * FailoverClusterInvoker 在调用失败时，会自动切换 Invoker 进行重试。
 * 默认配置下，Dubbo 会使用这个类作为缺省 Cluster Invoker。
 * 重试将导致时延
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {

        List<Invoker<T>> copyinvokers = invokers;

        // 参数校验
        checkInvokers(copyinvokers, invocation);

        // 获取方法名称
        String methodName = RpcUtils.getMethodName(invocation);

        // 获取重试次数
        int len = getUrl().getMethodParameter(methodName, Constants.RETRIES_KEY, Constants.DEFAULT_RETRIES) + 1;
        if (len <= 0) {
            // 最少要调用1次
            len = 1;
        }

        // 局部引用
        RpcException le = null;
        // 已经调用过的invoker列表
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyinvokers.size());
        // 调用失败的invoker地址
        Set<String> providers = new HashSet<String>(len);

        // i < len 作为循环条件，说明len是多少就循环多少次（len等于 重试次数 + 1）
        for (int i = 0; i < len; i++) {
            if (i > 0) {

                // 检查invoker是否被销毁
                checkWhetherDestroyed();

                // 在进行重试前重新列举 Invoker，这样做的好处是，如果某个服务挂了，
                // 通过调用 list 可得到最新可用的 Invoker 列表
                copyinvokers = list(invocation);

                // 对 copyinvokers 进行判空检查
                checkInvokers(copyinvokers, invocation);
            }

            /*
             * 通过负载均衡选择 Invoker
             * 因为上述步骤可能筛选出invoker数量大于1，所以再次经过loadBalance的筛选（同时避免获取到已经调用过的invoker）
             */
            Invoker<T> invoker = select(loadbalance, invocation, copyinvokers, invoked);

            // 添加到 invoker 到 invoked 列表中
            invoked.add(invoker);

            // 设置 invoked 到 RPC 上下文中
            RpcContext.getContext().setInvokers((List) invoked);

            try {
                // 调用目标 Invoker 的 invoke 方法
                // 即远程方法调用
                Result result = invoker.invoke(invocation);
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + methodName
                            + " in the service " + getInterface().getName()
                            + " was successful by the provider " + invoker.getUrl().getAddress()
                            + ", but there have been failed providers " + providers
                            + " (" + providers.size() + "/" + copyinvokers.size()
                            + ") from the registry " + directory.getUrl().getAddress()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                            + le.getMessage(), le);
                }

                // 正常执行，直接返回结果。否则，如果还有重试次数，则继续重试
                return result;
            } catch (RpcException e) {
                if (e.isBiz()) { // biz exception.
                    throw e;
                }
                le = e;
            } catch (Throwable e) {
                le = new RpcException(e.getMessage(), e);
            } finally {
                providers.add(invoker.getUrl().getAddress());
            }
        }

        // 若重试失败，则抛出异常
        throw new RpcException(le != null ? le.getCode() : 0, "Failed to invoke the method "
                + methodName + " in the service " + getInterface().getName()
                + ". Tried " + len + " times of the providers " + providers
                + " (" + providers.size() + "/" + copyinvokers.size()
                + ") from the registry " + directory.getUrl().getAddress()
                + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                + Version.getVersion() + ". Last error is: "
                + (le != null ? le.getMessage() : ""), le != null && le.getCause() != null ? le.getCause() : le);
    }

}
