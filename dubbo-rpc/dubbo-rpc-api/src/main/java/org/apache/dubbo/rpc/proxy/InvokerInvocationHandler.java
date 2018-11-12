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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * dubbo使用JDK动态代理，对接口对象进行注入
 * InvokerHandler
 *
 * 程序启动的过程中，在构造函数中，赋值下一个需要调用的invoker，从而形成执行链
 */
public class InvokerInvocationHandler implements InvocationHandler {

    private final Invoker<?> invoker;

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 获取方法名称
        String methodName = method.getName();
        // 获取参数类型
        Class<?>[] parameterTypes = method.getParameterTypes();
        // 方法所处的类 是 Object类，则直接调用
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }

        /*
         * toString、hashCode、equals方法比较特殊，如果interface里面定义了这几个方法，并且进行实现，
         * 通过dubbo远程调用是不会执行这些代码实现的。
         */

        /*
         * 方法调用是toString，依次执行MockClusterInvoker、AbstractClusterInvoker的toString方法
         */
        if ("toString".equals(methodName) && parameterTypes.length == 0) {
            return invoker.toString();
        }

        /*
         * interface中含有hashCode方法，直接调用invoker的hashCode
         */
        if ("hashCode".equals(methodName) && parameterTypes.length == 0) {
            return invoker.hashCode();
        }

        /*
         * interface中含有equals方法，直接调用invoker的equals
         */
        if ("equals".equals(methodName) && parameterTypes.length == 1) {
            return invoker.equals(args[0]);
        }

        /*
         * invocationv包含了远程调用的参数、方法信息
         */
        RpcInvocation invocation;

        /*
         * todo这段代码在最新的dubbo版本中没有
         */
        if (RpcUtils.hasGeneratedFuture(method)) {
            Class<?> clazz = method.getDeclaringClass();
            String syncMethodName = methodName.substring(0, methodName.length() - Constants.ASYNC_SUFFIX.length());
            Method syncMethod = clazz.getMethod(syncMethodName, method.getParameterTypes());
            invocation = new RpcInvocation(syncMethod, args);
            invocation.setAttachment(Constants.FUTURE_GENERATED_KEY, "true");
            invocation.setAttachment(Constants.ASYNC_KEY, "true");
        } else {
            invocation = new RpcInvocation(method, args);
            if (RpcUtils.hasFutureReturnType(method)) {
                invocation.setAttachment(Constants.FUTURE_RETURNTYPE_KEY, "true");
                invocation.setAttachment(Constants.ASYNC_KEY, "true");
            }
        }

        // 继续invoker链式调用
        return invoker.invoke(invocation).recreate();
    }


}
