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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RegistryDirectory 是一种动态服务目录，实现了 NotifyListener 接口。
 * 当注册中心服务配置发生变化后，RegistryDirectory 可收到与当前服务相关的变化。
 * 收到变更通知后，RegistryDirectory 可根据配置变更信息刷新 Invoker 列表。
 *
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();
    private final String serviceKey; // Initialization at construction time, assertion not null
    private final Class<T> serviceType; // Initialization at construction time, assertion not null
    private final Map<String, String> queryMap; // Initialization at construction time, assertion not null
    private final URL directoryUrl; // Initialization at construction time, assertion not null, and always assign non null value
    private final String[] serviceMethods;
    // 判断是否多个组
    private final boolean multiGroup;
    private Protocol protocol; // Initialization at the time of injection, the assertion is not null
    private Registry registry; // Initialization at the time of injection, the assertion is not null
    private volatile boolean forbidden = false;

    private volatile URL overrideDirectoryUrl; // Initialization at construction time, assertion not null, and always assign non null value

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    private volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<url, Invoker> cache service url to invoker mapping.
    private volatile Map<String, Invoker<T>> urlInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<methodName, Invoker> cache service method to invokers mapping.
    // 缓存服务的方法
    private volatile Map<String, List<Invoker<T>>> methodInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Set<invokerUrls> cache invokeUrls to invokers mapping.
    private volatile Set<URL> cachedInvokerUrls; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null)
            throw new IllegalArgumentException("service type is null.");
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0)
            throw new IllegalArgumentException("registry serviceKey is null.");
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        this.overrideDirectoryUrl = this.directoryUrl = url.setPath(url.getServiceInterface()).clearParameters().addParameters(queryMap).removeParameter(Constants.MONITOR_KEY);
        String group = directoryUrl.getParameter(Constants.GROUP_KEY, "");
        this.multiGroup = group != null && ("*".equals(group) || group.contains(","));
        String methods = queryMap.get(Constants.METHODS_KEY);
        this.serviceMethods = methods == null ? null : Constants.COMMA_SPLIT_PATTERN.split(methods);
    }

    /**
     * Convert override urls to map for use when re-refer.
     * Send all rules every time, the urls will be reassembled and calculated
     *
     * @param urls Contract:
     *             </br>1.override://0.0.0.0/...( or override://ip:port...?anyhost=true)&para1=value1... means global rules (all of the providers take effect)
     *             </br>2.override://ip:port...?anyhost=false Special rules (only for a certain provider)
     *             </br>3.override:// rule is not supported... ,needs to be calculated by registry itself.
     *             </br>4.override://0.0.0.0/ without parameters means clearing the override
     * @return
     */
    public static List<Configurator> toConfigurators(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Collections.emptyList();
        }

        List<Configurator> configurators = new ArrayList<Configurator>(urls.size());
        for (URL url : urls) {
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }
            Map<String, String> override = new HashMap<String, String>(url.getParameters());
            //The anyhost parameter of override may be added automatically, it can't change the judgement of changing url
            override.remove(Constants.ANYHOST_KEY);
            if (override.size() == 0) {
                configurators.clear();
                continue;
            }
            configurators.add(configuratorFactory.getConfigurator(url));
        }
        Collections.sort(configurators);
        return configurators;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    @Override
    public synchronized void notify(List<URL> urls) {

        // 声明invoker的URL引用数组
        List<URL> invokerUrls = new ArrayList<URL>();

        // 声明router的URL引用数组
        List<URL> routerUrls = new ArrayList<URL>();

        // 声明configurator（配置器）的URL引用数组
        List<URL> configuratorUrls = new ArrayList<URL>();

        for (URL url : urls) {

            // 获取协议名称
            String protocol = url.getProtocol();

            // 获取 category 参数
            String category = url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);

            // 根据 category 参数将 url 分别放到不同的列表中
            if (Constants.ROUTERS_CATEGORY.equals(category)
                    || Constants.ROUTE_PROTOCOL.equals(protocol)) {
                // 添加路由器 url
                routerUrls.add(url);
            } else if (Constants.CONFIGURATORS_CATEGORY.equals(category)
                    || Constants.OVERRIDE_PROTOCOL.equals(protocol)) {
                // 添加配置器 url
                configuratorUrls.add(url);
            } else if (Constants.PROVIDERS_CATEGORY.equals(category)) {
                // 添加服务提供者 url
                invokerUrls.add(url);
            } else {
                logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
            }
        }

        if (configuratorUrls != null && !configuratorUrls.isEmpty()) {
            // 将 url 转成 Configurator
            this.configurators = toConfigurators(configuratorUrls);
        }

        if (routerUrls != null && !routerUrls.isEmpty()) {
            // 将 url 转成 Router
            List<Router> routers = toRouters(routerUrls);
            if (routers != null) {
                setRouters(routers);
            }
        }

        List<Configurator> localConfigurators = this.configurators;

        // 合并override参数
        this.overrideDirectoryUrl = directoryUrl;
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                // 配置 overrideDirectoryUrl
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }

        // 刷新 Invoker 列表
        refreshInvoker(invokerUrls);
    }

    /**
     * 把invokerUrl列表 转化成 invoker Map。转换规则如下
     * 1.如果URL已经转换为invoker，则不再重新引用它，并且直接从缓存中获得它，并且注意URL中的任何参数更改都将被重新引用。
     * 2.如果传入的invoker列表不是空的，则意味着它是最新的调用列表。
     * 3.如果传入的invokerUrl列表为空，则意味着该规则仅是重写规则或路由规则，需要对其进行重新对比以决定是否重新引用。
     * @param invokerUrls this parameter can't be null
     */
    private void refreshInvoker(List<URL> invokerUrls) {

        // invokerUrls 仅有一个元素，且 url 协议头为 empty，此时表示禁用所有服务
        if (invokerUrls != null && invokerUrls.size() == 1 && invokerUrls.get(0) != null
                && Constants.EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {

            // 设置禁止访问
            this.forbidden = true;
            // 设置methodInvokerMap为null
            this.methodInvokerMap = null;
            // 销毁所有 Invoker
            destroyAllInvokers();

        } else {
            // 设置允许访问
            this.forbidden = false;

            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap;

            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                // 添加缓存 url 到 invokerUrls 中
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                // 缓存 invokerUrls
                this.cachedInvokerUrls = new HashSet<URL>();
                this.cachedInvokerUrls.addAll(invokerUrls);
            }
            if (invokerUrls.isEmpty()) {
                return;
            }

            // 将 url 转成 Invoker
            // key为url，value为invoker的Map
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);

            // 将 newUrlInvokerMap 转成方法名到 Invoker 列表的映射
            // key为method，value为invoker的Map
            Map<String, List<Invoker<T>>> newMethodInvokerMap = toMethodInvokers(newUrlInvokerMap);

            // 转换出错，直接打印异常，并返回
            if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls.toString()));
                return;
            }

            // 合并多个组的 Invoker
            this.methodInvokerMap =
                    multiGroup ? toMergeMethodInvokerMap(newMethodInvokerMap) : newMethodInvokerMap;

            this.urlInvokerMap = newUrlInvokerMap;

            try {
                // 销毁无用 Invoker
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    private Map<String, List<Invoker<T>>> toMergeMethodInvokerMap(Map<String, List<Invoker<T>>> methodMap) {

        Map<String, List<Invoker<T>>> result = new HashMap<String, List<Invoker<T>>>();

        // 遍历入参
        for (Map.Entry<String, List<Invoker<T>>> entry : methodMap.entrySet()) {

            // 方法名
            String method = entry.getKey();
            // 该方法对应的invoker列表
            List<Invoker<T>> invokers = entry.getValue();
            // group -> Invoker 列表
            Map<String, List<Invoker<T>>> groupMap = new HashMap<String, List<Invoker<T>>>();

            // 遍历 Invoker 列表
            for (Invoker<T> invoker : invokers) {
                // 获取分组配置
                String group = invoker.getUrl().getParameter(Constants.GROUP_KEY, "");

                List<Invoker<T>> groupInvokers = groupMap.get(group);
                if (groupInvokers == null) {
                    groupInvokers = new ArrayList<Invoker<T>>();
                    // 缓存 <group, List<Invoker>> 到 groupMap 中
                    groupMap.put(group, groupInvokers);
                }

                // 存储 invoker 到 groupInvokers
                groupInvokers.add(invoker);
            }

            // 如果 groupMap 中仅包含一组键值对，此时直接取出该键值对的值即可
            if (groupMap.size() == 1) {
                result.put(method, groupMap.values().iterator().next());

            } else if (groupMap.size() > 1) {
            // groupMap.size() > 1 成立，表示 groupMap 中包含多组键值对，比如：
            // {
            //     "group1": [invoker1, invoker2, invoker3, ...],
            //     "group2": [invoker4, invoker5, invoker6, ...]
            // }

                List<Invoker<T>> groupInvokers = new ArrayList<Invoker<T>>();
                for (List<Invoker<T>> groupList : groupMap.values()) {
                    // 通过集群类合并每个分组对应的 Invoker 列表
                    groupInvokers.add(cluster.join(new StaticDirectory<T>(groupList)));
                }
                // 缓存结果
                result.put(method, groupInvokers);
            } else {
                result.put(method, invokers);
            }
        }

        return result;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private List<Router> toRouters(List<URL> urls) {
        List<Router> routers = new ArrayList<Router>();
        if (urls == null || urls.isEmpty()) {
            return routers;
        }
        if (urls != null && !urls.isEmpty()) {
            for (URL url : urls) {
                if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                    continue;
                }
                String routerType = url.getParameter(Constants.ROUTER_KEY);
                if (routerType != null && routerType.length() > 0) {
                    url = url.setProtocol(routerType);
                }
                try {
                    Router router = routerFactory.getRouter(url);
                    if (!routers.contains(router))
                        routers.add(router);
                } catch (Throwable t) {
                    logger.error("convert router url to router error, url: " + url, t);
                }
            }
        }
        return routers;
    }

    /**
     * Turn urls into invokers, and if url has been refer, will not re-reference.
     *
     * @param urls
     * @return invokers
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<String, Invoker<T>>();
        if (urls == null || urls.isEmpty()) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<String>();

        // 获取服务消费端配置的协议
        String queryProtocols = this.queryMap.get(Constants.PROTOCOL_KEY);

        for (URL providerUrl : urls) {
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;

                String[] acceptProtocols = queryProtocols.split(",");

                // 检测服务提供者协议是否被服务消费者所支持
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }

                // 若服务提供者协议不被消费者所支持，则忽略当前 providerUrl
                if (!accept) {
                    continue;
                }
            }

            // 忽略 empty 协议
            if (Constants.EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
            }

            // 通过 SPI 检测服务提供者端协议是否被消费端支持，不支持则抛出异常
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() + " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost()
                        + ", supported protocol: " + ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }

            // 合并 url
            URL url = mergeUrl(providerUrl);

            String key = url.toFullString();

            // 忽略重复 url
            if (keys.contains(key)) {
                continue;
            }

            keys.add(key);

            // 将本地 Invoker 缓存赋值给 localUrlInvokerMap
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap;

            // 获取与 url 对应的 Invoker
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);

            // 缓存未命中
            if (invoker == null) {
                try {
                    boolean enabled = true;
                    if (url.hasParameter(Constants.DISABLED_KEY)) {
                        // 获取 disable 配置，取反，然后赋值给 enable 变量
                        enabled = !url.getParameter(Constants.DISABLED_KEY, false);
                    } else {
                        // 获取 enable 配置，并赋值给 enable 变量
                        enabled = url.getParameter(Constants.ENABLED_KEY, true);
                    }
                    if (enabled) {
                        // 调用 refer 获取 Invoker
                        invoker = new InvokerDelegate<T>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }

                // 缓存 Invoker 实例
                if (invoker != null) {
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
            // 缓存命中
                // 将 invoker 存储到 newUrlInvokerMap 中
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * Merge url parameters. the order is: override > -D >Consumer > Provider
     *
     * @param providerUrl
     * @return
     */
    private URL mergeUrl(URL providerUrl) {
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters

        List<Configurator> localConfigurators = this.configurators; // local reference
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }

        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // Do not check whether the connection is successful or not, always create Invoker!

        // The combination of directoryUrl and override is at the end of notify, which can't be handled here
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters()); // Merge the provider side parameters

        if ((providerUrl.getPath() == null || providerUrl.getPath().length() == 0)
                && "dubbo".equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getParameter(Constants.INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    private List<Invoker<T>> route(List<Invoker<T>> invokers, String method) {
        Invocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
        List<Router> routers = getRouters();
        if (routers != null) {
            for (Router router : routers) {
                if (router.getUrl() != null) {
                    invokers = router.route(invokers, getConsumerUrl(), invocation);
                }
            }
        }
        return invokers;
    }

    /**
     * Transform the invokers list into a mapping relationship with a method
     *
     * @param invokersMap Invoker Map
     * @return Mapping relation between Invoker and method
     */
    private Map<String, List<Invoker<T>>> toMethodInvokers(Map<String, Invoker<T>> invokersMap) {

        // 方法名 -> Invoker 列表
        Map<String, List<Invoker<T>>> newMethodInvokerMap = new HashMap<String, List<Invoker<T>>>();

        List<Invoker<T>> invokersList = new ArrayList<Invoker<T>>();

        if (invokersMap != null && invokersMap.size() > 0) {

            for (Invoker<T> invoker : invokersMap.values()) {

                // 获取 methods 参数
                String parameter = invoker.getUrl().getParameter(Constants.METHODS_KEY);

                if (parameter != null && parameter.length() > 0) {

                    // 切分 methods 参数值，得到方法名数组
                    String[] methods = Constants.COMMA_SPLIT_PATTERN.split(parameter);

                    if (methods != null && methods.length > 0) {
                        for (String method : methods) {

                            // 方法名不为 *
                            if (method != null && method.length() > 0
                                    && !Constants.ANY_VALUE.equals(method)) {

                                // 根据方法名获取 Invoker 列表
                                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                                if (methodInvokers == null) {
                                    methodInvokers = new ArrayList<Invoker<T>>();
                                    newMethodInvokerMap.put(method, methodInvokers);
                                }
                                // 存储 Invoker 到列表中
                                methodInvokers.add(invoker);
                            }
                        }
                    }
                }
                invokersList.add(invoker);
            }
        }

        // 进行服务级别路由，参考 pull request #749
        List<Invoker<T>> newInvokersList = route(invokersList, null);

        // 存储 <*, newInvokersList> 映射关系
        newMethodInvokerMap.put(Constants.ANY_VALUE, newInvokersList);

        if (serviceMethods != null && serviceMethods.length > 0) {
            for (String method : serviceMethods) {
                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                if (methodInvokers == null || methodInvokers.isEmpty()) {
                    methodInvokers = newInvokersList;
                }
                // 进行方法级别路由
                newMethodInvokerMap.put(method, route(methodInvokers, method));
            }
        }

        // 排序，转成不可变列表
        for (String method : new HashSet<String>(newMethodInvokerMap.keySet())) {
            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
            Collections.sort(methodInvokers, InvokerComparator.getComparator());
            newMethodInvokerMap.put(method, Collections.unmodifiableList(methodInvokers));
        }
        return Collections.unmodifiableMap(newMethodInvokerMap);
    }

    /**
     * Close all invokers
     */
    private void destroyAllInvokers() {
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        methodInvokerMap = null;
    }

    /**
     * Check whether the invoker in the cache needs to be destroyed
     * If set attribute of url: refer.autodestroy=false, the invokers will only increase without decreasing,there may be a refer leak
     *
     * @param oldUrlInvokerMap
     * @param newUrlInvokerMap
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {

        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }

        List<String> deleted = null;

        if (oldUrlInvokerMap != null) {

            // 获取新生成的 Invoker 列表
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();

            // 遍历老的 <url, Invoker> 映射表
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {

                // 检测 newInvokers 中是否包含老的 Invoker
                if (!newInvokers.contains(entry.getValue())) {
                    if (deleted == null) {
                        deleted = new ArrayList<String>();
                    }
                    // 若不包含，则将老的 Invoker 对应的 url 存入 deleted 列表中
                    deleted.add(entry.getKey());
                }
            }
        }

        if (deleted != null) {
            // 遍历 deleted 集合，并到老的 <url, Invoker> 映射关系表查出 Invoker，销毁之
            for (String url : deleted) {
                if (url != null) {
                    // 从 oldUrlInvokerMap 中移除 url 对应的 Invoker
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            // 销毁 Invoker
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] faild. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            // 服务提供者关闭或禁用了服务，此时抛出 No provider 异常
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                "No provider available from registry " + getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +  NetUtils.getLocalHost()
                        + " use dubbo version " + Version.getVersion() + ", please check status of providers(disabled, not registered or in blacklist).");
        }

        List<Invoker<T>> invokers = null;

        // 获取 Invoker 本地缓存
        Map<String, List<Invoker<T>>> localMethodInvokerMap = this.methodInvokerMap;

        if (localMethodInvokerMap != null && localMethodInvokerMap.size() > 0) {
            // 获取方法名称
            String methodName = RpcUtils.getMethodName(invocation);
            // 获取方法参数列表
            Object[] args = RpcUtils.getArguments(invocation);

            // 检测参数列表的第一个参数是否为 String 或 enum 类型
            if (args != null && args.length > 0 && args[0] != null
                    && (args[0] instanceof String || args[0].getClass().isEnum())) {
                // 通过 方法名 + 第一个参数名称 查询 Invoker 列表，具体的使用场景暂时没想到
                invokers = localMethodInvokerMap.get(methodName + "." + args[0]);
            }

            if (invokers == null) {
                // 通过方法名获取 Invoker 列表
                invokers = localMethodInvokerMap.get(methodName);
            }

            if (invokers == null) {
                // 通过星号 * 获取 Invoker 列表
                invokers = localMethodInvokerMap.get(Constants.ANY_VALUE);
            }

            if (invokers == null) {
                // 仍然为null，使用迭代器获取一个invoker
                Iterator<List<Invoker<T>>> iterator = localMethodInvokerMap.values().iterator();
                if (iterator.hasNext()) {
                    invokers = iterator.next();
                }
            }
        }

        // 返回 Invoker 列表
        return invokers == null ? new ArrayList<Invoker<T>>(0) : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, List<Invoker<T>>> getMethodInvokerMap() {
        return methodInvokerMap;
    }

    private static class InvokerComparator implements Comparator<Invoker<?>> {

        private static final InvokerComparator comparator = new InvokerComparator();

        private InvokerComparator() {
        }

        public static InvokerComparator getComparator() {
            return comparator;
        }

        @Override
        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }

    }

    /**
     * The delegate class, which is mainly used to store the URL address sent by the registry,and can be reassembled on the basis of providerURL queryMap overrideMap for re-refer.
     *
     * @param <T>
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }
}
