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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.support.ProviderConsumerRegTable;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.dubbo.common.Constants.ACCEPT_FOREIGN_IP;
import static org.apache.dubbo.common.Constants.INTERFACES;
import static org.apache.dubbo.common.Constants.QOS_ENABLE;
import static org.apache.dubbo.common.Constants.QOS_PORT;
import static org.apache.dubbo.common.Constants.VALIDATION_KEY;

/**
 * RegistryProtocol
 *
 */
public class RegistryProtocol implements Protocol {

    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);
    private static RegistryProtocol INSTANCE;
    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();
    // 为了解决RMI重复暴露端口冲突的问题，已经暴露的服务不再暴露.
    //providerurl <--> exporter
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();
    private Cluster cluster;
    private Protocol protocol;
    private RegistryFactory registryFactory;
    private ProxyFactory proxyFactory;

    public RegistryProtocol() {
        INSTANCE = this;
    }

    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }

    //Filter the parameters that do not need to be output in url(Starting with .)
    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[]{};
        }
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return 9090;
    }

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    public void register(URL registryUrl, URL registedProviderUrl) {
        /*
         * 获取注册中心实例
         * 依次经过：
         * AbstractRegistryFactory -> ZookeeperRegistryFactory -> ZookeeperRegistry
         *
         * ZookeeperRegistry 里面含有zkClient实例
         */
        Registry registry = registryFactory.getRegistry(registryUrl);

        // 注册服务
        registry.register(registedProviderUrl);
    }

    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {

        // // 导出服务（将invoker转成exporter）
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        // 获取注册中心 URL，以 zookeeper 注册中心为例，得到的示例 URL 如下：
        // zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?application=demo-provider&dubbo=2.0.2&export=dubbo%3A%2F%2F172.17.48.52%3A20880%2Fcom.alibaba.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddemo-provider
        URL registryUrl = getRegistryUrl(originInvoker);

        /*
         * 根据 URL 加载 Registry 实现类，比如 ZookeeperRegistry
         * 并且连接到注册中心
         * 此时提供者作为消费者 引用 注册中心的核心服务 RegistryService
         */
        final Registry registry = getRegistry(originInvoker);

        // 获取已注册的服务提供者 URL，比如：
        // dubbo://172.17.48.52:20880/com.alibaba.dubbo.demo.DemoService?anyhost=true&application=demo-provider&dubbo=2.0.2&generic=false&interface=com.alibaba.dubbo.demo.DemoService&methods=sayHello
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);

        // 获取 register 参数
        boolean register = registeredProviderUrl.getParameter("register", true);

        // 向服务提供者与消费者注册表中注册服务提供者
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);

        // 根据 register 的值决定是否注册服务
        if (register) {
            /*
             *  向注册中心注册服务：
             *  调用远端注册中心的register方法进行服务注册
             *  若有消费者订阅此服务，则推送消息让消费者引用此服务。
             *  注册中心缓存了所有提供者注册的服务以供消费者发现。
             */
            register(registryUrl, registeredProviderUrl);
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }

        // 获取订阅 URL，比如：
        // provider://172.17.48.52:20880/com.alibaba.dubbo.demo.DemoService?category=configurators&check=false&anyhost=true&application=demo-provider&dubbo=2.0.2&generic=false&interface=com.alibaba.dubbo.demo.DemoService&methods=sayHello
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);

        // 创建监听器
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);

        /*
         * 向注册中心进行订阅 overrideSubscribeUrl 数据（即订阅刚刚暴露的这个服务）
         * 提供者向注册中心订阅所有注册服务的覆盖配置
         * 当注册中心有此服务的覆盖配置注册进来时，推送消息给提供者，重新暴露服务，这由管理页面完成。
         */
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);

        /*  创建并返回 DestroyableExporter：
         *  保证每次export都返回一个新的exporter实例
         *  返回暴露后的Exporter给上层ServiceConfig进行缓存，便于后期撤销暴露。
         */
        return new DestroyableExporter<T>(exporter, originInvoker, overrideSubscribeUrl, registeredProviderUrl);
    }

    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {

        // 缓存的key，避免重复暴露
        String key = getCacheKey(originInvoker);

        // 查看缓存中是否存在，存在则说明已经暴露过了
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);

        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {  // 防止并发问题

                    // 得到一个Invoker代理，里面包含原来的Invoker
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));

                    // 调用代码中的protocol.export方法，会根据协议名选择调用具体的实现类
                    // 这里我们会调用 DubboProtocol 的export方法
                    // 导出完之后，返回一个新的ExporterChangeableWrapper实例
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);

                    // 写缓存
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    /**
     * Reexport the invoker of the modified url
     *
     * @param originInvoker
     * @param newInvokerUrl
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
        } else {
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * 根据invoker的地址获取registry实例
     *
     * @param originInvoker
     * @return
     */
    private Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        /*
         * 根据SPI机制获取具体的Registry实例，
         *   会调用到com.alibaba.dubbo.registry.support.AbstractRegistryFactory#getRegistry
         * 这里获取到的是ZookeeperRegistry
         */
        return registryFactory.getRegistry(registryUrl);
    }

    private URL getRegistryUrl(Invoker<?> originInvoker) {
        //获取invoker中的registryUrl
        URL registryUrl = originInvoker.getUrl();
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            //获取registry的值，这里获得是zookeeper，默认值是dubbo
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);

            /*
             * registryUrl的结果如下：
             * zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?
             * application=springProviderApplication....
             */
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        return registryUrl;
    }


    /**
     * Return the url that is registered to the registry and filter the url parameter once
     *
     * @param originInvoker
     * @return
     */
    private URL getRegisteredProviderUrl(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        //The address you see at the registry
        return providerUrl.removeParameters(getFilteredKeys(providerUrl))
                .removeParameter(Constants.MONITOR_KEY)
                .removeParameter(Constants.BIND_IP_KEY)
                .removeParameter(Constants.BIND_PORT_KEY)
                .removeParameter(QOS_ENABLE)
                .removeParameter(QOS_PORT)
                .removeParameter(ACCEPT_FOREIGN_IP)
                .removeParameter(VALIDATION_KEY)
                .removeParameter(INTERFACES);
    }

    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL)
                .addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY,
                        Constants.CHECK_KEY, String.valueOf(false));
    }

    /**
     * Get the address of the providerUrl through the url of the invoker
     *
     * @param origininvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> origininvoker) {
        String export = origininvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }

        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * Get the key cached in bounds by invoker
     *
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        // 取 registry 参数值，并将其设置为协议头
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);

        // 获取注册中心实例
        Registry registry = registryFactory.getRegistry(url);

        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // 将 url 查询字符串转为 Map
        // group="a,b" or group="*"
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));

        // 获取 group 配置
        String group = qs.get(Constants.GROUP_KEY);

        if (group != null && group.length() > 0) {
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                // 通过 SPI 加载 MergeableCluster 实例，并调用 doRefer 继续执行服务引用逻辑
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }

        // 调用 doRefer 继续执行服务引用逻辑
        return doRefer(cluster, registry, type, url);
    }

    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // 创建 RegistryDirectory 实例
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        // 设置注册中心和协议
        directory.setRegistry(registry);
        directory.setProtocol(protocol);

        // all attributes of REFER_KEY
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());

        // 生成服务消费者链接
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);

        // 注册服务消费者，在 consumers 目录下生成新节点
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            registry.register(subscribeUrl.addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY,
                    Constants.CHECK_KEY, String.valueOf(false)));
        }

        // 订阅 providers、configurators、routers 等节点数据
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY,
                Constants.PROVIDERS_CATEGORY
                        + "," + Constants.CONFIGURATORS_CATEGORY
                        + "," + Constants.ROUTERS_CATEGORY));

        // 一个注册中心可能有多个服务提供者，因此这里需要将多个服务提供者合并为一个
        Invoker invoker = cluster.join(directory);

        ProviderConsumerRegTable.registerConsumer(invoker, url, subscribeUrl, directory);

        return invoker;
    }

    @Override
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    public static class InvokerDelegete<T> extends InvokerWrapper<T> {
        private final Invoker<T> invoker;

        /**
         * @param invoker
         * @param url     invoker.getUrl return this value
         */
        public InvokerDelegete(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegete) {
                return ((InvokerDelegete<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    /**
     * Reexport: the exporter destroy problem in protocol
     * 1.Ensure that the exporter returned by registryprotocol can be normal destroyed
     * 2.No need to re-register to the registry after notify
     * 3.The invoker passed by the export method , would better to be the invoker of exporter
     */
    private class OverrideListener implements NotifyListener {

        private final URL subscribeUrl;
        private final Invoker originInvoker;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * @param urls The list of registered information , is always not empty, The meaning is the same as the return value of {@link org.apache.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            logger.debug("original override urls: " + urls);
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            List<Configurator> configurators = RegistryDirectory.toConfigurators(matchedUrls);

            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            //The origin invoker
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            //The current, may have been merged many times
            URL currentUrl = exporter.getInvoker().getUrl();
            //Merged with this configuration
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            if (!currentUrl.equals(newUrl)) {
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getParameter(Constants.CATEGORY_KEY) == null
                        && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        //Merge the urls of configurators
        private URL getConfigedInvokerUrl(List<Configurator> configurators, URL url) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
            return url;
        }
    }

    /**
     * exporter proxy, establish the corresponding relationship between the returned exporter and the exporter exported by the protocol, and can modify the relationship at the time of override.
     *
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private final Invoker<T> originInvoker;
        private Exporter<T> exporter;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }

    static private class DestroyableExporter<T> implements Exporter<T> {

        public static final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        private Exporter<T> exporter;
        private Invoker<T> originInvoker;
        private URL subscribeUrl;
        private URL registerUrl;

        public DestroyableExporter(Exporter<T> exporter, Invoker<T> originInvoker, URL subscribeUrl, URL registerUrl) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
            this.subscribeUrl = subscribeUrl;
            this.registerUrl = registerUrl;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            Registry registry = RegistryProtocol.INSTANCE.getRegistry(originInvoker);
            try {
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            try {
                NotifyListener listener = RegistryProtocol.INSTANCE.overrideListeners.remove(subscribeUrl);
                registry.unsubscribe(subscribeUrl, listener);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int timeout = ConfigUtils.getServerShutdownTimeout();
                        if (timeout > 0) {
                            logger.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. Usually, this is called when you use dubbo API");
                            Thread.sleep(timeout);
                        }
                        exporter.unexport();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            });
        }
    }
}
