/*
 * Copyright 2012 Claude Mamo
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.mule.transport.zmq.adapters;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import org.mule.api.MuleContext;
import org.mule.api.config.MuleProperties;
import org.mule.api.config.ThreadingProfile;
import org.mule.api.context.MuleContextAware;
import org.mule.api.devkit.capability.Capabilities;
import org.mule.api.devkit.capability.ModuleCapability;
import org.mule.api.lifecycle.Disposable;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.api.retry.RetryPolicyTemplate;
import org.mule.config.PoolingProfile;
import org.mule.devkit.dynamic.api.helper.ConnectionManager;
import org.mule.transport.zmq.ZMQTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZeroMQTransportConnectionManager
        implements Capabilities, ConnectionManager<ZeroMQTransportConnectionManager.ConnectionKey,
        ZMQTransportLifecycleAdapter>, MuleContextAware, Initialisable {


    private ZMQTransport.ExchangePattern exchangePattern;
    private ZMQTransport.SocketOperation socketOperation;
    private String address;
    private String filter;
    private Boolean multipart;
    private static Logger logger = LoggerFactory.getLogger(ZeroMQTransportConnectionManager.class);
    private MuleContext muleContext;
    private GenericKeyedObjectPool connectionPool;
    protected PoolingProfile connectionPoolingProfile;
    private ThreadingProfile receiverThreadingProfile;
    private String name;
    private Integer ioThreads;

    @Override
    public ConnectionKey getDefaultConnectionKey() {

        /*
        ToDo not sure if there's a better way to determine whether its inbound or not then this for the connection key
         */
        if (receiverThreadingProfile != null) {
            return new ConnectionKey(exchangePattern, socketOperation, address, filter, true, multipart, name);
        } else {
            return new ConnectionKey(exchangePattern, socketOperation, address, filter, false, multipart, name);

        }
    }

    @Override
    public RetryPolicyTemplate getRetryPolicyTemplate() {
        return muleContext.getRegistry().lookupObject(MuleProperties.OBJECT_DEFAULT_RETRY_POLICY_TEMPLATE);
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public void setIoThreads(int ioThreads) {
        this.ioThreads = ioThreads;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setReceiverThreadingProfile(ThreadingProfile receiverThreadingProfile) {
        this.receiverThreadingProfile = receiverThreadingProfile;
    }

    public ThreadingProfile getReceiverThreadingProfile() {
        return receiverThreadingProfile;
    }

    public void setConnectionPoolingProfile(PoolingProfile value) {
        this.connectionPoolingProfile = value;
    }

    public PoolingProfile getConnectionPoolingProfile() {
        return this.connectionPoolingProfile;
    }

    public void setSocketOperation(ZMQTransport.SocketOperation value) {
        this.socketOperation = value;
    }

    public ZMQTransport.SocketOperation getSocketOperation() {
        return this.socketOperation;
    }

    public void setAddress(String value) {
        this.address = value;
    }

    public String getAddress() {
        return this.address;
    }

    public void setExchangePattern(ZMQTransport.ExchangePattern value) {
        this.exchangePattern = value;
    }

    public ZMQTransport.ExchangePattern getExchangePattern() {
        return this.exchangePattern;
    }

    public void setFilter(String value) {
        this.filter = value;
    }

    public String getFilter() {
        return this.filter;
    }

    public Boolean getMultipart() {
        return multipart;
    }

    public void setMultipart(Boolean multipart) {
        this.multipart = multipart;
    }

    public void setMuleContext(MuleContext context) {
        this.muleContext = context;
    }

    public void initialise() {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        if (connectionPoolingProfile != null) {
            config.maxIdle = connectionPoolingProfile.getMaxIdle();
            config.maxActive = connectionPoolingProfile.getMaxActive();
            config.maxWait = connectionPoolingProfile.getMaxWait();
            config.whenExhaustedAction = ((byte) connectionPoolingProfile.getExhaustedAction());
        }
        connectionPool = new GenericKeyedObjectPool(new ConnectionFactory(this), config);
    }

    public ZMQTransportLifecycleAdapter acquireConnection(ConnectionKey key)
            throws Exception {
        return ((ZMQTransportLifecycleAdapter) connectionPool.borrowObject(key));
    }

    public void releaseConnection(ConnectionKey key, ZMQTransportLifecycleAdapter connection)
            throws Exception {
        connectionPool.returnObject(key, connection);
    }

    public void destroyConnection(ConnectionKey key, ZMQTransportLifecycleAdapter connection)
            throws Exception {
        connectionPool.invalidateObject(key, connection);
    }

    @Override
    public boolean isCapableOf(ModuleCapability capability) {
        if (capability == ModuleCapability.LIFECYCLE_CAPABLE) {
            return true;
        }
        if (capability == ModuleCapability.CONNECTION_MANAGEMENT_CAPABLE) {
            return true;
        }
        return false;
    }

    private class ConnectionFactory
            implements KeyedPoolableObjectFactory {

        private ZeroMQTransportConnectionManager connectionManager;

        public ConnectionFactory(ZeroMQTransportConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
        }

        public Object makeObject(Object key)
                throws Exception {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            ZMQTransportLifecycleAdapter connector = new ZMQTransportLifecycleAdapter();

            connector.setMuleContext(connectionManager.muleContext);
            connector.setReceiverThreadingProfile(receiverThreadingProfile);
            connector.setIoThreads(ioThreads);
            connector.initialise();
            connector.start();

            return connector;
        }

        public void destroyObject(Object key, Object obj)
                throws Exception {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            if (!(obj instanceof ZMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }

            if (((ZMQTransportLifecycleAdapter) obj) instanceof Stoppable) {
                ((ZMQTransportLifecycleAdapter) obj).stop();
            }
            if (((ZMQTransportLifecycleAdapter) obj) instanceof Disposable) {
                ((ZMQTransportLifecycleAdapter) obj).dispose();
            }

        }

        public boolean validateObject(Object key, Object obj) {
            if (!(obj instanceof ZMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }
            try {
                return ((ZMQTransportLifecycleAdapter) obj).isConnected();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }

        public void activateObject(Object key, Object obj)
                throws Exception {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            if (!(obj instanceof ZMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }
            try {
                if (!((ZMQTransportLifecycleAdapter) obj).isConnected()) {
                    ((ZMQTransportLifecycleAdapter) obj).connect(((ConnectionKey) key).getExchangePattern(), ((ConnectionKey) key).getSocketOperation(), ((ConnectionKey) key).getAddress(), ((ConnectionKey) key).getFilter(), ((ConnectionKey) key).isInbound(), ((ConnectionKey) key).isMultipart(), ((ConnectionKey) key).getIdentity());
                }
            } catch (Exception e) {
                throw e;
            }
        }

        public void passivateObject(Object key, Object obj)
                throws Exception {
        }

    }

    public static class ConnectionKey {

        private ZMQTransport.ExchangePattern exchangePattern;
        private ZMQTransport.SocketOperation socketOperation;
        private String address;
        private String filter;
        private String identity;
        private boolean isInbound;
        private boolean multipart;

        public ConnectionKey(ZMQTransport.ExchangePattern exchangePattern, ZMQTransport.SocketOperation socketOperation, String address, String filter, boolean isInbound, boolean multipart, String identity) {
            this.exchangePattern = exchangePattern;
            this.socketOperation = socketOperation;
            this.address = address;
            this.filter = filter;
            this.isInbound = isInbound;
            this.multipart = multipart;
            this.identity = identity;
        }

        public String getIdentity() {
            return identity;
        }

        public void setIdentity(String identity) {
            this.identity = identity;
        }

        public boolean isMultipart() {
            return multipart;
        }

        public void setMultipart(boolean multipart) {
            this.multipart = multipart;
        }

        public void setSocketOperation(ZMQTransport.SocketOperation value) {
            this.socketOperation = value;
        }

        public ZMQTransport.SocketOperation getSocketOperation() {
            return this.socketOperation;
        }

        public boolean isInbound() {
            return isInbound;
        }

        public void setInbound(boolean isInbound) {
            this.isInbound = isInbound;
        }

        public void setAddress(String value) {
            this.address = value;
        }

        public String getAddress() {
            return this.address;
        }

        public void setExchangePattern(ZMQTransport.ExchangePattern value) {
            this.exchangePattern = value;
        }

        public ZMQTransport.ExchangePattern getExchangePattern() {
            return this.exchangePattern;
        }

        public void setFilter(String value) {
            this.filter = value;
        }

        public String getFilter() {
            return this.filter;
        }

        public int hashCode() {
            int hash = 1;
            hash = ((hash * 31) + this.socketOperation.hashCode());
            hash = ((hash * 31) + this.address.hashCode());
            hash = ((hash * 31) + this.exchangePattern.hashCode());
            if (this.filter != null) {
                hash = ((hash * 31) + this.filter.hashCode());
            }

            return hash;
        }

        public boolean equals(Object obj) {
            if (this.filter != null) {
                return (((obj instanceof ConnectionKey) && (this.socketOperation == ((ConnectionKey) obj).socketOperation)) && (this.address == ((ConnectionKey) obj).address) && (this.exchangePattern == ((ConnectionKey) obj).exchangePattern) && (this.filter == ((ConnectionKey) obj).filter));
            } else {
                return (((obj instanceof ConnectionKey) && (this.socketOperation == ((ConnectionKey) obj).socketOperation)) && (this.address == ((ConnectionKey) obj).address) && (this.exchangePattern == ((ConnectionKey) obj).exchangePattern));
            }
        }

    }

}
