
package org.mule.transport.adapters;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.mule.api.Capabilities;
import org.mule.api.Capability;
import org.mule.api.ConnectionManager;
import org.mule.api.MuleContext;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.context.MuleContextAware;
import org.mule.api.lifecycle.Disposable;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.lifecycle.Startable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.config.PoolingProfile;
import org.mule.transport.ZeroMQTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@code ZeroMQTransportConnectionManager} is a wrapper around {@link org.mule.transport.ZeroMQTransport } that adds connection management capabilities to the pojo.
 *
 */
public class ZeroMQTransportConnectionManager
    implements Capabilities, ConnectionManager<ZeroMQTransportConnectionManager.ConnectionKey, ZeroMQTransportLifecycleAdapter> , MuleContextAware, Initialisable
{

    /**
     *
     */
    private ZeroMQTransport.ExchangePattern exchangePattern;
    /**
     *
     */
    private ZeroMQTransport.SocketOperation socketOperation;
    /**
     *
     */
    private String address;
    /**
     *
     */
    private String filter;
    private static Logger logger = LoggerFactory.getLogger(ZeroMQTransportConnectionManager.class);
    /**
     * Mule Context
     *
     */
    private MuleContext muleContext;
    /**
     * Flow construct
     *
     */
    private FlowConstruct flowConstruct;
    /**
     * Connector Pool
     *
     */
    private GenericKeyedObjectPool connectionPool;
    protected PoolingProfile connectionPoolingProfile;

    /**
     * Sets connectionPoolingProfile
     *
     * @param value Value to set
     */
    public void setConnectionPoolingProfile(PoolingProfile value) {
        this.connectionPoolingProfile = value;
    }

    /**
     * Retrieves connectionPoolingProfile
     *
     */
    public PoolingProfile getConnectionPoolingProfile() {
        return this.connectionPoolingProfile;
    }

    /**
     * Sets socketOperation
     *
     * @param value Value to set
     */
    public void setSocketOperation(ZeroMQTransport.SocketOperation value) {
        this.socketOperation = value;
    }

    /**
     * Retrieves socketOperation
     *
     */
    public ZeroMQTransport.SocketOperation getSocketOperation() {
        return this.socketOperation;
    }

    /**
     * Sets address
     *
     * @param value Value to set
     */
    public void setAddress(String value) {
        this.address = value;
    }

    /**
     * Retrieves address
     *
     */
    public String getAddress() {
        return this.address;
    }

    /**
     * Sets exchangePattern
     *
     * @param value Value to set
     */
    public void setExchangePattern(ZeroMQTransport.ExchangePattern value) {
        this.exchangePattern = value;
    }

    /**
     * Retrieves exchangePattern
     *
     */
    public ZeroMQTransport.ExchangePattern getExchangePattern() {
        return this.exchangePattern;
    }

    /**
     * Sets filter
     *
     * @param value Value to set
     */
    public void setFilter(String value) {
        this.filter = value;
    }

    /**
     * Retrieves filter
     *
     */
    public String getFilter() {
        return this.filter;
    }

    /**
     * Sets flow construct
     *
     * @param flowConstruct Flow construct to set
     */
    public void setFlowConstruct(FlowConstruct flowConstruct) {
        this.flowConstruct = flowConstruct;
    }

    /**
     * Set the Mule context
     *
     * @param context Mule context to set
     */
    public void setMuleContext(MuleContext context) {
        this.muleContext = context;
    }

    public void initialise() {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        if (connectionPoolingProfile!= null) {
            config.maxIdle = connectionPoolingProfile.getMaxIdle();
            config.maxActive = connectionPoolingProfile.getMaxActive();
            config.maxWait = connectionPoolingProfile.getMaxWait();
            config.whenExhaustedAction = ((byte) connectionPoolingProfile.getExhaustedAction());
        }
        connectionPool = new GenericKeyedObjectPool(new ConnectionFactory(this), config);
    }

    public ZeroMQTransportLifecycleAdapter acquireConnection(ConnectionKey key)
        throws Exception
    {
        return ((ZeroMQTransportLifecycleAdapter) connectionPool.borrowObject(key));
    }

    public void releaseConnection(ConnectionKey key, ZeroMQTransportLifecycleAdapter connection)
        throws Exception
    {
        connectionPool.returnObject(key, connection);
    }

    public void destroyConnection(ConnectionKey key, ZeroMQTransportLifecycleAdapter connection)
        throws Exception
    {
        connectionPool.invalidateObject(key, connection);
    }

    /**
     * Returns true if this module implements such capability
     *
     */
    public boolean isCapableOf(Capability capability) {
        if (capability == Capability.LIFECYCLE_CAPABLE) {
            return true;
        }
        if (capability == Capability.CONNECTION_MANAGEMENT_CAPABLE) {
            return true;
        }
        return false;
    }

    private static class ConnectionFactory
        implements KeyedPoolableObjectFactory
    {

        private ZeroMQTransportConnectionManager connectionManager;

        public ConnectionFactory(ZeroMQTransportConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
        }

        public Object makeObject(Object key)
            throws Exception
        {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            ZeroMQTransportLifecycleAdapter connector = new ZeroMQTransportLifecycleAdapter();
            if (connector instanceof Initialisable) {
                connector.initialise();
            }
            if (connector instanceof Startable) {
                connector.start();
            }
            return connector;
        }

        public void destroyObject(Object key, Object obj)
            throws Exception
        {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            if (!(obj instanceof ZeroMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }
            try {
                ((ZeroMQTransportLifecycleAdapter) obj).disconnect();
            } catch (Exception e) {
                throw e;
            } finally {
                if (((ZeroMQTransportLifecycleAdapter) obj) instanceof Stoppable) {
                    ((ZeroMQTransportLifecycleAdapter) obj).stop();
                }
                if (((ZeroMQTransportLifecycleAdapter) obj) instanceof Disposable) {
                    ((ZeroMQTransportLifecycleAdapter) obj).dispose();
                }
            }
        }

        public boolean validateObject(Object key, Object obj) {
            if (!(obj instanceof ZeroMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }
            try {
                return ((ZeroMQTransportLifecycleAdapter) obj).isConnected();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }

        public void activateObject(Object key, Object obj)
            throws Exception
        {
            if (!(key instanceof ConnectionKey)) {
                throw new RuntimeException("Invalid key type");
            }
            if (!(obj instanceof ZeroMQTransportLifecycleAdapter)) {
                throw new RuntimeException("Invalid connector type");
            }
            try {
                if (!((ZeroMQTransportLifecycleAdapter) obj).isConnected()) {
                    ((ZeroMQTransportLifecycleAdapter) obj).connect(((ConnectionKey) key).getExchangePattern(), ((ConnectionKey) key).getSocketOperation(), ((ConnectionKey) key).getAddress(), ((ConnectionKey) key).getFilter(), ((ConnectionKey) key).isInbound());
                }
            } catch (Exception e) {
                throw e;
            }
        }

        public void passivateObject(Object key, Object obj)
            throws Exception
        {
        }

    }


    /**
     * A tuple of connection parameters
     *
     */
    public static class ConnectionKey {

        /**
         *
         */
        private ZeroMQTransport.ExchangePattern exchangePattern;
        /**
         *
         */
        private ZeroMQTransport.SocketOperation socketOperation;
        /**
         *
         */
        private String address;
        /**
         *
         */
        private String filter;
        /**
         *
         */
        private boolean isInbound;

        public ConnectionKey(ZeroMQTransport.ExchangePattern exchangePattern, ZeroMQTransport.SocketOperation socketOperation, String address, String filter, boolean isInbound) {
            this.exchangePattern = exchangePattern;
            this.socketOperation = socketOperation;
            this.address = address;
            this.filter = filter;
            this.setInbound(isInbound);
        }

        /**
         * Sets socketOperation
         *
         * @param value Value to set
         */
        public void setSocketOperation(ZeroMQTransport.SocketOperation value) {
            this.socketOperation = value;
        }

        /**
         * Retrieves socketOperation
         *
         */
        public ZeroMQTransport.SocketOperation getSocketOperation() {
            return this.socketOperation;
        }

        public boolean isInbound() {
            return isInbound;
        }

        public void setInbound(boolean isInbound) {
            this.isInbound = isInbound;
        }

        /**
         * Sets address
         *
         * @param value Value to set
         */
        public void setAddress(String value) {
            this.address = value;
        }

        /**
         * Retrieves address
         *
         */
        public String getAddress() {
            return this.address;
        }

        /**
         * Sets exchangePattern
         *
         * @param value Value to set
         */
        public void setExchangePattern(ZeroMQTransport.ExchangePattern value) {
            this.exchangePattern = value;
        }

        /**
         * Retrieves exchangePattern
         *
         */
        public ZeroMQTransport.ExchangePattern getExchangePattern() {
            return this.exchangePattern;
        }

        /**
         * Sets filter
         *
         * @param value Value to set
         */
        public void setFilter(String value) {
            this.filter = value;
        }

        /**
         * Retrieves filter
         *
         */
        public String getFilter() {
            return this.filter;
        }

        public int hashCode() {
            int hash = 1;
            hash = ((hash* 31)+ this.socketOperation.hashCode());
            hash = ((hash* 31)+ this.address.hashCode());
            return hash;
        }

        public boolean equals(Object obj) {
            return (((obj instanceof ConnectionKey)&&(this.socketOperation == ((ConnectionKey) obj).socketOperation))&&(this.address == ((ConnectionKey) obj).address));
        }

    }

}
