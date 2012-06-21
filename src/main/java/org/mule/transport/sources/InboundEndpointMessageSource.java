/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.mule.transport.sources;

import org.mule.DefaultMuleEvent;
import org.mule.DefaultMuleMessage;
import org.mule.MessageExchangePattern;
import org.mule.RequestContext;
import org.mule.api.*;
import org.mule.api.callback.SourceCallback;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.construct.FlowConstructAware;
import org.mule.api.context.MuleContextAware;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.lifecycle.Startable;
import org.mule.api.lifecycle.Stoppable;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.registry.RegistrationException;
import org.mule.api.source.MessageSource;
import org.mule.api.transformer.DataType;
import org.mule.api.transformer.Transformer;
import org.mule.config.i18n.CoreMessages;
import org.mule.config.i18n.MessageFactory;
import org.mule.session.DefaultMuleSession;
import org.mule.transformer.types.DataTypeFactory;
import org.mule.transport.ZeroMQTransport;
import org.mule.transport.adapters.ZeroMQTransportConnectionManager;
import org.mule.transport.adapters.ZeroMQTransportLifecycleAdapter;

import java.util.Map;


public class InboundEndpointMessageSource implements Runnable, SourceCallback, FlowConstructAware, MuleContextAware, Initialisable, Startable, Stoppable, MessageSource {

    private Object exchangePattern;
    private ZeroMQTransport.ExchangePattern _exchangePatternType;
    private Object socketOperation;
    private ZeroMQTransport.SocketOperation _socketOperationType;
    private Object address;
    private Object filter;
    private Object multipart;
    private ZeroMQTransport.ExchangePattern transformedExchangePattern;

    private Object moduleObject;

    private MuleContext muleContext;

    private FlowConstruct flowConstruct;

    private MessageProcessor messageProcessor;

    private Thread thread;

    public void initialise()
            throws InitialisationException {
        if (moduleObject == null) {
            try {
                moduleObject = muleContext.getRegistry().lookupObject(ZeroMQTransportConnectionManager.class);
                if (moduleObject == null) {
                    moduleObject = new ZeroMQTransportConnectionManager();
                    muleContext.getRegistry().registerObject(ZeroMQTransportConnectionManager.class.getName(), moduleObject);
                }
            } catch (RegistrationException e) {
                throw new InitialisationException(CoreMessages.initialisationFailure("org.mule.transport.adapters.ZeroMQTransportConnectionManager"), e, this);
            }
        }
        if (moduleObject instanceof String) {
            moduleObject = muleContext.getRegistry().lookupObject(((String) moduleObject));
            if (moduleObject == null) {
                throw new InitialisationException(MessageFactory.createStaticMessage("Cannot find object by config name"), this);
            }
        }
    }

    public void setMuleContext(MuleContext context) {
        this.muleContext = context;
    }

    public void setModuleObject(Object moduleObject) {
        this.moduleObject = moduleObject;
    }

    public void setListener(MessageProcessor listener) {
        this.messageProcessor = listener;
    }

    public void setFlowConstruct(FlowConstruct flowConstruct) {
        this.flowConstruct = flowConstruct;
    }

    public void setSocketOperation(Object value) {
        this.socketOperation = value;
    }

    public void setAddress(Object value) {
        this.address = value;
    }

    public void setExchangePattern(Object value) {
        this.exchangePattern = value;
    }

    public void setFilter(Object value) {
        this.filter = value;
    }

    public void setMultipart(Object value) {
        this.multipart = value;
    }

    public Object process(Object message)
            throws Exception {
        MuleMessage muleMessage;
        muleMessage = new DefaultMuleMessage(message, muleContext);
        MuleSession muleSession;
        muleSession = new DefaultMuleSession(flowConstruct, muleContext);
        MuleEvent muleEvent;
        if (transformedExchangePattern.equals(ZeroMQTransport.ExchangePattern.REQUEST_RESPONSE)) {
            muleEvent = new DefaultMuleEvent(muleMessage, MessageExchangePattern.REQUEST_RESPONSE, muleSession);
        } else {
            muleEvent = new DefaultMuleEvent(muleMessage, MessageExchangePattern.ONE_WAY, muleSession);
        }

        try {
            MuleEvent responseEvent;
            responseEvent = messageProcessor.process(muleEvent);
            if ((responseEvent != null) && (responseEvent.getMessage() != null)) {
                return responseEvent.getMessage().getPayload();
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }

    public Object process(Object message, Map<String, Object> properties) throws Exception {
        MuleMessage muleMessage;
        muleMessage = new DefaultMuleMessage(message, properties, null, null, muleContext);
        MuleSession muleSession;
        muleSession = new DefaultMuleSession(flowConstruct, muleContext);
        MuleEvent muleEvent;
        muleEvent = new DefaultMuleEvent(muleMessage, MessageExchangePattern.ONE_WAY, muleSession);
        try {
            MuleEvent responseEvent;
            responseEvent = messageProcessor.process(muleEvent);
            if ((responseEvent != null) && (responseEvent.getMessage() != null)) {
                return responseEvent.getMessage().getPayload();
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }

    public Object process()
            throws Exception {
        try {
            MuleEvent responseEvent;
            responseEvent = messageProcessor.process(RequestContext.getEvent());
            if ((responseEvent != null) && (responseEvent.getMessage() != null)) {
                return responseEvent.getMessage().getPayload();
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }

    public void start()
            throws MuleException {
        if (thread == null) {
            thread = new Thread(this, "Receiving Thread");
        }
        thread.start();
    }

    public void stop() throws MuleException {
        thread.interrupt();
    }

    public void run() {
        ZeroMQTransportConnectionManager castedModuleObject = null;
        ZeroMQTransportLifecycleAdapter connection = null;
        ZeroMQTransport.SocketOperation transformedSocketOperation = null;
        String transformedAddress = null;
        String transformedFilter = null;
        Boolean transformedMultipart = null;

        try {
            if (moduleObject instanceof String) {
                castedModuleObject = ((ZeroMQTransportConnectionManager) muleContext.getRegistry().lookupObject(((String) moduleObject)));
                if (castedModuleObject == null) {
                    throw new MessagingException(CoreMessages.failedToCreate("inboundEndpoint"), ((MuleEvent) null), new RuntimeException("Cannot find the configuration specified by the config-ref attribute."));
                }
            } else {
                castedModuleObject = ((ZeroMQTransportConnectionManager) moduleObject);
            }

            if (exchangePattern != null) {
                if (!ZeroMQTransport.ExchangePattern.class.isAssignableFrom(exchangePattern.getClass())) {
                    DataType source;
                    DataType target;
                    source = DataTypeFactory.create(exchangePattern.getClass());
                    target = DataTypeFactory.create(ZeroMQTransport.ExchangePattern.class);
                    Transformer t;
                    t = muleContext.getRegistry().lookupTransformer(source, target);
                    transformedExchangePattern = ((ZeroMQTransport.ExchangePattern) t.transform(exchangePattern));
                } else {
                    transformedExchangePattern = ((ZeroMQTransport.ExchangePattern) exchangePattern);
                }
            }

            if (socketOperation != null) {
                if (!ZeroMQTransport.SocketOperation.class.isAssignableFrom(socketOperation.getClass())) {
                    DataType source;
                    DataType target;
                    source = DataTypeFactory.create(socketOperation.getClass());
                    target = DataTypeFactory.create(ZeroMQTransport.SocketOperation.class);
                    Transformer t;
                    t = muleContext.getRegistry().lookupTransformer(source, target);
                    transformedSocketOperation = ((ZeroMQTransport.SocketOperation) t.transform(socketOperation));
                } else {
                    transformedSocketOperation = ((ZeroMQTransport.SocketOperation) socketOperation);
                }
            }

            if (address != null) {
                if (!String.class.isAssignableFrom(address.getClass())) {
                    DataType source;
                    DataType target;
                    source = DataTypeFactory.create(address.getClass());
                    target = DataTypeFactory.create(String.class);
                    Transformer t;
                    t = muleContext.getRegistry().lookupTransformer(source, target);
                    transformedAddress = ((String) t.transform(address));
                } else {
                    transformedAddress = ((String) address);
                }
            }

            if (filter != null) {
                if (!String.class.isAssignableFrom(filter.getClass())) {
                    DataType source;
                    DataType target;
                    source = DataTypeFactory.create(filter.getClass());
                    target = DataTypeFactory.create(String.class);
                    Transformer t;
                    t = muleContext.getRegistry().lookupTransformer(source, target);
                    transformedFilter = ((String) t.transform(filter));
                } else {
                    transformedFilter = ((String) filter);
                }
            }

            if (multipart != null) {
                if (!Boolean.class.isAssignableFrom(multipart.getClass())) {
                    DataType source;
                    DataType target;
                    source = DataTypeFactory.create(multipart.getClass());
                    target = DataTypeFactory.create(Boolean.class);
                    Transformer t;
                    t = muleContext.getRegistry().lookupTransformer(source, target);
                    transformedMultipart = ((Boolean) t.transform(multipart));
                } else {
                    transformedMultipart = ((Boolean) multipart);
                }
            }

            connection = castedModuleObject.acquireConnection(new ZeroMQTransportConnectionManager.ConnectionKey(transformedExchangePattern, transformedSocketOperation, transformedAddress, transformedFilter, true, transformedMultipart));

            if (connection == null) {
                throw new MessagingException(CoreMessages.failedToCreate("inboundEndpoint"), ((MuleEvent) null), new RuntimeException("Cannot create connection"));
            }
            connection.inboundEndpoint(transformedExchangePattern, transformedSocketOperation, transformedAddress, transformedFilter, transformedMultipart, this);
        } catch (MessagingException e) {
            flowConstruct.getExceptionListener().handleException(e, e.getEvent());
        } catch (Exception e) {
            muleContext.getExceptionListener().handleException(e);
        } finally {
            if (connection != null) {
                try {
                    castedModuleObject.releaseConnection(new ZeroMQTransportConnectionManager.ConnectionKey(transformedExchangePattern, transformedSocketOperation, transformedAddress, transformedFilter, false, transformedMultipart), connection);
                } catch (Exception _x) {
                }
            }
        }
    }

}
