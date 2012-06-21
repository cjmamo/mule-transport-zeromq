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
package org.mule.transport.config;

import org.mule.api.config.MuleProperties;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.generic.MuleOrphanDefinitionParser;
import org.mule.config.spring.parsers.specific.ThreadingProfileDefinitionParser;
import org.mule.transport.adapters.ZeroMQTransportConnectionManager;

public class ZeroMQTransportNamespaceHandler extends AbstractMuleNamespaceHandler
{
    public void init() {
        registerBeanDefinitionParser("connector", new MuleOrphanDefinitionParser(ZeroMQTransportConnectionManager.class, true));
        registerBeanDefinitionParser("receiver-threading-profile", new ThreadingProfileDefinitionParser("receiverThreadingProfile", MuleProperties.OBJECT_DEFAULT_MESSAGE_RECEIVER_THREADING_PROFILE));
        registerBeanDefinitionParser("connection-pooling-profile", new PoolingProfileDefinitionParser());
        registerBeanDefinitionParser("outbound-endpoint", new OutboundEndpointDefinitionParser());
        registerBeanDefinitionParser("inbound-endpoint", new InboundEndpointDefinitionParser());
    }

}
