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
package org.mule.transport.zmq.config;

import org.apache.commons.lang.StringUtils;
import org.mule.config.spring.MuleHierarchicalBeanDefinitionParserDelegate;
import org.mule.transport.zmq.processors.OutboundEndpointMessageProcessor;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import java.util.List;

public class OutboundEndpointDefinitionParser implements BeanDefinitionParser {

    public BeanDefinition parse(Element element, ParserContext parserContent) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(OutboundEndpointMessageProcessor.class.getName());
        String configRef = element.getAttribute("config-ref");
        if ((configRef != null) && (!StringUtils.isBlank(configRef))) {
            builder.addPropertyValue("moduleObject", configRef);
        }
        if ((element.getAttribute("payload-ref") != null) && (!StringUtils.isBlank(element.getAttribute("payload-ref")))) {
            if (element.getAttribute("payload-ref").startsWith("#")) {
                builder.addPropertyValue("payload", element.getAttribute("payload-ref"));
            } else {
                builder.addPropertyValue("payload", (("#[registry:" + element.getAttribute("payload-ref")) + "]"));
            }
        }
        if ((element.getAttribute("retryMax") != null) && (!StringUtils.isBlank(element.getAttribute("retryMax")))) {
            builder.addPropertyValue("retryMax", element.getAttribute("retryMax"));
        }
        if (element.hasAttribute("exchange-pattern")) {
            builder.addPropertyValue("exchangePattern", element.getAttribute("exchange-pattern"));
        }
        if (element.hasAttribute("socket-operation")) {
            builder.addPropertyValue("socketOperation", element.getAttribute("socket-operation"));
        }
        if ((element.getAttribute("address") != null) && (!StringUtils.isBlank(element.getAttribute("address")))) {
            builder.addPropertyValue("address", element.getAttribute("address"));
        }
        if ((element.getAttribute("filter") != null) && (!StringUtils.isBlank(element.getAttribute("filter")))) {
            builder.addPropertyValue("filter", element.getAttribute("filter"));
        }
        if ((element.getAttribute("multipart") != null) && (!StringUtils.isBlank(element.getAttribute("multipart")))) {
            builder.addPropertyValue("multipart", element.getAttribute("multipart"));
        }
        BeanDefinition definition = builder.getBeanDefinition();
        definition.setAttribute(MuleHierarchicalBeanDefinitionParserDelegate.MULE_NO_RECURSE, Boolean.TRUE);
        MutablePropertyValues propertyValues = parserContent.getContainingBeanDefinition().getPropertyValues();
        if (parserContent.getContainingBeanDefinition().getBeanClassName().equals("org.mule.config.spring.factories.PollingMessageSourceFactoryBean")) {
            propertyValues.addPropertyValue("messageProcessor", definition);
        } else {
            if (parserContent.getContainingBeanDefinition().getBeanClassName().equals("org.mule.enricher.MessageEnricher")) {
                propertyValues.addPropertyValue("enrichmentMessageProcessor", definition);
            } else {
                PropertyValue messageProcessors = propertyValues.getPropertyValue("messageProcessors");
                if ((messageProcessors == null) || (messageProcessors.getValue() == null)) {
                    propertyValues.addPropertyValue("messageProcessors", new ManagedList());
                }
                List listMessageProcessors = ((List) propertyValues.getPropertyValue("messageProcessors").getValue());
                listMessageProcessors.add(definition);
            }
        }
        return definition;
    }

}
