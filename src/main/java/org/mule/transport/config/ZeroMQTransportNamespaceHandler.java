
package org.mule.transport.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

public class ZeroMQTransportNamespaceHandler extends NamespaceHandlerSupport
{
    public void init() {
        registerBeanDefinitionParser("config", new ZeroMQTransportConfigDefinitionParser());
        registerBeanDefinitionParser("outbound-endpoint", new OutboundEndpointDefinitionParser());
        registerBeanDefinitionParser("inbound-endpoint", new InboundEndpointDefinitionParser());
    }

}
