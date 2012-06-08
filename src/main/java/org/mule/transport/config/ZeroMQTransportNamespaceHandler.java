
package org.mule.transport.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;


/**
 * Registers bean definitions parsers for handling elements in <code>http://www.mulesoft.org/schema/mule/zeromq</code>.
 * 
 */
public class ZeroMQTransportNamespaceHandler
    extends NamespaceHandlerSupport
{


    /**
     * Invoked by the {@link DefaultBeanDefinitionDocumentReader} after construction but before any custom elements are parsed. 
     * @see org.springframework.beans.factory.xml.NamespaceHandlerSupport#registerBeanDefinitionParser(String, BeanDefinitionParser)
     * 
     */
    public void init() {
        registerBeanDefinitionParser("config", new ZeroMQTransportConfigDefinitionParser());
        registerBeanDefinitionParser("outbound-endpoint", new OutboundEndpointDefinitionParser());
        registerBeanDefinitionParser("inbound-endpoint", new InboundEndpointDefinitionParser());
    }

}
