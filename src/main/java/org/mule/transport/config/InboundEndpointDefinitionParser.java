
package org.mule.transport.config;

import org.apache.commons.lang.StringUtils;
import org.mule.config.spring.MuleHierarchicalBeanDefinitionParserDelegate;
import org.mule.config.spring.util.SpringXMLUtils;
import org.mule.transport.sources.InboundEndpointMessageSource;
import org.mule.util.TemplateParser;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

public class InboundEndpointDefinitionParser
    implements BeanDefinitionParser
{

    /**
     * Mule Pattern Info
     * 
     */
    private TemplateParser.PatternInfo patternInfo;

    public InboundEndpointDefinitionParser() {
        patternInfo = TemplateParser.createMuleStyleParser().getStyle();
    }

    public BeanDefinition parse(Element element, ParserContext parserContent) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(InboundEndpointMessageSource.class.getName());
        String configRef = element.getAttribute("config-ref");
        if ((configRef!= null)&&(!StringUtils.isBlank(configRef))) {
            builder.addPropertyValue("moduleObject", configRef);
        }
        if (element.hasAttribute("exchangePattern")) {
            builder.addPropertyValue("exchangePattern", element.getAttribute("exchangePattern"));
        }
        if (element.hasAttribute("socketOperation")) {
            builder.addPropertyValue("socketOperation", element.getAttribute("socketOperation"));
        }
        if ((element.getAttribute("address")!= null)&&(!StringUtils.isBlank(element.getAttribute("address")))) {
            builder.addPropertyValue("address", element.getAttribute("address"));
        }
        if ((element.getAttribute("filter")!= null)&&(!StringUtils.isBlank(element.getAttribute("filter")))) {
            builder.addPropertyValue("filter", element.getAttribute("filter"));
        }
        if ((element.getAttribute("retryMax")!= null)&&(!StringUtils.isBlank(element.getAttribute("retryMax")))) {
            builder.addPropertyValue("retryMax", element.getAttribute("retryMax"));
        }

        BeanDefinition definition = builder.getBeanDefinition();
        definition.setAttribute(MuleHierarchicalBeanDefinitionParserDelegate.MULE_NO_RECURSE, Boolean.TRUE);
        MutablePropertyValues propertyValues = parserContent.getContainingBeanDefinition().getPropertyValues();
        propertyValues.addPropertyValue("messageSource", definition);
        return definition;
    }

    protected String getAttributeValue(Element element, String attributeName) {
        if (!StringUtils.isEmpty(element.getAttribute(attributeName))) {
            return element.getAttribute(attributeName);
        }
        return null;
    }

    private String generateChildBeanName(Element element) {
        String id = SpringXMLUtils.getNameOrId(element);
        if (StringUtils.isBlank(id)) {
            String parentId = SpringXMLUtils.getNameOrId(((Element) element.getParentNode()));
            return ((("."+ parentId)+":")+ element.getLocalName());
        } else {
            return id;
        }
    }

}
