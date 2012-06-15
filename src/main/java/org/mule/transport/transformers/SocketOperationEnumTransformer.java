package org.mule.transport.transformers;

import org.mule.api.transformer.DiscoverableTransformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractTransformer;
import org.mule.transformer.types.DataTypeFactory;

public class SocketOperationEnumTransformer extends AbstractTransformer implements DiscoverableTransformer {

    private int weighting = DiscoverableTransformer.DEFAULT_PRIORITY_WEIGHTING;

    public SocketOperationEnumTransformer() {
        registerSourceType(DataTypeFactory.create(String.class));
        setReturnClass(org.mule.transport.ZeroMQTransport.SocketOperation.class);
        setName("SocketOperationEnumTransformer");
    }

    protected Object doTransform(Object src, String encoding) throws TransformerException {
        org.mule.transport.ZeroMQTransport.SocketOperation result = null;
        String transformedSrc = ((String) src).toUpperCase().replace("-", "_");
        result = Enum.valueOf(org.mule.transport.ZeroMQTransport.SocketOperation.class, transformedSrc);
        return result;
    }

    public int getPriorityWeighting() {
        return weighting;
    }

    public void setPriorityWeighting(int weighting) {
        this.weighting = weighting;
    }

}
