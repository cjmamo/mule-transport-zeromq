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
package org.mule.transport.zmq.transformers;

import org.mule.api.transformer.DiscoverableTransformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractTransformer;
import org.mule.transformer.types.DataTypeFactory;
import org.mule.transport.zmq.ZMQTransport;

public class SocketOperationEnumTransformer extends AbstractTransformer implements DiscoverableTransformer {

    private int weighting = DiscoverableTransformer.DEFAULT_PRIORITY_WEIGHTING;

    public SocketOperationEnumTransformer() {
        registerSourceType(DataTypeFactory.create(String.class));
        setReturnClass(ZMQTransport.SocketOperation.class);
        setName("SocketOperationEnumTransformer");
    }

    protected Object doTransform(Object src, String encoding) throws TransformerException {
        ZMQTransport.SocketOperation result = null;
        String transformedSrc = ((String) src).toUpperCase().replace("-", "_");
        result = Enum.valueOf(ZMQTransport.SocketOperation.class, transformedSrc);
        return result;
    }

    public int getPriorityWeighting() {
        return weighting;
    }

    public void setPriorityWeighting(int weighting) {
        this.weighting = weighting;
    }

}
