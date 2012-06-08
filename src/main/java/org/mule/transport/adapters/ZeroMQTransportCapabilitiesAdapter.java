
package org.mule.transport.adapters;

import org.mule.api.Capabilities;
import org.mule.api.Capability;
import org.mule.transport.ZeroMQTransport;


/**
 * A <code>ZeroMQTransportCapabilitiesAdapter</code> is a wrapper around {@link org.mule.transport.ZeroMQTransport } that implements {@link org.mule.api.Capabilities} interface.
 * 
 */
public class ZeroMQTransportCapabilitiesAdapter
    extends ZeroMQTransport
    implements Capabilities
{


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

}
