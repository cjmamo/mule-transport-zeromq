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
package org.mule.transport.zmq.adapters;

import org.mule.api.MuleException;
import org.mule.api.lifecycle.*;
import org.mule.config.MuleManifest;
import org.mule.devkit.dynamic.api.helper.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZMQTransportLifecycleAdapter
        extends ZMQTransportCapabilitiesAdapter
        implements Disposable, Initialisable, Startable, Stoppable, Connection {


    public void start() throws MuleException {
        super.initialise();
    }

    public void stop()
            throws MuleException {
    }

    @Override
    public String getConnectionIdentifier() {
        // ToDo JD implement
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void initialise() {
        Logger log = LoggerFactory.getLogger(ZMQTransportLifecycleAdapter.class);
        String runtimeVersion = MuleManifest.getProductVersion();
        if (runtimeVersion.equals("Unknown")) {
            log.warn("Unknown Mule runtime version. This module may not work properly!");
        } else {
            String[] expectedMinVersion = "3.2".split("\\.");
            if (runtimeVersion.contains("-")) {
                runtimeVersion = runtimeVersion.split("-")[0];
            }
            String[] currentRuntimeVersion = runtimeVersion.split("\\.");
            for (int i = 0; (i < expectedMinVersion.length); i++) {
                try {
                    if (Integer.parseInt(currentRuntimeVersion[i]) < Integer.parseInt(expectedMinVersion[i])) {
                        throw new RuntimeException("This module is only valid for Mule 3.2");
                    }
                } catch (NumberFormatException nfe) {
                    log.warn("Error parsing Mule version, cannot validate current Mule version");
                }
            }
        }
    }

    public void dispose() {
        super.destroy();
    }

}
