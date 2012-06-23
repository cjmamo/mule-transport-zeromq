Mule ZeroMQ Transport - User Guide
================================

Configuration Reference
-----------------------

### Connector Properties

<table class="confluenceTable">
  <tr>
    <th style="width:10%" class="confluenceTh">Name</th><th style="width:10%" class="confluenceTh">Type</th><th style="width:10%" class="confluenceTh">Required</th><th style="width:10%" class="confluenceTh">Default</th><th class="confluenceTh">Description</th>
  </tr>
   <tr>
      <td rowspan="1" class="confluenceTd">name</td><td style="text-align: center" class="confluenceTd">string</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
        <p>
            The connector's name. Used to reference a connector from an endpoint.
          </p>
      </td>
    </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">receiver-threading-profile</td><td style="text-align: center" class="confluenceTd">element</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd">The default receiver threading profile set on the Mule configuration</td><td class="confluenceTd">
      <p>
          The threading properties to use when receiving events from the connector.
        </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">connection-pooling-profile</td><td style="text-align: center" class="confluenceTd">element</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
      <p>
          Controls how the pool of connections should behave.
        </p>
    </td>
  </tr>
</table>

### <a href="http://www.mulesoft.org/documentation/display/MULE3USER/Configuring+a+Transport#ConfiguringaTransport-receiverthreadingprofile">Receiver Threading Profile Attributes</a>

### <a href="http://www.mulesoft.org/documentation/display/MULE3USER/Tuning+Performance#TuningPerformance-pooling">Connection Pooling Profile Attributes</a>

### Endpoint Attributes

<table class="confluenceTable">
  <tr>
    <th style="width:10%" class="confluenceTh">Name</th><th style="width:10%" class="confluenceTh">Type</th><th style="width:10%" class="confluenceTh">Required</th><th style="width:10%" class="confluenceTh">Default</th><th class="confluenceTh">Description</th>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">exchange-pattern</td><td style="text-align: center" class="confluenceTd"><b>request-response</b> / <b>one-way</b> / <b>pull</b> / <b>push</b>/ <b>subscribe</b> / <b>publish</b></td><td style="text-align: center" class="confluenceTd">yes</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
      <p>
      The messaging pattern used by the endpoint.
    </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">socket-operation</td><td style="text-align: center" class="confluenceTd"><b>bind</b> / <b>connect</b></td><td style="text-align: center" class="confluenceTd">yes</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
      <p>
      Determines whether the endpoint accepts (bind) connections or connects (connect) to a socket.
    </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">address</td><td style="text-align: center" class="confluenceTd">string</td><td style="text-align: center" class="confluenceTd">yes</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
      <p>
      The socket address the endpoint is binding/connecting to.
    </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">multipart</td><td style="text-align: center" class="confluenceTd">boolean</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd">false</td><td class="confluenceTd">
      <p>
      Whether a java.util.List payload is sent as a multi-part message. If set to true, each element in the list becomes a message part. Applies only to outbound endpoints. On inbound endpoints, multi-part messages are transformed to a java.util.List object where each element is a message part.
      </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">filter</td><td style="text-align: center" class="confluenceTd">string</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd"></td><td class="confluenceTd">
      <p>
      The criteria used to filter out messages. Applies only when the exchange-pattern attribute is set to subscribe. If not set, every received message is consumed.
    </p>
    </td>
  </tr>
</table>


Examples
--------

### Subscribing to a socket on an inbound-endpoint

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:zeromq="http://www.mulesoft.org/schema/mule/zeromq"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/zeromq http://www.mulesoft.org/schema/mule/zeromq/current/mule-zeromq.xsd">

    <flow name="SubscribeOnInboundFlow">
        <zeromq:inbound-endpoint address="tcp://localhost:8080" filter="Foo"
                                 socket-operation="connect" exchange-pattern="subscribe"/>
        ...
    </flow>
    ...
</mule>
```

### Receiving messages from multiple inbound endpoints

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:zeromq="http://www.mulesoft.org/schema/mule/zeromq"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/zeromq http://www.mulesoft.org/schema/mule/zeromq/current/mule-zeromq.xsd">

    <flow name="MultipleSourcesFlow">
        <composite-source>
            <zeromq:inbound-endpoint address="tcp://localhost:8080" socket-operation="connect"
                                     exchange-pattern="subscribe"/>
            <zeromq:inbound-endpoint address="tcp://*:8080" socket-operation="bind"
                                     exchange-pattern="pull"/>
        </composite-source>
        ...
    </flow>
    ...
</mule>
```

### Pushing messages out

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:zeromq="http://www.mulesoft.org/schema/mule/zeromq"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/zeromq http://www.mulesoft.org/schema/mule/zeromq/current/mule-zeromq.xsd">

    <flow name="PushFlow">
        ...
        <zeromq:outbound-endpoint address="tcp://*:8080" socket-operation="bind"
                                  exchange-pattern="one-way"/>
        ...
    </flow>
    ...
</mule>
```

### Sending messages as multi-part messages

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:zeromq="http://www.mulesoft.org/schema/mule/zeromq"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/zeromq http://www.mulesoft.org/schema/mule/zeromq/current/mule-zeromq.xsd">

    <flow name="MultiPartMessageOnOutboundFlow">
        ...
        <zeromq:outbound-endpoint address="tcp://*:8080" multipart="true"
                                  socket-operation="bind" exchange-pattern="request-response"/>
        ...
    </flow>
    ...
</mule>
```