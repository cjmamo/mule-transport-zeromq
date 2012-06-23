Mule ZeroMQ Transport - User Guide
================================

# Usage

## Bean Property Expression Enricher

Enriches a payload Java object property.

### Example

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/3.2/mule.xsd">

    <flow name="Enrich">
        ...
        <enricher target="#[bean-property:apple.washed]">
            <vm:outbound-endpoint path="status" exchange-pattern="request-response"/>
        </enricher>
        ...
    </flow>
    ...
</mule>
```

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
    <td rowspan="1" class="confluenceTd">receiver-threading-profile</td><td style="text-align: center" class="confluenceTd">element</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd">The default receiver threading profile set on the Mule Configuration</td><td class="confluenceTd">
      <p>
          The threading properties to use when receiving events from the connector.
        </p>
    </td>
  </tr>
  <tr>
    <td rowspan="1" class="confluenceTd">connection-pooling-profile</td><td style="text-align: center" class="confluenceTd">element</td><td style="text-align: center" class="confluenceTd">no</td><td style="text-align: center" class="confluenceTd">5672</td><td class="confluenceTd">
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
      The criteria used to filter out messages. Applies only when the exchange-pattern attribute is set to subscribe. If not set, all messages are consumed.
    </p>
    </td>
  </tr>
</table>


Examples
--------

### Subscribing to a socket

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

### Receiving message from multiple sources

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

## Exception Message Processor Chain

Invokes the next message processor in the flow. If any of the subsequent messages processors throws an exception, the
Exception Message Processor Chain picks up the exception, creates an exception message (similar behaviour to the
classical exception handler) and then applies the chain of nested processors which are configured on it.

### Example

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:ricston="http://www.ricston.com/schema/mule/ricston"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/3.2/mule.xsd
        http://www.ricston.com/schema/mule/ricston http://www.ricston.com/schema/mule/ricston/3.2/mule-ricston.xsd">

    <flow name="ExceptionMessageProcessorChain">
        ...
        <ricston:exception-message-processor-chain>
           <expression-transformer evaluator="groovy" expression="return 'Sad Path'"/>
        </ricston:exception-message-processor-chain>
        ...
    </flow>
    ...
</mule>
```

## Transaction Aware Object Store

An object store which can join an XA transaction. This could be used, for example, to make the idempotent message filter
participate in transactions.

### Example

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      ...
      xsi:schemaLocation="
        ...
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/3.2/mule.xsd">

    <jbossts:transaction-manager/>

    <flow name="TransactionAwareObjectStore">
        <vm:inbound-endpoint path="in" exchange-pattern="one-way">
            <xa-transaction action="BEGIN_OR_JOIN"/>
        </vm:inbound-endpoint>
        <idempotent-message-filter>
            <custom-object-store class="org.mule.module.ricston.objectstore.TransactionAwareObjectStore"/>
        </idempotent-message-filter>
        <test:component appendString=" Received"/>
        <vm:outbound-endpoint path="out" exchange-pattern="one-way">
            <xa-transaction action="ALWAYS_JOIN"/>
        </vm:outbound-endpoint>
    </flow>
    ...
</mule>
```