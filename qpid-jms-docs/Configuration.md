# Client configuration

This file details various configuration options for the client, such as how to configure and create a JNDI InitialContext, the syntax for its related configuration, and various URI options that can be set when defining a ConnectionFactory.

## Configuring a JNDI InitialContext

Applications use a JNDI InitialContext, itself obtained from an InitialContextFactory, to look up JMS objects such as ConnectionFactory. The Qpid JMS client provides an implementation of the InitialContextFactory in class *org.apache.qpid.jms.jndi.JmsInitialContextFactory*. This may be configured and used in three main ways:

1.  Via jndi.properties file on the Java Classpath.

    By including a file named jndi.properties on the Classpath and setting the *java.naming.factory.initial* property
    to value *org.apache.qpid.jms.jndi.JmsInitialContextFactory*, the Qpid InitialContextFactory implementation will
    be discovered when instantiating InitialContext object.

        javax.naming.Context ctx = new javax.naming.InitialContext();

    The particular ConnectionFactory, Queue and Topic objects you wish the context to contain are configured using
    properties (the syntax for which is detailed below) either directly within the jndi.properties file,
    or in a separate file which is referenced in jndi.properties using the *java.naming.provider.url* property.

2.  Via system properties.

    By setting the *java.naming.factory.initial* system property to value *org.apache.qpid.jms.jndi.JmsInitialContextFactory*,
    the Qpid InitialContextFactory implementation will be discovered when instantiating InitialContext object.

         javax.naming.Context ctx = new javax.naming.InitialContext();

    The particular ConnectionFactory, Queue and Topic objects you wish the context to contain are configured as properties in
    a file, which is passed using the *java.naming.provider.url* system property. The syntax for these properties is detailed
    below.

3.  Programatically using an environment Hashtable.

    The InitialContext may also be configured directly by passing an environment during creation:

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        javax.naming.Context context = new javax.naming.InitialContext(env);

    The particular ConnectionFactory, Queue and Topic objects you wish the context to contain are configured as properties
    (the syntax for which is detailed below), either directly within the environment Hashtable, or in a
    separate file which is referenced using the *java.naming.provider.url* property within the environment Hashtable.

The property syntax used in the properties file or environment Hashtable is as follows:

+   To define a ConnectionFactory, use format: *connectionfactory.lookupName = URI*
+   To define a Queue, use format: *queue.lookupName = queueName*
+   To define a Topic use format: *topic.lookupName = topicName*

For more details of the Connection URI, see the next section.

As an example, consider the following properties used to define a ConnectionFactory, Queue, and Topic:

    connectionfactory.myFactoryLookup = amqp://localhost:5672
    queue.myQueueLookup = queueA
    topic.myTopicLookup = topicA

These objects could then be looked up from a Context as follows:

    ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
    Queue queue = (Queue) context.lookup("myQueueLookup");
    Topic topic = (Topic) context.lookup("myTopicLookup");

## Connection URI

The basic format of the clients Connection URI is as follows:

      amqp://hostname:port[?option=value[&option2=value...]]

The client can be configured with a number of different settings using the URI while defining the ConnectionFactory, these are detailed in the following sections.

### JMS Configuration options

The options apply to the behaviour of the JMS objects such as Connection, Session, MessageConsumer and MessageProducer.

+ **jms.username** User name value used to authenticate the connection
+ **jms.password** The password value used to authenticate the connection
+ **jms.clientID** The ClientID value that is applied to the connection.
+ **jms.forceAsyncSend** Configures whether all Messages sent from a MessageProducer are sent asynchronously or only those Message that qualify such as Messages inside a transaction or non-persistent messages.
+ **jms.alwaysSyncSend** Override all asynchronous send conditions and always sends every Message from a MessageProducer synchronously.
+ **jms.sendAcksAsync** Causes all Message acknowledgments to be sent asynchronously.
+ **jms.localMessagePriority** If enabled prefetched messages are reordered locally based on their given Message priority value.
+ **jms.validatePropertyNames** If message property names should be validated as valid Java identifiers. Default is true.
+ **jms.queuePrefix** Optional prefix value added to the name of any Queue created from a JMS Session.
+ **jms.topicPrefix** Optional prefix value added to the name of any Topic created from a JMS Session.
+ **jms.closeTimeout** Timeout value that controls how long the client waits on Connection close before returning. (By default the client waits 15 seconds for a normal close completion event).
+ **jms.connectTimeout** Timeout value that controls how long the client waits on Connection establishment before returning with an error. (By default the client waits 15 seconds for a connection to be established before failing).
+ **jms.clientIDPrefix** Optional prefix value that is used for generated Client ID values when a new Connection is created for the JMS ConnectionFactory.  The default prefix is 'ID:'.
+ **jms.connectionIDPrefix** Optional prefix value that is used for generated Connection ID values when a new Connection is created for the JMS ConnectionFactory.  This connection ID is used when logging some information from the JMS Connection object so a configurable prefix can make breadcrumbing the logs easier.  The default prefix is 'ID:'.

These values control how many messages the remote peer can send to the client and be held in a prefetch buffer for each consumer instance.

+ **jms.prefetchPolicy.queuePrefetch** defaults to 1000
+ **jms.prefetchPolicy.topicPrefetch** defaults to 1000
+ **jms.prefetchPolicy.queueBrowserPrefetch** defaults to 1000
+ **jms.prefetchPolicy.durableTopicPrefetch** defaults to 1000
+ **jms.prefetchPolicy.all** used to set all prefetch values at once.

The RedeliveryPolicy controls how redelivered messages are handled on the client.

+ **jms.redeliveryPolicy.maxRedeliveries** controls when an incoming message is rejected based on the number of times it has been redelivered, the default value is (-1) disabled.  A value of zero would indicate no message redeliveries are accepted, a value of five would allow a message to be redelivered five times, etc.

### TCP Transport Configuration options

When connected to a remote using plain TCP these options configure the behaviour of the underlying socket.  These options are appended to the connection URI along with the other configuration options, for example:

      amqp://localhost:5672?jms.clientID=foo&transport.connectTimeout=30000

The complete set of TCP Transport options is listed below:

+ **transport.sendBufferSize** default is 64k
+ **transport.receiveBufferSize** default is 64k
+ **transport.trafficClass** default is 0
+ **transport.connectTimeout** default is 60 seconds
+ **transport.soTimeout** default is -1
+ **transport.soLinger** default is -1
+ **transport.tcpKeepAlive** default is false
+ **transport.tcpNoDelay** default is true

### SSL Transport Configuration options

The SSL Transport extends the TCP Transport and is enabled using the *amqps* URI scheme.  Because the SSL Transport extends the functionality of the TCP based Transport all the TCP Transport options are valid on an SSL Transport URI.

A simple SSL based client URI is shown below:

    amqps://localhost:5673

The complete set of SSL Transport options is listed below:

+ **transport.keyStoreLocation**  default is to read from the system property "javax.net.ssl.keyStore"
+ **transport.keyStorePassword**  default is to read from the system property "javax.net.ssl.keyStorePassword"
+ **transport.trustStoreLocation**  default is to read from the system property "javax.net.ssl.trustStore"
+ **transport.trustStorePassword**  default is to read from the system property "javax.net.ssl.keyStorePassword"
+ **transport.storeType** The type of trust store being used. Default is "JKS".
+ **transport.contextProtocol** The protocol argument used when getting an SSLContext. Default is "TLS".
+ **transport.enabledCipherSuites** The cipher suites to enable, comma separated. No default, meaning the context default ciphers are used. Any disabled ciphers are removed from this.
+ **transport.disabledCipherSuites** The cipher suites to disable, comma separated. Ciphers listed here are removed from the enabled ciphers. No default.
+ **transport.enabledProtocols** The protocols to enable, comma separated. No default, meaning the context default protocols are used. Any disabled protocols are removed from this.
+ **transport.disabledProtocols** The protocols to disable, comma separated. Protocols listed here are removed from the enabled protocols. Default is "SSLv2Hello,SSLv3".
+ **transport.trustAll** Whether to trust the provided server certificate implicitly, regardless of any configured trust store. Defaults to false.
+ **transport.verifyHost** Whether to verify that the hostname being connected to matches with the provided server certificate. Defaults to true.
+ **transport.keyAlias** The alias to use when selecting a keypair from the keystore if required to send a client certificate to the server. No default.

### Failover Configuration options

With failover enabled the client can reconnect to a different broker automatically when the connection to the current connection is lost for some reason.  The failover URI is always initiated with the *failover* prefix and a list of URIs for the brokers is contained inside a set of parenthesis.

The URI for failover looks something like the following:

    failover:(amqp://broker1:5672,amqp://broker2:5672)?failover.maxReconnectAttempts=20

The complete set of configuration options for failover is listed below:

+ **failover.initialReconnectDelay** The amount of time the client will wait before the first attempt to reconnect to a remote peer.  The default value is zero, meaning the first attempt happens immediately.
+ **failover.reconnectDelay** Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
+ **failover.maxReconnectDelay** The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow too large.  Defaults to 30 seconds as the max time between connect attempts.
+ **failover.useReconnectBackOff** Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
+ **failover.reconnectBackOffMultiplier** The multiplier used to grow the reconnection delay value, defaults to 2.0d.
+ **failover.maxReconnectAttempts** The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
+ **failover.startupMaxReconnectAttempts** For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to use the value of maxReconnectAttempts.
+ **failover.warnAfterReconnectAttempts** Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.

The failover URI options also supports defining 'nested' options applicable to each individual broker URI, which can be used to avoid repetition and define options common to them all. This is accomplished using the same URI options outlined earlier for the indivual broker URI but now prefixed with *failover.nested.*:

    failover:(amqp://broker1:5672,amqp://broker2:5672)?failover.nested.jms.clientID=foo


### AMQP Configuration options

These options apply to the behaviour of certain AMQP functionality.

+ **amqp.idleTimeout** The idle timeout in milliseconds after which the connection will be failed if the peer sends no AMQP frames. Default is 60000.
+ **amqp.vhost** The vhost to connect to. Used to populate the Sasl and Open hostname fields. Default is the main hostname from the Connection URI.


### Discovery Configuration options

The client has an optional Discovery module, which provides a customised failover layer where the broker URIs to connect to are not given in the initial URI, but discovered as the client operates via associated discovery agents. There are currently two discovery agent implementations, a file watcher that loads URIs from a file, and a multicast listener that works with ActiveMQ 5 brokers which have been configured to broadcast their broker addresses for listening clients.

The general set of failover related options when using discovery are the same as those detailed earlier, with the main prefix updated from *failover.* to *discovery.*, and with the 'nested' options prefix used to supply URI options common to all the discovered broker URIs bring updated from *failover.nested.* to *discovery.discovered*. For example, without the agent URI details, a general discovery URI might look like:

    discovery:(<agent-uri>)?discovery.maxReconnectAttempts=20&discovery.discovered.jms.clientID=foo

To use the file watcher discovery agent, utilise an agent URI of the form:

    discovery:(file:///path/to/monitored-file?updateInterval=60000)

The URI options for the file watcher discovery agent are listed below:

+ **updateInterval** Controls the frequency in milliseconds which the file is inspected for change. The default value is 30000.


To use the multicast discovery agent with an ActiveMQ 5 broker, utilise an agent URI of the form:

    discovery:(multicast://default?group=default)

Note that the use of *default* as the host in the multicast agent URI above is a special value (that is substituted by the agent with the default "239.255.2.3:6155"). You may change this to specify the actual IP and port in use with your multicast configuration.

The URI options for the multicast discovery agent are listed below:

+ **group** Controls which multicast group messages are listened for on. The default value is "default".


## Logging

The client makes use of the SLF4J API, allowing users to select a particular logging implementation based on their needs by supplying a SLF4J 'binding', such as *slf4j-log4j* in order to use Log4J. More details on SLF4J are available from http://www.slf4j.org/.

The client uses Logger names residing within the *org.apache.qpid.jms* heirarchy, which you can use to configure a logging implementation based on your needs.

When debugging some issues, it may sometimes be useful to enable additional protocol trace logging from the Qpid Proton AMQP 1.0 library. There are two options to achieve this:

+ Set the environment variable (not Java system property) *PN_TRACE_FRM* to *true*, which will cause Proton to emit frame logging to stdout.
+ Add the option *amqp.traceFrames=true* to your connection URI to have the client add a protocol tracer to Proton, and configure the *org.apache.qpid.jms.provider.amqp.FRAMES* Logger to *TRACE* level to include the output in your logs.
