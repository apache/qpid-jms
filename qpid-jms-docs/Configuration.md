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
+ **jms.clientId** A client ID value that is applied to the connection.
+ **jms.forceAsyncSend** Configures whether all Messages sent from a MessageProducer are sent asynchronously or only those Message that qualify such as Messages inside a transaction or non-persistent messages.
+ **jms.alwaysSyncSend** Override all asynchronous send conditions and always sends every Message from a MessageProducer synchronously.
+ **jms.sendAcksAsync** Causes all Message acknowledgments to be sent asynchronously.
+ **jms.localMessagePriority** If enabled prefetched messages are reordered locally based on their given Message priority value.
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

### TCP Transport Configuration options

When connected to a remote using plain TCP these options configure the behaviour of the underlying socket.  These options are appended to the connection URI along with the other configuration options, for example:

      amqp://localhost:5672?jms.topicPrefix=foo&transport.connectTimeout=30000

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
+ **transport.storeType**  default is 'JKS'
+ **transport.enabledCipherSuites**  defaults to Java defaults
+ **transport.enabledProtocols**  defaults to Java defaults
+ **transport.trustAll**  defaults to false
+ **transport.verifyHost**  defaults to true

### Failover Configuration options

With failover enabled the client can reconnect to a different broker automatically when the connection to the current connection is lost for some reason.  The failover URI is always initiated with the *failover* prefix and a list of URIs for the brokers is contained inside a set of parenthesis.

The URI for failover looks something like the following:

    failover:(amqp://broker1:5672,amqp://broker2:5672)?failover.maxReconnectAttempts=20

The complete set of configuration options for failover is listed below:

+ **failover.initialReconnectDelay** The amount of time the client will wait before the first attempt to reconnect to a remote peer.  The default value is zero, meaning the first attempt happens immediately.
+ **failover.reconnectDelay** Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
+ **failover.maxReconnectDelay** The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow to large.  Defaults to 30 seconds as the max time between connect attempts.
+ **failover.useReconnectBackOff** Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
+ **failover.reconnectBackOffMultiplier** The multiplier used to grow the reconnection delay value, defaults to 2.0d
+ **failover.maxReconnectAttempts** The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
+ **failover.startupMaxReconnectAttempts** For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to default to the value of maxReconnectAttempts.
+ **failover.warnAfterReconnectAttempts** Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.
