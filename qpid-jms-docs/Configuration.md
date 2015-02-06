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
    properties (the syntax for which is detailed in the next section) either directly within the jndi.properties file,
    or in a separate file which is referenced in jndi.properties using the *java.naming.provider.url* property.

2.  Via system properties.

    By setting the *java.naming.factory.initial* system property to value *org.apache.qpid.jms.jndi.JmsInitialContextFactory*,
    the Qpid InitialContextFactory implementation will be discovered when instantiating InitialContext object.

         javax.naming.Context ctx = new javax.naming.InitialContext();

    The particular ConnectionFactory, Queue and Topic objects you wish the context to contain are configured as properties in
    a file, which is passed using the *java.naming.provider.url* system property. The syntax for these properties is detailed
    in the next section.

3.  Programatically using an environment Hashtable.

    The InitialContext may also be configured directly by passing an environment during creation:

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        javax.naming.Context context = new javax.naming.InitialContext(env);

    The particular ConnectionFactory, Queue and Topic objects you wish the context to contain are configured as properties
    (the syntax for which is detailed in the next section), either directly within the environment Hashtable, or in a
    separate file which is referenced using the *java.naming.provider.url* property within the environment Hashtable.

## Qpid JmsInitialContextFactory properties syntax

The property syntax used in the properties file or environment Hashtable is as follows:

*   To define a ConnectionFactory, use format: *connectionfactory.lookupName = URI*
*   To define a Queue, use format: *queue.lookupName = queueName*
*   To define a Topic use format: *topic.lookupName = topicName*

For example, consider the following properties to define a ConnectionFactory, Queue, and Topic:

    connectionfactory.myFactoryLookup = amqp://localhost:5672
    queue.myQueueLookup = queueA
    topic.myTopicLookup = topicA

These objects could then be looked up from a Context as follows:

    ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
    Queue queue = (Queue) context.lookup("myQueueLookup");
    Topic topic = (Topic) context.lookup("myTopicLookup");

# Connection URI options

The client can be configured with a number of different settings using the connection URI while defining the ConnectionFactory.

## JMS Configuration options

The options apply to the behaviour of the JMS objects such as Connection, Session, MessageConsumer and MessageProducer.

* __jms.username__ User name value used to authenticate the connection
* __jms.password__ The password value used to authenticate the connection
* __jms.clientId__ A client ID value that is applied to the connection.
* __jms.forceAsyncSend__ Configures whether all Messages sent from a MessageProducer are sent asynchronously or only those Message that qualify such as Messages inside a transaction or non-persistent messages.
* __jms.alwaysSyncSend__ Override all asynchronous send conditions and always sends every Message from a MessageProducer synchronously.
* __jms.sendAcksAsync__ Causes all Message acknowledgments to be sent asynchronously.
* __jms.localMessagePriority__ If enabled prefetched messages are reordered locally based on their given Message priority value.
* __jms.queuePrefix__ Optional prefix value added to the name of any Queue created from a JMS Session.
* __jms.topicPrefix__ Optional prefix value added to the name of any Topic created from a JMS Session.
* __jms.closeTimeout__ Timeout value that controls how long the client waits on Connection close before returning. (By default the client waits 15 seconds for a normal close completion event).
* __jms.connectTimeout__ Timeout value that controls how long the client waits on Connection establishment before returning with an error. (By default the client wait 15 seconds for a connection to be established before failing).
* __jms.clientIDPrefix__ Optional prefix value that is used for generated Client ID values when a new Connection is created for the JMS ConnectionFactory.  The default prefix is 'ID:'.
* __jms.connectionIDPrefix__ Optional prefix value that is used for generated Connection ID values when a new Connection is created for the JMS ConnectionFactory.  This connection ID is used when logging some information from the JMS Connection object so a configurable prefix can make breadcrumbing the logs easier.  The default prefix is 'ID:'.

These values control how many message the remote can send to the client and held in a prefetch buffer for each consumer instance.

* __jms.prefetchPolicy.queuePrefetch__ defaults to 1000
* __jms.prefetchPolicy.topicPrefetch__ defaults to 1000
* __jms.prefetchPolicy.queueBrowserPrefetch__ defaults to 1000
* __jms.prefetchPolicy.durableTopicPrefetch__ defaults to 1000
* __jms.prefetchPolicy.all__ used to set all values at once.

## TCP Transport Configuration options

When connected to a remote using plain TCP these options configure the behaviour of the underlying socket.  These options are appended to the connection URI along with the other configuration options, for example:

      amqp://localhost:5672?jms.topicPrefix=foo&transport.connectTimeout=30000

The complete set of TCP Transport options is listed below:

* __transport.sendBufferSize__ default is 64k
* __transport.receiveBufferSize__ default is 64k
* __transport.trafficClass__ default is 0
* __transport.connectTimeout__ default is 60 seconds
* __transport.soTimeout__ default is -1
* __transport.soLinger__ default is -1
* __transport.tcpKeepAlive__ default is false
* __transport.tcpNoDelay__ default is true

## SSL Transport Configuration options

The SSL Transport extends the TCP Transport and is enabled using the *amqps* URI scheme.  Because the SSL Transport extends the functionality of the TCP based Transport all the TCP Transport options are valid on an SSL Transport URI.

A simple SSL based client URI is shown below:

    amqps://localhost:5673

The complete set of SSL Transport options is listed below:

* __transport.keyStoreLocation__  default is to read from the system property "javax.net.ssl.keyStore"
* __transport.keyStorePassword__  default is to read from the system property "javax.net.ssl.keyStorePassword"
* __transport.trustStoreLocation__  default is to read from the system property "javax.net.ssl.trustStore"
* __transport.trustStorePassword__  default is to read from the system property "javax.net.ssl.keyStorePassword"
* __transport.storeType__  default is 'JKS'
* __transport.enabledCipherSuites__  defaults to Java defaults
* __transport.enabledProtocols__  defaults to Java defaults
* __transport.trustAll__  defaults to false
* __transport.verifyHost__  defaults to false

## Failover Configuration options

With failover enabled the client can reconnect to a different broker automatically when the connection to the current connection is lost for some reason.  The failover URI is always initiated with the *failover* prefix and a list of URIs for the brokers is contained inside a set of parenthesis.

The URI for failover looks something like the following:

    failover:(amqp://broker1:5672,amqp://broker2:5672)?failover.maxReconnectAttempts=20

The complete set of configuration options for failover is listed below:

* __failover.initialReconnectDelay__ The amount of time the client will wait before the first attempt to reconnect to a remote peer.  The default value is zero, meaning the first attempt happens immediately.
* __failover.reconnectDelay__ Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
* __failover.maxReconnectDelay__ The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow to large.  Defaults to 30 seconds as the max time between connect attempts.
* __failover.useReconnectBackOff__ Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
* __failover.reconnectBackOffMultiplier__ The multiplier used to grow the reconnection delay value, defaults to 2.0d
* __failover.maxReconnectAttempts__ The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
* __failover.startupMaxReconnectAttempts__ For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to default to the value of maxReconnectAttempts.
* __failover.warnAfterReconnectAttempts__ Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.
