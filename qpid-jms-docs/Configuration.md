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

3.  Programmatically using an environment Hashtable.

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

      amqp[s]://hostname:port[?option=value[&option2=value...]]

or for WebSocket connections:

     amqpws[s]://hostname:port[/path][?option=value[&option2=value...]]

Where the *amqps* and *amqpwss* scheme is specified to use SSL/TLS, the hostname segment from the URI can be used by the JVM for the
TLS SNI (Server Name Indication) extension in order to communicate the desired server hostname during a TLS handshake.
The SNI extension will be automatically included if a Fully Qualified name (e.g myhost.mydomain) is specified, but not
when an unqualified name (e.g myhost) or bare IP address are used.

The client can be configured with a number of different settings using the URI while defining the ConnectionFactory, these are detailed in the following sections.

### JMS Configuration options

The options apply to the behaviour of the JMS objects such as Connection, Session, MessageConsumer and MessageProducer.

+ **jms.username** User name value used to authenticate the connection
+ **jms.password** The password value used to authenticate the connection
+ **jms.clientID** The ClientID value that is applied to the connection.
+ **jms.forceAsyncSend** Configures whether all Messages sent from a MessageProducer are sent asynchronously or only those Message that qualify such as Messages inside a transaction or non-persistent messages.
+ **jms.forceSyncSend** Override all asynchronous send conditions and always sends every Message from a MessageProducer synchronously.
+ **jms.forceAsyncAcks** Causes all Message acknowledgments to be sent asynchronously.
+ **jms.localMessageExpiry** Controls whether MessageConsumer instances will locally filter expired Messages or deliver them.  By default this value is set to true and expired messages will be filtered.
+ **jms.localMessagePriority** If enabled prefetched messages are reordered locally based on their given Message priority value. Default is false.
+ **jms.validatePropertyNames** If message property names should be validated as valid Java identifiers. Default is true.
+ **jms.receiveLocalOnly** If enabled receive calls with a timeout will only check a consumers local message buffer, otherwise the remote peer is checked to ensure there are really no messages available if the local timeout expires before a message arrives. Default is false, the remote is checked.
+ **jms.receiveNoWaitLocalOnly** If enabled receiveNoWait calls will only check a consumers local message buffer, otherwise the remote peer is checked to ensure there are really no messages available. Default is false, the remote is checked.
+ **jms.queuePrefix** Optional prefix value added to the name of any Queue created from a JMS Session.
+ **jms.topicPrefix** Optional prefix value added to the name of any Topic created from a JMS Session.
+ **jms.closeTimeout** Timeout value that controls how long the client waits on resource closure before returning. (By default the client waits 60 seconds for a normal close completion event).
+ **jms.connectTimeout** Timeout value that controls how long the client waits on Connection establishment before returning with an error. (By default the client waits 15 seconds for a connection to be established before failing).
+ **jms.sendTimeout** Timeout value that controls how long the client waits on completion of a synchronous message send before returning an error (By default the client will wait indefinitely for a send to complete).
+ **jms.requestTimeout** Timeout value that controls how long the client waits on completion of various synchronous interactions with the remote peer before returning an error (By default the client will wait indefinitely for a request to complete
+ **jms.clientIDPrefix** Optional prefix value that is used for generated Client ID values when a new Connection is created for the JMS ConnectionFactory.  The default prefix is 'ID:'.
+ **jms.connectionIDPrefix** Optional prefix value that is used for generated Connection ID values when a new Connection is created for the JMS ConnectionFactory.  This connection ID is used when logging some information from the JMS Connection object so a configurable prefix can make breadcrumbing the logs easier.  The default prefix is 'ID:'.
+ **jms.populateJMSXUserID** Controls whether a MessageProducer will populate the JMSXUserID value for each sent message using the authenticated username from the connection.  This value defaults to false and the JMSXUserID for all sent message will not be populated.
+ **jms.awaitClientID** Controls whether a Connection with no ClientID configured in the URI will wait for a ClientID being set programatically (or the connection being used otherwise to signal none can be set) before sending the AMQP connection Open. Defaults to true.
+ **jms.useDaemonThread** Controls whether a Connection will use a daemon thread for its executor. Defaults to false to ensure a non-daemon thread is present by default.

The Prefetch Policy controls how many messages the remote peer can send to the client and be held in a prefetch buffer for each consumer instance.

+ **jms.prefetchPolicy.queuePrefetch** defaults to 1000
+ **jms.prefetchPolicy.topicPrefetch** defaults to 1000
+ **jms.prefetchPolicy.queueBrowserPrefetch** defaults to 1000
+ **jms.prefetchPolicy.durableTopicPrefetch** defaults to 1000
+ **jms.prefetchPolicy.all** used to set all prefetch values at once.

The Redelivery Policy controls how redelivered messages are handled on the client.

+ **jms.redeliveryPolicy.maxRedeliveries** controls when an incoming message is rejected based on the number of times it has been redelivered, the default value is (-1) disabled.  A value of zero would indicate no message redeliveries are accepted, a value of five would allow a message to be redelivered five times, etc.
+ **jms.redeliveryPolicy.outcome** controls the outcome that is applied to a message that is being rejected due to it having exceeded the configured maxRedeliveries value.  This option is configured on the URI using the following set of outcome options:  ACCEPTED, REJECTED, RELEASED, MODIFIED_FAILED and MODIFIED_FAILED_UNDELIVERABLE. The default outcome value is MODIFIED_FAILED_UNDELIVERABLE.

The MessageID Policy controls the type of the Message ID assigned to messages sent from the client.

+ **jms.messageIDPolicy.messageIDType** By default a generated String value is used for the MessageID on outgoing messages. Other available types are UUID, UUID_STRING, and PREFIXED_UUID_STRING.

The Presettle Policy controls when a producer or consumer instance will be configured to use AMQP presettled messaging semantics.

+ **jms.presettlePolicy.presettleAll** when true all producers and non-transacted consumers created operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleProducers** when true all producers operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleTopicProducers** when true any producer that is sending to a Topic or Temporary Topic destination will operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleQueueProducers** when true any producer that is sending to a Queue or Temporary Queue destination will operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleTransactedProducers** when true any producer that is created in a transacted Session will operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleConsumers** when true all consumers operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleTopicConsumers** when true any consumer that is receiving from a Topic or Temporary Topic destination will operate in presettled mode, defaults to false.
+ **jms.presettlePolicy.presettleQueueConsumers** when true any consumer that is receiving from a Queue or Temporary Queue destination will operate in presettled mode, defaults to false.

The Deserialization Policy provides a means of controlling which types are trusted to be deserialized from the object stream while retrieving the body from an incoming JMS ObjectMessage composed of serialized Java Object content. By default all types are trusted during attempt to deserialize the body. The default Deserialization Policy object provides URI options that allow specifying a whitelist and a blacklist of Java class or package names.

**jms.deserializationPolicy.whiteList** A comma separated list of class/package names that should be allowed when deserializing the contents of a JMS ObjectMessage, unless overridden by the blackList. The names in this list are not pattern values, the exact class or package name must be configured, e.g "java.util.Map" or "java.util". Package matches include sub-packages. Default is to allow all.
**jms.deserializationPolicy.blackList** A comma separated list of class/package names that should be rejected when deserializing the contents of a JMS ObjectMessage. The names in this list are not pattern values, the exact class or package name must be configured, e.g "java.util.Map" or "java.util". Package matches include sub-packages. Default is to prevent none.

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

A simple SSL/TLS based client URI is shown below:

    amqps://myhost.mydomain:5671


The complete set of SSL Transport options is listed below:

+ **transport.keyStoreLocation**  default is to read from the system property "javax.net.ssl.keyStore"
+ **transport.keyStorePassword**  default is to read from the system property "javax.net.ssl.keyStorePassword"
+ **transport.trustStoreLocation**  default is to read from the system property "javax.net.ssl.trustStore"
+ **transport.trustStorePassword**  default is to read from the system property "javax.net.ssl.trustStorePassword"
+ **transport.storeType** The type of trust store being used. Default is "JKS".
+ **transport.contextProtocol** The protocol argument used when getting an SSLContext. Default is "TLS".
+ **transport.enabledCipherSuites** The cipher suites to enable, comma separated. No default, meaning the context default ciphers are used. Any disabled ciphers are removed from this.
+ **transport.disabledCipherSuites** The cipher suites to disable, comma separated. Ciphers listed here are removed from the enabled ciphers. No default.
+ **transport.enabledProtocols** The protocols to enable, comma separated. No default, meaning the context default protocols are used. Any disabled protocols are removed from this.
+ **transport.disabledProtocols** The protocols to disable, comma separated. Protocols listed here are removed from the enabled protocols. Default is "SSLv2Hello,SSLv3".
+ **transport.trustAll** Whether to trust the provided server certificate implicitly, regardless of any configured trust store. Defaults to false.
+ **transport.verifyHost** Whether to verify that the hostname being connected to matches with the provided server certificate. Defaults to true.
+ **transport.keyAlias** The alias to use when selecting a keypair from the keystore if required to send a client certificate to the server. No default.
+ **transport.useEpoll** When true the transport will use the native Epoll layer when available instead of the NIO layer, which can improve performance. Defaults to true.

### Websocket Transport Configuration options

The WebSocket (WS) Transport extends the TCP and SSL Transports to provide both unsecured and secured Websocket connectivity and is enabled using the *amqpws* and *amqpwss* URI schemes.  The unsecured WS Transport extends the basic TCP transport which means all the normal TCP Transport configuration options also apply to the WS Transport. Similarly the WSS Transport extends the SSL Transport which means both the TCP and SSL Transport options can be applied to configure it.

A simple WS[S] based client URI is shown below:

    amqpws[s]://myhost.mydomain:5671/[optional-path]


### AMQP Configuration options

These options apply to the behaviour of certain AMQP functionality.

+ **amqp.idleTimeout** The idle timeout in milliseconds after which the connection will be failed if the peer sends no AMQP frames. Default is 60000.
+ **amqp.vhost** The vhost to connect to. Used to populate the Sasl and Open hostname fields. Default is the main hostname from the Connection URI.
+ **amqp.saslLayer** Controls whether connections should use a SASL layer or not. Default is true.
+ **amqp.saslMechanisms** Which SASL mechanism(s) the client should allow selection of, if offered by the server and usable with the configured credentials. Comma separated if specifying more than 1 mechanism. The clients supported mechanisms are currently EXTERNAL, SCRAM-SHA-256, SCRAM-SHA-1, CRAM-MD5, PLAIN, ANONYMOUS, and GSSAPI for Kerberos.  Default is to allow selection from all mechanisms except GSSAPI, which must be specified here to enable.
+ **amqp.maxFrameSize** The max-frame-size value in bytes that is advertised to the peer. Default is 1048576.
+ **amqp.drainTimeout** The time in milliseconds that the client will wait for a response from the remote when a consumer drain request is made. If no response is seen in the allotted timeout period the link will be considered failed and the associated consumer will be closed. Default is 60000.
+ **amqp.allowNonSecureRedirects** Controls whether an AMQP connection will allow for a redirect to an alternative host over a connection that is not secure when the existing connection is secure, e.g. redirecting an SSL connection to a raw TCP connection.  This value defaults to false.

### Failover Configuration options

With failover enabled the client can reconnect to another server automatically when connection to the current server is lost for some reason.  The failover URI is always initiated with the *failover* prefix and a list of URIs for the server(s) is contained inside a set of parentheses. The "jms." options are applied to the overall failover URI, outside the parentheses, and affect the JMS Connection object for its lifetime.

The URI for failover looks something like the following:

    failover:(amqp://host1:5672,amqp://host2:5672)?jms.clientID=foo&failover.maxReconnectAttempts=20

The individual broker details within the parentheses can use the "transport." or "amqp." options defined earlier, with these being applied as each host is connected to:

    failover:(amqp://host1:5672?amqp.option=value,amqp://host2:5672?transport.option=value)?jms.clientID=foo

The complete set of configuration options for failover is listed below:

+ **failover.initialReconnectDelay** The amount of time the client will wait before the first attempt to reconnect to a remote peer.  The default value is zero, meaning the first attempt happens immediately.
+ **failover.reconnectDelay** Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
+ **failover.maxReconnectDelay** The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow too large.  Defaults to 30 seconds as the max time between connect attempts.
+ **failover.useReconnectBackOff** Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
+ **failover.reconnectBackOffMultiplier** The multiplier used to grow the reconnection delay value, defaults to 2.0d.
+ **failover.maxReconnectAttempts** The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
+ **failover.startupMaxReconnectAttempts** For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to use the value of maxReconnectAttempts.
+ **failover.warnAfterReconnectAttempts** Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.
+ **failover.randomize** When true the set of failover URIs is randomly shuffled prior to attempting to connect to one of them.  This can help to distribute client connections more evenly across multiple remote peers.  The default value is false.
+ **failover.amqpOpenServerListAction** Controls how the failover transport behaves when the connection Open frame from the remote peer provides a list of failover hosts to the client.  This option accepts one of three values; REPLACE, ADD, or IGNORE (default is REPLACE).  If REPLACE is configured then all failover URIs other than the one for the current server are replaced with those provided by the remote peer.  If ADD is configured then the URIs provided by the remote are added to the existing set of failover URIs, with de-duplication.  If IGNORE is configured then any updates from the remote are dropped and no changes are made to the set of failover URIs in use.

The failover URI also supports defining 'nested' options as a means of specifying AMQP and transport option values applicable to all the individual nested broker URI's, which can be useful to avoid repetition. This is accomplished using the same "transport." and "amqp." URI options outlined earlier for a non-failover broker URI but prefixed with *failover.nested.*. For example, to apply the same value for the *amqp.vhost* option to every broker connected to you might have a URI like:

    failover:(amqp://host1:5672,amqp://host2:5672)?jms.clientID=foo&failover.nested.amqp.vhost=myhost



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

The client uses Logger names residing within the *org.apache.qpid.jms* hierarchy, which you can use to configure a logging implementation based on your needs.

When debugging some issues, it may sometimes be useful to enable additional protocol trace logging from the Qpid Proton AMQP 1.0 library. There are two options to achieve this:

+ Set the environment variable (not Java system property) *PN_TRACE_FRM* to *true*, which will cause Proton to emit frame logging to stdout.
+ Add the option *amqp.traceFrames=true* to your connection URI to have the client add a protocol tracer to Proton, and configure the *org.apache.qpid.jms.provider.amqp.FRAMES* Logger to *TRACE* level to include the output in your logs.


## Authenticating using Kerberos

The client can be configured to authenticate using Kerberos when used with an appropriately configured server. To do so, you must:

1.  Configure the client to use the GSSAPI mechanism for SASL authentication using the *amqp.saslMechanisms* URI option, e.g:

        amqp://myhost:5672?amqp.saslMechanisms=GSSAPI
        failover:(amqp://myhost:5672?amqp.saslMechanisms=GSSAPI)

2.  Set the *java.security.auth.login.config* system property to the path of a JAAS Login Configuration file containing appropriate configuration for a Kerberos LoginModule, e.g:

        -Djava.security.auth.login.config=/path/to/login.config

    An example login.config configuration file might look like the following:

        amqp-jms-client {
            com.sun.security.auth.module.Krb5LoginModule required
            useTicketCache=true;
        };

The precise configuration used will depend on how you wish the credentials to be established for the connection, and the particular LoginModule in use. For details of the Sun/Oracle Krb5LoginModule, see [https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html](https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html). For details of the IBM Java 8 Krb5LoginModule, see [https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.api.doc/jgss/com/ibm/security/auth/module/Krb5LoginModule.html](https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.api.doc/jgss/com/ibm/security/auth/module/Krb5LoginModule.html).

It is possible to configure the LoginModule to establish the credentials to use for the Kerberos process, such as specifying a Principal and whether to use an existing ticket cache or keytab. If however the LoginModule configuration does not provide means to establish all necessary credentials, it may then request and be passed the username and/or password values from the client Connection object if they were either supplied when creating the Connection using the ConnectionFactory or previously configured via its URI options.

Note that Kerberos is only only supported for authentication purposes. Use SSL/TLS connections for encryption.

The following URI options can be used to influence the Kerberos authentication process:

+ **sasl.options.configScope** The Login Configuration entry name to use when authenticating. Default is "amqp-jms-client".
+ **sasl.options.protocol** The protocol value used during the GSSAPI SASL process. Default is "amqp".
+ **sasl.options.serverName** The serverName value used during the GSSAPI SASL process. Default is the server hostname from the connection URI.

Similar to the "amqp." and "transport." options detailed previously, these options must be specified on a per-host basis or as all-host nested options in a failover URI.
