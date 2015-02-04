# Client configuration options

The client can be configured using the following options applied on the connection URI.

## JMS Configuration options

The options apply to the behavior of the JMS objects.

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

* __jms.prefetchPolicy.queuePrefetch__ defaults to 1000
* __jms.prefetchPolicy.topicPrefetch__ defaults to 32767
* __jms.prefetchPolicy.queueBrowserPrefetch__ defaults to 500
* __jms.prefetchPolicy.durableTopicPrefetch__ defaults to 100
* __jms.prefetchPolicy.all__ used to set all values at once.

## TCP Transport Configuration options

* __transport.sendBufferSize__ default is 64k
* __transport.receiveBufferSize__ default is 64k
* __transport.trafficClass__ default is 0
* __transport.connectTimeout__ default is 60 seconds
* __transport.soTimeout__ default is -1
* __transport.soLinger__ default is -1
* __transport.tcpKeepAlive__ default is false
* __transport.tcpNoDelay__ default is true

## SSL Transport Configuration options

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

With failover enabled the client can reconnect to a different broker automatically when the connection to the current connection is lost for some reason.

The URI for failover looks something like the following:

    failover:(amqp://broker1:5672,amqp://broker2:5672)?failover.maxReconnectAttempts=20

The complete set of configuration options for failover is documented below:

* __failover.initialReconnectDelay__ The amount of time the client will wait before the first attempt to reconnect to a remote peer.  The default value is zero, meaning the first attempt happens immediately.
* __failover.reconnectDelay__ Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
* __failover.maxReconnectDelay__ The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow to large.  Defaults to 30 seconds as the max time between connect attempts.
* __failover.useExponentialBackOff__ Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
* __failover.backOffMultiplier__ The multiplier used to grow the reconnection delay value, defaults to 2.0d
* __failover.maxReconnectAttempts__ The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
* __failover.startupMaxReconnectAttempts__ For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to default to the value of maxReconnectAttempts.
* __failover.warnAfterReconnectAttempts__ Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.