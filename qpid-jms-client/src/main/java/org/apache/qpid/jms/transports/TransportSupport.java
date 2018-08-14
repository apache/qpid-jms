/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.transports;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslX509KeyManagerFactory;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Static class that provides various utility methods used by Transport implementations.
 */
public class TransportSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TransportSupport.class);

    /**
     * Determines if Netty OpenSSL support is available and applicable based on the configuration
     * in the given TransportOptions instance.
     *
     * @param options
     * 		  The configuration of the Transport being created.
     *
     * @return true if OpenSSL support is available and usable given the requested configuration.
     */
    public static boolean isOpenSSLPossible(TransportOptions options) {
        boolean result = false;

        if (options.isUseOpenSSL()) {
            if (!OpenSsl.isAvailable()) {
                LOG.debug("OpenSSL could not be enabled because a suitable implementation could not be found.", OpenSsl.unavailabilityCause());
            } else if (options.getSslContextOverride() != null) {
                LOG.debug("OpenSSL could not be enabled due to user SSLContext being supplied.");
            } else if (!OpenSsl.supportsKeyManagerFactory()) {
                LOG.debug("OpenSSL could not be enabled because the version provided does not allow a KeyManagerFactory to be used.");
            } else if (options.isVerifyHost() && !OpenSsl.supportsHostnameValidation()) {
                LOG.debug("OpenSSL could not be enabled due to verifyHost being enabled but not supported by the provided OpenSSL version.");
            } else if (options.getKeyAlias() != null) {
                LOG.debug("OpenSSL could not be enabled because a keyAlias is set and that feature is not supported for OpenSSL.");
            } else {
                LOG.debug("OpenSSL Enabled: Version {} of OpenSSL will be used", OpenSsl.versionString());
                result = true;
            }
        }

        return result;
    }

    /**
     * Creates a Netty SslHandler instance for use in Transports that require
     * an SSL encoder / decoder.
     *
     * If the given options contain an SSLContext override, this will be used directly
     * when creating the handler. If they do not, an SSLContext will first be created
     * using the other option values.
     *
     * @param allocator
     *		  The Netty Buffer Allocator to use when Netty resources need to be created.
     * @param remote
     *        The URI of the remote peer that the SslHandler will be used against.
     * @param options
     *        The SSL options object to build the SslHandler instance from.
     *
     * @return a new SslHandler that is configured from the given options.
     *
     * @throws Exception if an error occurs while creating the SslHandler instance.
     */
    public static SslHandler createSslHandler(ByteBufAllocator allocator, URI remote, TransportOptions options) throws Exception {
        final SSLEngine sslEngine;

        if (isOpenSSLPossible(options)) {
            SslContext sslContext = createOpenSslContext(options);
            sslEngine = createOpenSslEngine(allocator, remote, sslContext, options);
        } else {
            SSLContext sslContext = options.getSslContextOverride();
            if (sslContext == null) {
                sslContext = createJdkSslContext(options);
            }

            sslEngine = createJdkSslEngine(remote, sslContext, options);
        }

        return new SslHandler(sslEngine);
    }

    //----- JDK SSL Support Methods ------------------------------------------//

    /**
     * Create a new SSLContext using the options specific in the given TransportOptions
     * instance.
     *
     * @param options
     *        the configured options used to create the SSLContext.
     *
     * @return a new SSLContext instance.
     *
     * @throws Exception if an error occurs while creating the context.
     */
    public static SSLContext createJdkSslContext(TransportOptions options) throws Exception {
        try {
            String contextProtocol = options.getContextProtocol();
            LOG.trace("Getting SSLContext instance using protocol: {}", contextProtocol);

            SSLContext context = SSLContext.getInstance(contextProtocol);

            KeyManager[] keyMgrs = loadKeyManagers(options);
            TrustManager[] trustManagers = loadTrustManagers(options);

            context.init(keyMgrs, trustManagers, new SecureRandom());
            return context;
        } catch (Exception e) {
            LOG.error("Failed to create SSLContext: {}", e, e);
            throw e;
        }
    }

    /**
     * Create a new JDK SSLEngine instance in client mode from the given SSLContext and
     * TransportOptions instances.
     *
     * @param remote
     *        the URI of the remote peer that will be used to initialize the engine, may be null if none should.
     * @param context
     *        the SSLContext to use when creating the engine.
     * @param options
     *        the TransportOptions to use to configure the new SSLEngine.
     *
     * @return a new SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createJdkSslEngine(URI remote, SSLContext context, TransportOptions options) throws Exception {
        SSLEngine engine = null;
        if (remote == null) {
            engine = context.createSSLEngine();
        } else {
            engine = context.createSSLEngine(remote.getHost(), remote.getPort());
        }

        engine.setEnabledProtocols(buildEnabledProtocols(engine, options));
        engine.setEnabledCipherSuites(buildEnabledCipherSuites(engine, options));
        engine.setUseClientMode(true);

        if (options.isVerifyHost()) {
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParameters);
        }

        return engine;
    }

    //----- OpenSSL Support Methods ------------------------------------------//

    /**
     * Create a new Netty SslContext using the options specific in the given TransportOptions
     * instance.
     *
     * @param options
     *        the configured options used to create the SslContext.
     *
     * @return a new SslContext instance.
     *
     * @throws Exception if an error occurs while creating the context.
     */
    public static SslContext createOpenSslContext(TransportOptions options) throws Exception {
        try {
            String contextProtocol = options.getContextProtocol();
            LOG.trace("Getting SslContext instance using protocol: {}", contextProtocol);

            KeyManagerFactory keyManagerFactory = loadKeyManagerFactory(options, SslProvider.OPENSSL);
            TrustManagerFactory trustManagerFactory = loadTrustManagerFactory(options);
            SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL);

            // TODO - There is oddly no way in Netty right now to get the set of supported protocols
            //        when creating the SslContext or really even when creating the SSLEngine.  Seems
            //        like an oversight, for now we call it with TLSv1.2 so it looks like we did something.
            if (options.getContextProtocol().equals(TransportOptions.DEFAULT_CONTEXT_PROTOCOL)) {
                builder.protocols("TLSv1.2");
            } else {
                builder.protocols(options.getContextProtocol());
            }
            builder.keyManager(keyManagerFactory);
            builder.trustManager(trustManagerFactory);

            return builder.build();
        } catch (Exception e) {
            LOG.error("Failed to create SslContext: {}", e, e);
            throw e;
        }
    }

    /**
     * Create a new OpenSSL SSLEngine instance in client mode from the given SSLContext and
     * TransportOptions instances.
     *
     * @param allocator
     *		  the Netty ByteBufAllocator to use to create the OpenSSL engine
     * @param remote
     *        the URI of the remote peer that will be used to initialize the engine, may be null if none should.
     * @param context
     *        the Netty SslContext to use when creating the engine.
     * @param options
     *        the TransportOptions to use to configure the new SSLEngine.
     *
     * @return a new Netty managed SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createOpenSslEngine(ByteBufAllocator allocator, URI remote, SslContext context, TransportOptions options) throws Exception {
        SSLEngine engine = null;

        if (allocator == null) {
            throw new IllegalArgumentException("OpenSSL engine requires a valid ByteBufAllocator to operate");
        }

        if (remote == null) {
            engine = context.newEngine(allocator);
        } else {
            engine = context.newEngine(allocator, remote.getHost(), remote.getPort());
        }

        engine.setEnabledProtocols(buildEnabledProtocols(engine, options));
        engine.setEnabledCipherSuites(buildEnabledCipherSuites(engine, options));
        engine.setUseClientMode(true);

        if (options.isVerifyHost()) {
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParameters);
        }

        return engine;
    }

    //----- Internal support methods -----------------------------------------//

    private static String[] buildEnabledProtocols(SSLEngine engine, TransportOptions options) {
        List<String> enabledProtocols = new ArrayList<String>();

        if (options.getEnabledProtocols() != null) {
            List<String> configuredProtocols = Arrays.asList(options.getEnabledProtocols());
            LOG.trace("Configured protocols from transport options: {}", configuredProtocols);
            enabledProtocols.addAll(configuredProtocols);
        } else {
            List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
            LOG.trace("Default protocols from the SSLEngine: {}", engineProtocols);
            enabledProtocols.addAll(engineProtocols);
        }

        String[] disabledProtocols = options.getDisabledProtocols();
        if (disabledProtocols != null) {
            List<String> disabled = Arrays.asList(disabledProtocols);
            LOG.trace("Disabled protocols: {}", disabled);
            enabledProtocols.removeAll(disabled);
        }

        LOG.trace("Enabled protocols: {}", enabledProtocols);

        return enabledProtocols.toArray(new String[0]);
    }

    private static String[] buildEnabledCipherSuites(SSLEngine engine, TransportOptions options) {
        List<String> enabledCipherSuites = new ArrayList<String>();

        if (options.getEnabledCipherSuites() != null) {
            List<String> configuredCipherSuites = Arrays.asList(options.getEnabledCipherSuites());
            LOG.trace("Configured cipher suites from transport options: {}", configuredCipherSuites);
            enabledCipherSuites.addAll(configuredCipherSuites);
        } else {
            List<String> engineCipherSuites = Arrays.asList(engine.getEnabledCipherSuites());
            LOG.trace("Default cipher suites from the SSLEngine: {}", engineCipherSuites);
            enabledCipherSuites.addAll(engineCipherSuites);
        }

        String[] disabledCipherSuites = options.getDisabledCipherSuites();
        if (disabledCipherSuites != null) {
            List<String> disabled = Arrays.asList(disabledCipherSuites);
            LOG.trace("Disabled cipher suites: {}", disabled);
            enabledCipherSuites.removeAll(disabled);
        }

        LOG.trace("Enabled cipher suites: {}", enabledCipherSuites);

        return enabledCipherSuites.toArray(new String[0]);
    }

    private static TrustManager[] loadTrustManagers(TransportOptions options) throws Exception {
        TrustManagerFactory factory = loadTrustManagerFactory(options);
        if (factory != null) {
            return factory.getTrustManagers();
        } else {
            return null;
        }
    }

    private static TrustManagerFactory loadTrustManagerFactory(TransportOptions options) throws Exception {
        if (options.isTrustAll()) {
            return InsecureTrustManagerFactory.INSTANCE;
        }

        if (options.getTrustStoreLocation() == null) {
            return null;
        }

        TrustManagerFactory fact = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.getTrustStoreLocation();
        String storePassword = options.getTrustStorePassword();
        String storeType = options.getTrustStoreType();

        LOG.trace("Attempt to load TrustStore from location {} of type {}", storeLocation, storeType);

        KeyStore trustStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(trustStore);

        return fact;
    }

    private static KeyManager[] loadKeyManagers(TransportOptions options) throws Exception {
        if (options.getKeyStoreLocation() == null) {
            return null;
        }

        KeyManagerFactory fact = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.getKeyStoreLocation();
        String storePassword = options.getKeyStorePassword();
        String storeType = options.getKeyStoreType();
        String alias = options.getKeyAlias();

        LOG.trace("Attempt to load KeyStore from location {} of type {}", storeLocation, storeType);

        KeyStore keyStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(keyStore, storePassword != null ? storePassword.toCharArray() : null);

        if (alias == null) {
            return fact.getKeyManagers();
        } else {
            validateAlias(keyStore, alias);
            return wrapKeyManagers(alias, fact.getKeyManagers());
        }
    }

    private static KeyManagerFactory loadKeyManagerFactory(TransportOptions options, SslProvider provider) throws Exception {
        if (options.getKeyStoreLocation() == null) {
            return null;
        }

        final KeyManagerFactory factory;
        if (provider.equals(SslProvider.JDK)) {
            factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } else {
            factory = new OpenSslX509KeyManagerFactory();
        }

        String storeLocation = options.getKeyStoreLocation();
        String storePassword = options.getKeyStorePassword();
        String storeType = options.getKeyStoreType();

        LOG.trace("Attempt to load KeyStore from location {} of type {}", storeLocation, storeType);

        KeyStore keyStore = loadStore(storeLocation, storePassword, storeType);
        factory.init(keyStore, storePassword != null ? storePassword.toCharArray() : null);

        return factory;
    }

    private static KeyManager[] wrapKeyManagers(String alias, KeyManager[] origKeyManagers) {
        KeyManager[] keyManagers = new KeyManager[origKeyManagers.length];
        for (int i = 0; i < origKeyManagers.length; i++) {
            KeyManager km = origKeyManagers[i];
            if (km instanceof X509ExtendedKeyManager) {
                km = new X509AliasKeyManager(alias, (X509ExtendedKeyManager) km);
            }

            keyManagers[i] = km;
        }

        return keyManagers;
    }

    private static void validateAlias(KeyStore store, String alias) throws IllegalArgumentException, KeyStoreException {
        if (!store.containsAlias(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' doesn't exist in the key store");
        }

        if (!store.isKeyEntry(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' in the keystore doesn't represent a key entry");
        }
    }

    private static KeyStore loadStore(String storePath, final String password, String storeType) throws Exception {
        KeyStore store = KeyStore.getInstance(storeType);
        try (InputStream in = new FileInputStream(new File(storePath));) {
            store.load(in, password != null ? password.toCharArray() : null);
        }

        return store;
    }
}
