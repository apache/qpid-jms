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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.junit.jupiter.api.Test;

public class X509AliasKeyManagerTest {

    @Test
    public void testNullAliasCausesIAE() {
        try {
            new X509AliasKeyManager(null, mock(X509ExtendedKeyManager.class));
            fail("Expected an exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testChooseClientAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseClientAlias(any(String[].class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals(wrapperAlias, wrapper.chooseClientAlias(new String[0], new Principal[0], new Socket()), "Expected wrapper alias");
    }

    @Test
    public void testChooseServerAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseServerAlias(any(String.class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals(wrapperAlias, wrapper.chooseServerAlias("", new Principal[0], new Socket()), "Expected wrapper alias");
    }

    @Test
    public void testGetCertificateChainDelegates() {
        String wrapperAlias = "wrapperAlias";
        X509Certificate[] certs = new X509Certificate[7];

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getCertificateChain(any(String.class))).thenReturn(certs);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame(certs, wrapper.getCertificateChain(wrapperAlias), "Different object returned");
    }

    @Test
    public void testGetClientAliasesReturnsGivenAliasOnly() {
        String wrapperAlias = "wrapperAlias";
        String[] delegateAliases = new String[] { "a", "b", wrapperAlias};

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getClientAliases(any(String.class), any(Principal[].class))).thenReturn(delegateAliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertArrayEquals(new String[] { wrapperAlias }, wrapper.getClientAliases("", new Principal[0]), "Expected array containing only the wrapper alias");
    }

    @Test
    public void testGetServerAliasesReturnsGivenAliasOnly() {
        String wrapperAlias = "wrapperAlias";
        String[] delegateAliases = new String[] { "a", "b", wrapperAlias};

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getServerAliases(any(String.class), any(Principal[].class))).thenReturn(delegateAliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertArrayEquals(new String[] { wrapperAlias }, wrapper.getServerAliases("", new Principal[0]), "Expected array containing only the wrapper alias");
    }

    @Test
    public void testGetPrivateKeyDelegates() {
        String wrapperAlias = "wrapperAlias";
        PrivateKey mockKey = mock(PrivateKey.class);

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getPrivateKey(any(String.class))).thenReturn(mockKey);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame(mockKey, wrapper.getPrivateKey(wrapperAlias), "Different object returned");
    }

    @Test
    public void testChooseEngineClientAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseEngineClientAlias(any(String[].class), any(Principal[].class), any(SSLEngine.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals(wrapperAlias, wrapper.chooseEngineClientAlias(new String[0], new Principal[0], mock(SSLEngine.class)), "Expected wrapper alias");
    }

    @Test
    public void testChooseEngineServerAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseEngineServerAlias(any(String.class), any(Principal[].class), any(SSLEngine.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals(wrapperAlias, wrapper.chooseEngineServerAlias("", new Principal[0], mock(SSLEngine.class)), "Expected wrapper alias");
    }
}
