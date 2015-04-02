package org.apache.qpid.jms.transports;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.junit.Test;

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

        assertEquals("Expected wrapper alias", wrapperAlias, wrapper.chooseClientAlias(new String[0], new Principal[0], new Socket()));
    }

    @Test
    public void testChooseServerAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseServerAlias(any(String.class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected wrapper alias", wrapperAlias, wrapper.chooseServerAlias("", new Principal[0], new Socket()));
    }

    @Test
    public void testGetCertificateChainDelegates() {
        String wrapperAlias = "wrapperAlias";
        X509Certificate[] certs = new X509Certificate[7];

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getCertificateChain(any(String.class))).thenReturn(certs);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame("Different object returned", certs, wrapper.getCertificateChain(wrapperAlias));
    }

    @Test
    public void testGetClientAliasesReturnsGivenAliasOnly() {
        String wrapperAlias = "wrapperAlias";
        String[] delegateAliases = new String[] { "a", "b", wrapperAlias};

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getClientAliases(any(String.class), any(Principal[].class))).thenReturn(delegateAliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertArrayEquals("Expected array containing only the wrapper alias", new String[] { wrapperAlias }, wrapper.getClientAliases("", new Principal[0]));
    }

    @Test
    public void testGetServerAliasesReturnsGivenAliasOnly() {
        String wrapperAlias = "wrapperAlias";
        String[] delegateAliases = new String[] { "a", "b", wrapperAlias};

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getServerAliases(any(String.class), any(Principal[].class))).thenReturn(delegateAliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertArrayEquals("Expected array containing only the wrapper alias", new String[] { wrapperAlias }, wrapper.getServerAliases("", new Principal[0]));
    }

    @Test
    public void testGetPrivateKeyDelegates() {
        String wrapperAlias = "wrapperAlias";
        PrivateKey mockKey = mock(PrivateKey.class);

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getPrivateKey(any(String.class))).thenReturn(mockKey);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame("Different object returned", mockKey, wrapper.getPrivateKey(wrapperAlias));
    }

    @Test
    public void testChooseEngineClientAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseEngineClientAlias(any(String[].class), any(Principal[].class), any(SSLEngine.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected wrapper alias", wrapperAlias, wrapper.chooseEngineClientAlias(new String[0], new Principal[0], mock(SSLEngine.class)));
    }

    @Test
    public void testChooseEngineServerAliasReturnsGivenAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseEngineServerAlias(any(String.class), any(Principal[].class), any(SSLEngine.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected wrapper alias", wrapperAlias, wrapper.chooseEngineServerAlias("", new Principal[0], mock(SSLEngine.class)));
    }
}
