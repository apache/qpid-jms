package org.apache.qpid.jms.transports;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509ExtendedKeyManager;

import org.junit.Test;

public class X509AliasKeyManagerTest {

    @Test
    public void testChooseClientAliasDelegatesWithNullWrapperAlias() {
        String wrapperAlias = null;
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseClientAlias(any(String[].class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected delegate alias", myDelegateAlias, wrapper.chooseClientAlias(new String[0], new Principal[0], new Socket()));
    }

    @Test
    public void testChooseClientAliasDoesNotDelegateWithNonNullWrapperAlias() {
        String wrapperAlias = "wrapperAlias";
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseClientAlias(any(String[].class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected wrapper alias", wrapperAlias, wrapper.chooseClientAlias(new String[0], new Principal[0], new Socket()));
    }

    @Test
    public void testChooseServerAliasDelegatesWithNullWrapperAlias() {
        String wrapperAlias = null;
        String myDelegateAlias = "delegateAlias";
        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.chooseServerAlias(any(String.class), any(Principal[].class), any(Socket.class))).thenReturn(myDelegateAlias);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertEquals("Expected delegate alias", myDelegateAlias, wrapper.chooseServerAlias("", new Principal[0], new Socket()));
    }

    @Test
    public void testChooseServerAliasDoesNotDelegateWithNonNullWrapperAlias() {
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
    public void testGetClientAliasesDelegates() {
        String wrapperAlias = "wrapperAlias";
        String[] aliases = new String[5];

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getClientAliases(any(String.class), any(Principal[].class))).thenReturn(aliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame("Different object returned", aliases, wrapper.getClientAliases("", new Principal[0]));
    }

    @Test
    public void testGetServerAliasesDelegates() {
        String wrapperAlias = "wrapperAlias";
        String[] aliases = new String[3];

        X509ExtendedKeyManager mock = mock(X509ExtendedKeyManager.class);
        when(mock.getServerAliases(any(String.class), any(Principal[].class))).thenReturn(aliases);

        X509ExtendedKeyManager wrapper = new X509AliasKeyManager(wrapperAlias, mock);

        assertSame("Different object returned", aliases, wrapper.getServerAliases("", new Principal[0]));
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
}
