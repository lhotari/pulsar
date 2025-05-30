/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedDataAttributeName;
import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedRoleAttributeName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.util.FutureUtil;
import org.assertj.core.util.Lists;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for {@link AuthenticationProviderList}.
 */
public class AuthenticationProviderListTest {

    private static final String SUBJECT_A = "my-subject-a";
    private static final String SUBJECT_B = "my-subject-b";

    private AuthenticationProviderToken providerA;
    private KeyPair keyPairA;
    private AuthenticationProviderToken providerB;
    private KeyPair keyPairB;
    private String tokenAA;
    private String tokenAB;
    private String tokenBA;
    private String tokenBB;
    private String expiringTokenAA;
    private String expiringTokenAB;
    private String expiringTokenBA;
    private String expiringTokenBB;

    private AuthenticationProviderList authProvider;

    @BeforeMethod
    public void setUp() throws Exception {
        this.keyPairA = Keys.keyPairFor(SignatureAlgorithm.ES256);
        this.keyPairB = Keys.keyPairFor(SignatureAlgorithm.RS512);

        this.providerA = new AuthenticationProviderToken();
        this.providerB = new AuthenticationProviderToken();

        Properties propertiesA = new Properties();
        propertiesA.setProperty(AuthenticationProviderToken.CONF_TOKEN_SETTING_PREFIX, "a");
        propertiesA.setProperty(
            "a" + AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY,
            AuthTokenUtils.encodeKeyBase64(keyPairA.getPublic()));
        propertiesA.setProperty(
            "a" + AuthenticationProviderToken.CONF_TOKEN_PUBLIC_ALG,
            SignatureAlgorithm.ES256.getValue()
        );
        ServiceConfiguration confA = new ServiceConfiguration();
        confA.setProperties(propertiesA);
        providerA.initialize(AuthenticationProvider.Context.builder().config(confA).build());

        Properties propertiesB = new Properties();
        propertiesB.setProperty(AuthenticationProviderToken.CONF_TOKEN_SETTING_PREFIX, "b");
        propertiesB.setProperty(
            "b" + AuthenticationProviderToken.CONF_TOKEN_PUBLIC_KEY,
            AuthTokenUtils.encodeKeyBase64(keyPairB.getPublic()));
        propertiesB.setProperty(
            "b" + AuthenticationProviderToken.CONF_TOKEN_PUBLIC_ALG,
            SignatureAlgorithm.RS512.getValue()
        );
        ServiceConfiguration confB = new ServiceConfiguration();
        confB.setProperties(propertiesB);
        providerB.initialize(AuthenticationProvider.Context.builder().config(confB).build());

        this.authProvider = new AuthenticationProviderList(Lists.newArrayList(
            providerA, providerB
        ));

        // generate tokens
        PrivateKey privateKeyA = AuthTokenUtils.decodePrivateKey(
            Decoders.BASE64.decode(AuthTokenUtils.encodeKeyBase64(keyPairA.getPrivate())),
            SignatureAlgorithm.ES256
        );
        this.tokenAA = AuthTokenUtils.createToken(privateKeyA, SUBJECT_A, Optional.empty());
        this.tokenAB = AuthTokenUtils.createToken(privateKeyA, SUBJECT_B, Optional.empty());
        this.expiringTokenAA = AuthTokenUtils.createToken(privateKeyA, SUBJECT_A,
            Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3))));
        this.expiringTokenAB = AuthTokenUtils.createToken(privateKeyA, SUBJECT_B,
            Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3))));

        PrivateKey privateKeyB = AuthTokenUtils.decodePrivateKey(
            Decoders.BASE64.decode(AuthTokenUtils.encodeKeyBase64(keyPairB.getPrivate())),
            SignatureAlgorithm.RS512
        );
        this.tokenBA = AuthTokenUtils.createToken(privateKeyB, SUBJECT_A, Optional.empty());
        this.tokenBB = AuthTokenUtils.createToken(privateKeyB, SUBJECT_B, Optional.empty());
        this.expiringTokenBA = AuthTokenUtils.createToken(privateKeyB, SUBJECT_A,
            Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3))));
        this.expiringTokenBB = AuthTokenUtils.createToken(privateKeyB, SUBJECT_B,
            Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3))));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        this.authProvider.close();
    }

    private void testAuthenticate(String token, String expectedSubject) throws Exception {
        String actualSubject = authProvider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });
        assertEquals(actualSubject, expectedSubject);
    }

    @Test
    public void testAuthenticate() throws Exception {
        testAuthenticate(tokenAA, SUBJECT_A);
        testAuthenticate(tokenAB, SUBJECT_B);
        testAuthenticate(tokenBA, SUBJECT_A);
        testAuthenticate(tokenBB, SUBJECT_B);
    }

    private void testAuthenticateAsync(String token, String expectedSubject) throws Exception {
        String actualSubject = authProvider.authenticateAsync(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        }).get();
        assertEquals(actualSubject, expectedSubject);
    }

    @Test
    public void testAuthenticateAsync() throws Exception {
        testAuthenticateAsync(tokenAA, SUBJECT_A);
        testAuthenticateAsync(tokenAB, SUBJECT_B);
        testAuthenticateAsync(tokenBA, SUBJECT_A);
        testAuthenticateAsync(tokenBB, SUBJECT_B);
    }


    private AuthenticationState newAuthState(String token, String expectedSubject) throws Exception {
        // Must pass the token to the newAuthState for legacy reasons.
        AuthenticationState authState = authProvider.newAuthState(
            AuthData.of(token.getBytes(UTF_8)),
            null,
            null
        );
        authState.authenticateAsync(AuthData.of(token.getBytes(UTF_8))).get();
        assertEquals(authState.getAuthRole(), expectedSubject);
        assertTrue(authState.isComplete());
        assertFalse(authState.isExpired());
        return authState;
    }

    private void verifyAuthStateExpired(AuthenticationState authState, String expectedSubject)
        throws Exception {
        assertEquals(authState.getAuthRole(), expectedSubject);
        assertTrue(authState.isComplete());
        assertTrue(authState.isExpired());
    }

    @Test
    public void testNewAuthState() throws Exception {
        AuthenticationState authStateAA = newAuthState(expiringTokenAA, SUBJECT_A);
        AuthenticationState authStateAB = newAuthState(expiringTokenAB, SUBJECT_B);
        AuthenticationState authStateBA = newAuthState(expiringTokenBA, SUBJECT_A);
        AuthenticationState authStateBB = newAuthState(expiringTokenBB, SUBJECT_B);

        Thread.sleep(TimeUnit.SECONDS.toMillis(6));

        verifyAuthStateExpired(authStateAA, SUBJECT_A);
        verifyAuthStateExpired(authStateAB, SUBJECT_B);
        verifyAuthStateExpired(authStateBA, SUBJECT_A);
        verifyAuthStateExpired(authStateBB, SUBJECT_B);

    }

    @Test
    public void testAuthenticateHttpRequest() throws Exception {
        HttpServletRequest requestAA = mock(HttpServletRequest.class);
        when(requestAA.getRemoteAddr()).thenReturn("127.0.0.1");
        when(requestAA.getRemotePort()).thenReturn(8080);
        when(requestAA.getHeader("Authorization")).thenReturn("Bearer " + expiringTokenAA);
        boolean doFilterAA = authProvider.authenticateHttpRequest(requestAA, null);
        assertTrue(doFilterAA);
        verify(requestAA).setAttribute(eq(AuthenticatedRoleAttributeName), eq(SUBJECT_A));
        verify(requestAA).setAttribute(eq(AuthenticatedDataAttributeName), isA(AuthenticationDataSource.class));

        HttpServletRequest requestAB = mock(HttpServletRequest.class);
        when(requestAB.getRemoteAddr()).thenReturn("127.0.0.1");
        when(requestAB.getRemotePort()).thenReturn(8080);
        when(requestAB.getHeader("Authorization")).thenReturn("Bearer " + expiringTokenAB);
        boolean doFilterAB = authProvider.authenticateHttpRequest(requestAB, null);
        assertTrue(doFilterAB);
        verify(requestAB).setAttribute(eq(AuthenticatedRoleAttributeName), eq(SUBJECT_B));
        verify(requestAB).setAttribute(eq(AuthenticatedDataAttributeName), isA(AuthenticationDataSource.class));

        HttpServletRequest requestBA = mock(HttpServletRequest.class);
        when(requestBA.getRemoteAddr()).thenReturn("127.0.0.1");
        when(requestBA.getRemotePort()).thenReturn(8080);
        when(requestBA.getHeader("Authorization")).thenReturn("Bearer " + expiringTokenBA);
        boolean doFilterBA = authProvider.authenticateHttpRequest(requestBA, null);
        assertTrue(doFilterBA);
        verify(requestBA).setAttribute(eq(AuthenticatedRoleAttributeName), eq(SUBJECT_A));
        verify(requestBA).setAttribute(eq(AuthenticatedDataAttributeName), isA(AuthenticationDataSource.class));

        HttpServletRequest requestBB = mock(HttpServletRequest.class);
        when(requestBB.getRemoteAddr()).thenReturn("127.0.0.1");
        when(requestBB.getRemotePort()).thenReturn(8080);
        when(requestBB.getHeader("Authorization")).thenReturn("Bearer " + expiringTokenBB);
        boolean doFilterBB = authProvider.authenticateHttpRequest(requestBB, null);
        assertTrue(doFilterBB);
        verify(requestBB).setAttribute(eq(AuthenticatedRoleAttributeName), eq(SUBJECT_B));
        verify(requestBB).setAttribute(eq(AuthenticatedDataAttributeName), isA(AuthenticationDataSource.class));
    }

    @Test
    public void testAuthenticateWithMultipleProviders() throws Exception {
        HttpServletRequest httpRequest = mock(HttpServletRequest.class);
        AuthenticationDataSource authenticationDataSource = mock(AuthenticationDataSource.class);

        AuthenticationProvider failingProvider = mock(AuthenticationProvider.class);
        List<AuthenticationProvider> providers = Lists.newArrayList(
                failingProvider
        );
        try (AuthenticationProvider provider = new AuthenticationProviderList(providers)) {
            provider.initialize(new ServiceConfiguration());
            RuntimeException authenticateException = new RuntimeException("authenticateException");

            when(failingProvider.authenticateAsync(authenticationDataSource))
                    .thenReturn(FutureUtil.failedFuture(authenticateException));
            when(failingProvider.authenticate(authenticationDataSource))
                    .thenThrow(authenticateException);
            assertThat(provider.authenticateAsync(authenticationDataSource))
                    .failsWithin(3, TimeUnit.SECONDS)
                    .withThrowableThat().withCause(authenticateException);
            assertThatThrownBy(() -> provider.authenticate(authenticationDataSource))
                    .isInstanceOf(AuthenticationException.class)
                    .hasCause(authenticateException);

            RuntimeException authenticateHttpRequestException = new RuntimeException("authenticateHttpRequestAsync");
            when(failingProvider.authenticateHttpRequestAsync(httpRequest, null))
                    .thenReturn(FutureUtil.failedFuture(authenticateHttpRequestException));
            when(failingProvider.authenticateHttpRequest(httpRequest, null))
                    .thenThrow(authenticateHttpRequestException);
            assertThat(provider.authenticateHttpRequestAsync(httpRequest, null))
                    .failsWithin(3, TimeUnit.SECONDS)
                    .withThrowableThat()
                    .havingCause()
                    .withCause(authenticateHttpRequestException);
            assertThatThrownBy(() -> provider.authenticateHttpRequest(httpRequest, null))
                    .isInstanceOf(AuthenticationException.class)
                    .hasCause(authenticateHttpRequestException);

            RuntimeException newAuthStateException = new RuntimeException("newAuthState");
            when(failingProvider.newAuthState(null, null, null))
                    .thenThrow(newAuthStateException);
            assertThatThrownBy(() -> provider.newAuthState(null, null, null))
                    .isInstanceOf(AuthenticationException.class)
                    .hasCause(newAuthStateException);

            RuntimeException newHttpAuthStateException = new RuntimeException("newHttpAuthState");
            when(failingProvider.newHttpAuthState(httpRequest))
                    .thenThrow(newHttpAuthStateException);
            assertThatThrownBy(() -> provider.newHttpAuthState(httpRequest))
                    .isInstanceOf(AuthenticationException.class)
                    .hasCause(newHttpAuthStateException);
        }

        AuthenticationProvider successfulProvider = mock(AuthenticationProvider.class);
        providers.add(successfulProvider);
        String subject = "test-role";

        try (AuthenticationProvider provider = new AuthenticationProviderList(providers)) {
            provider.initialize(new ServiceConfiguration());

            when(successfulProvider.authenticateAsync(authenticationDataSource))
                    .thenReturn(CompletableFuture.completedFuture(subject));
            when(successfulProvider.authenticate(authenticationDataSource))
                    .thenReturn(subject);
            assertThat(provider.authenticateAsync(authenticationDataSource))
                    .succeedsWithin(3, TimeUnit.SECONDS)
                    .matches(subject::equals);
            assertThat(provider.authenticate(authenticationDataSource))
                    .isEqualTo(subject);

            when(successfulProvider.authenticateHttpRequestAsync(httpRequest, null))
                    .thenReturn(CompletableFuture.completedFuture(true));
            when(successfulProvider.authenticateHttpRequest(httpRequest, null))
                    .thenReturn(true);
            assertThat(provider.authenticateHttpRequestAsync(httpRequest, null))
                    .succeedsWithin(3, TimeUnit.SECONDS)
                    .isEqualTo(true);
            assertThat(provider.authenticateHttpRequest(httpRequest, null))
                    .isEqualTo(true);

            AuthenticationState authenticationState = new AuthenticationState() {
                @Override
                public String getAuthRole() {
                    return subject;
                }

                @Override
                public AuthData authenticate(AuthData authData) {
                    return null;
                }

                @Override
                public AuthenticationDataSource getAuthDataSource() {
                    return null;
                }

                @Override
                public boolean isComplete() {
                    return false;
                }
            };
            when(successfulProvider.newAuthState(null, null, null))
                    .thenReturn(authenticationState);
            when(successfulProvider.newHttpAuthState(httpRequest)).thenReturn(authenticationState);
            verifyAuthenticationStateSuccess(provider.newAuthState(null, null, null), true, subject);
            verifyAuthenticationStateSuccess(provider.newAuthState(null, null, null), false, subject);
            verifyAuthenticationStateSuccess(provider.newHttpAuthState(httpRequest), true, subject);
            verifyAuthenticationStateSuccess(provider.newHttpAuthState(httpRequest), false, subject);
        }
    }

    private void verifyAuthenticationStateSuccess(AuthenticationState authState, boolean isAsync, String expectedRole)
            throws Exception {
        assertThat(authState).isNotNull();
        if (isAsync) {
            assertThat(authState.authenticateAsync(null)).succeedsWithin(3, TimeUnit.SECONDS);
        } else {
            assertThat(authState.authenticate(null)).isNull();
        }
        assertThat(authState.getAuthRole()).isEqualTo(expectedRole);
    }
}