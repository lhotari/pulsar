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
package org.apache.pulsar.common.util.tls;

import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.TlsHostnameVerifier;

/**
 * Resolves the JCA/JCE security providers Pulsar relies on: the Bouncy Castle provider (FIPS or non-FIPS)
 * and the optional Conscrypt (OpenSSL) provider. This is the single provider-resolution primitive extracted
 * from the former {@code SecurityUtility} grab-bag (PIP-478); loading either provider (as a side effect of
 * class initialization, via {@link #BC_PROVIDER} / {@link #CONSCRYPT_PROVIDER}) calls
 * {@code Security.addProvider} so the provider is installed process-wide.
 */
@CustomLog
public final class JcaProviders {

    public static final Provider BC_PROVIDER = getProvider();
    public static final String BC_FIPS_PROVIDER_CLASS = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
    public static final String BC_NON_FIPS_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider";
    public static final String CONSCRYPT_PROVIDER_CLASS = "org.conscrypt.OpenSSLProvider";
    public static final Provider CONSCRYPT_PROVIDER = loadConscryptProvider();

    // Security.getProvider("BC") / Security.getProvider("BCFIPS").
    // also used to get Factories. e.g. CertificateFactory.getInstance("X.509", "BCFIPS")
    public static final String BC_FIPS = "BCFIPS";
    public static final String BC = "BC";

    private JcaProviders() {
    }

    public static boolean isBCFIPS() {
        return BC_PROVIDER.getClass().getCanonicalName().equals(BC_FIPS_PROVIDER_CLASS);
    }

    /**
     * Get Bouncy Castle provider, and call Security.addProvider(provider) if success.
     *  1. try get from classpath.
     *  2. try get from Nar.
     */
    public static Provider getProvider() {
        boolean isProviderInstalled =
                Security.getProvider(BC) != null || Security.getProvider(BC_FIPS) != null;

        if (isProviderInstalled) {
            Provider provider = Security.getProvider(BC) != null
                    ? Security.getProvider(BC)
                    : Security.getProvider(BC_FIPS);
            log.debug().attr("provider", provider.getName()).log("Already instantiated Bouncy Castle provider");
            return provider;
        }

        // Not installed, try load from class path
        try {
            return getBCProviderFromClassPath();
        } catch (Exception e) {
            log.warn().exception(e)
                    .log("Not able to get Bouncy Castle provider for both FIPS and Non-FIPS from class path");
            throw new RuntimeException(e);
        }
    }

    private static Provider loadConscryptProvider() {
        Class<?> conscryptClazz;

        try {
            conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
            conscryptClazz.getMethod("checkAvailability").invoke(null);
        } catch (Throwable e) {
            if (e instanceof ClassNotFoundException) {
                log.debug("Conscrypt isn't available in the classpath. Using JDK default security provider.");
            } else if (e.getCause() instanceof UnsatisfiedLinkError) {
                log.debug().attr("os", System.getProperty("os.name")).attr("arch", System.getProperty("os.arch"))
                        .log("Conscrypt isn't available. Using JDK default security provider");
            } else {
                log.debug().attr("cause", e.getCause()).attr("reason", e.getMessage())
                        .log("Conscrypt isn't available. Using JDK default security provider");
            }
            return null;
        }

        Provider provider;
        try {
            provider = (Provider) Class.forName(CONSCRYPT_PROVIDER_CLASS).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            log.debug().attr("class", CONSCRYPT_PROVIDER_CLASS).exception(e)
                    .log("Unable to get security provider");
            return null;
        }

        // Configure Conscrypt's default hostname verifier to use Pulsar's TlsHostnameVerifier which
        // is more relaxed than the Conscrypt HostnameVerifier checking for RFC 2818 conformity.
        //
        // Certificates used in Pulsar docs and examples aren't strictly RFC 2818 compliant since they use the
        // deprecated way of specifying the hostname in the CN field of the subject DN of the certificate.
        // RFC 2818 recommends the use of SAN (subjectAltName) extension for specifying the hostname in the dNSName
        // field of the subjectAltName extension.
        //
        // Conscrypt's default HostnameVerifier has dropped support for the deprecated method of specifying the hostname
        // in the CN field. Pulsar's TlsHostnameVerifier continues to support the CN field.
        //
        // more details of Conscrypt's hostname verification:
        // https://github.com/google/conscrypt/blob/master/IMPLEMENTATION_NOTES.md#hostname-verification
        // there's a bug in Conscrypt while setting a custom HostnameVerifier,
        // https://github.com/google/conscrypt/issues/1015 and therefore this solution alone
        // isn't sufficient to configure Conscrypt's hostname verifier. The method processConscryptTrustManager
        // contains the workaround.
        try {
            HostnameVerifier hostnameVerifier = new TlsHostnameVerifier();
            Object wrappedHostnameVerifier = conscryptClazz
                    .getMethod("wrapHostnameVerifier",
                            new Class<?>[]{HostnameVerifier.class}).invoke(null, hostnameVerifier);
            Method setDefaultHostnameVerifierMethod =
                    conscryptClazz
                            .getMethod("setDefaultHostnameVerifier",
                                    new Class<?>[]{Class.forName("org.conscrypt.ConscryptHostnameVerifier")});
            setDefaultHostnameVerifierMethod.invoke(null, wrappedHostnameVerifier);
        } catch (Exception e) {
            log.warn().exception(e).log("Unable to set default hostname verifier for Conscrypt");
        }

        Security.addProvider(provider);
        log.debug().attr("provider", provider.getName()).attr("class", CONSCRYPT_PROVIDER_CLASS)
                .log("Added security provider");
        return provider;
    }

    /**
     * Get Bouncy Castle provider from classpath, and call Security.addProvider.
     * Throw Exception if failed.
     */
    private static Provider getBCProviderFromClassPath() throws Exception {
        Class<?> clazz;
        try {
            // prefer non FIPS, for backward compatibility concern.
            clazz = Class.forName(BC_NON_FIPS_PROVIDER_CLASS);
        } catch (ClassNotFoundException cnf) {
            log.warn().attr("nonFipsClass", BC_NON_FIPS_PROVIDER_CLASS).attr("fipsClass", BC_FIPS_PROVIDER_CLASS)
                    .log("Not able to get Bouncy Castle provider, try to get FIPS provider");
            // attempt to use the FIPS provider.
            clazz = Class.forName(BC_FIPS_PROVIDER_CLASS);
        }

        Provider provider = (Provider) clazz.getDeclaredConstructor().newInstance();
        Security.addProvider(provider);
        log.debug().attr("provider", provider.getName())
                .log("Found and Instantiated Bouncy Castle provider in classpath");
        return provider;
    }

    /**
     * Resolve a security {@link Provider} by name, falling back to the default {@code TLS}
     * {@code SSLContext} provider when the name is blank or unknown.
     */
    static Provider resolveProvider(String providerName) throws NoSuchAlgorithmException {
        Provider provider = null;
        if (!StringUtils.isEmpty(providerName)) {
            provider = Security.getProvider(providerName);
        }

        if (provider == null) {
            provider = SSLContext.getDefault().getProvider();
        }

        return provider;
    }

    /**
     * Conscrypt {@link TrustManager} instances are configured to use Pulsar's
     * {@link TlsHostnameVerifier}. This is a workaround for https://github.com/google/conscrypt/issues/1015
     * when Conscrypt / OpenSSL is used as the TLS security provider.
     *
     * @param trustManagers the array of TrustManager instances to process.
     * @return same instance passed as parameter
     */
    static TrustManager[] processConscryptTrustManagers(TrustManager[] trustManagers) {
        for (TrustManager trustManager : trustManagers) {
            processConscryptTrustManager(trustManager);
        }
        return trustManagers;
    }

    // workaround https://github.com/google/conscrypt/issues/1015
    private static void processConscryptTrustManager(TrustManager trustManager) {
        if (trustManager.getClass().getName().equals("org.conscrypt.TrustManagerImpl")) {
            try {
                Class<?> conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
                Object hostnameVerifier = conscryptClazz.getMethod("getHostnameVerifier",
                        new Class<?>[]{TrustManager.class}).invoke(null, trustManager);
                if (hostnameVerifier == null) {
                    Object defaultHostnameVerifier = conscryptClazz.getMethod("getDefaultHostnameVerifier",
                            new Class<?>[]{TrustManager.class}).invoke(null, trustManager);
                    if (defaultHostnameVerifier != null) {
                        conscryptClazz.getMethod("setHostnameVerifier", new Class<?>[]{
                                TrustManager.class,
                                Class.forName("org.conscrypt.ConscryptHostnameVerifier")
                        }).invoke(null, trustManager, defaultHostnameVerifier);
                    }
                }
            } catch (ReflectiveOperationException e) {
                log.warn().exception(e)
                        .log("Unable to set hostname verifier for Conscrypt TrustManager implementation");
            }
        }
    }
}
