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
package org.apache.pulsar.common.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FileModifiedTimeUpdater;

/**
 * A {@link TlsMaterialSource} that produces client-side {@link ClientTlsMaterial} from a
 * {@link FileBasedTlsMaterialSource} configuration.
 *
 * <p>Each configured file is tracked with a {@link FileModifiedTimeUpdater}. {@link #getTlsMaterial()}
 * returns the same cached {@link ClientTlsMaterial} instance until one of the tracked files changes
 * on disk, at which point the material is reloaded and a new, non-equal instance is returned.
 */
public final class FileBasedClientTlsMaterialSource implements TlsMaterialSource {

    private final FileBasedTlsMaterialSource config;
    private final FileModifiedTimeUpdater[] watchedFiles;

    private ClientTlsMaterial cached;

    /**
     * Create a client material source for the supplied configuration.
     *
     * @param config the file-based configuration
     */
    public FileBasedClientTlsMaterialSource(FileBasedTlsMaterialSource config) {
        this.config = config;
        this.watchedFiles = createWatchers(config);
    }

    private static FileModifiedTimeUpdater[] createWatchers(FileBasedTlsMaterialSource config) {
        return new FileModifiedTimeUpdater[] {
                watcher(config.getTrustCertsFilePath()),
                watcher(config.getCertificateFilePath()),
                watcher(config.getKeyFilePath()),
                watcher(config.getKeyStorePath()),
                watcher(config.getTrustStorePath()),
        };
    }

    private static FileModifiedTimeUpdater watcher(String path) {
        return StringUtils.isNotBlank(path) ? new FileModifiedTimeUpdater(path) : null;
    }

    @Override
    public synchronized TlsMaterial getTlsMaterial() throws Exception {
        if (cached == null || anyFileChanged()) {
            cached = load();
        }
        return cached;
    }

    private boolean anyFileChanged() {
        boolean changed = false;
        // Always evaluate every watcher so each one refreshes its recorded modification time.
        for (FileModifiedTimeUpdater watcher : watchedFiles) {
            if (watcher != null && watcher.checkAndRefresh()) {
                changed = true;
            }
        }
        return changed;
    }

    private ClientTlsMaterial load() throws Exception {
        List<X509Certificate> trustCerts = FileBasedTlsMaterialLoader.loadTrustCerts(config);
        PrivateKey privateKey = FileBasedTlsMaterialLoader.loadPrivateKey(config);
        List<X509Certificate> certificateChain = FileBasedTlsMaterialLoader.loadCertificateChain(config);
        return DefaultClientTlsMaterial.builder()
                .trustCerts(trustCerts)
                .privateKey(privateKey)
                .keyCertChain(certificateChain)
                .trustAnyCaCert(config.isAllowInsecureConnection())
                .hostnameVerificationRequired(config.isHostnameVerificationEnabled())
                .tlsCiphers(config.getTlsCiphers().isEmpty() ? null : List.copyOf(config.getTlsCiphers()))
                .tlsProtocols(config.getTlsProtocols().isEmpty() ? null : List.copyOf(config.getTlsProtocols()))
                .build();
    }

    @Override
    public boolean isServer() {
        return false;
    }
}
