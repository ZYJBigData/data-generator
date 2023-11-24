package com.bizseer.bigdata.es.utils;

import cn.hutool.core.lang.Assert;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xuzhenchang
 */
@Slf4j
@Getter
@ToString
@SuppressWarnings("deprecation")
public class ElasticsearchClient {

    private static final String HTTPS_SCHEME = "https";

    private final String nodes;

    private String scheme;

    private final String username;

    private String password;

    private final boolean decryptEnabled;

    private final String prefix;

    private final int indexNumberOfShards;

    private final int indexNumberOfReplicas;

    private final int connectTimeout = 10_000;

    private final int socketTimeout = 60_000;

    private final int keepAlive = 10;

    private static final String PEM_SUFFIX = ".crt";

    private static final String PKCS_SUFFIX = ".p12";

    private static final String JKS_SUFFIX = ".keystore";

    public ElasticsearchClient(String nodes) {
        this.nodes = nodes;
        this.username = "elastic";
        this.password = "Bizseer@2020";
        this.decryptEnabled = false;
        this.prefix = "dataplat_meta_";
        this.indexNumberOfShards = 3;
        this.indexNumberOfReplicas = 1;
    }

    public RestHighLevelClient getRestHighLevelClient() {
        List<HttpHost> httpHosts = new ArrayList<>();
        List<String> nodeList = Arrays.asList(nodes.split(","));
        log.info("nodeList:{}", nodeList);
        nodeList.forEach(node -> {
            String[] address = node.split(":");
            Assert.state(address.length == 2, "es address Must be defined as 'host:port'");
            httpHosts.add(new HttpHost(address[0], Integer.parseInt(address[1]), scheme));
        });
        RestClientBuilder.RequestConfigCallback requestConfigCallback = requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeout);
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            requestConfigBuilder.setConnectionRequestTimeout(connectTimeout);
            return requestConfigBuilder;
        };
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    Optional.ofNullable(buildCredentialsProvider()).ifPresent(httpClientBuilder::setDefaultCredentialsProvider);
                    httpClientBuilder.setKeepAliveStrategy(new CustomKeepAliveStrategy());
                    return httpClientBuilder;
                }).setRequestConfigCallback(requestConfigCallback);
        return new RestHighLevelClient(builder);
    }

    private class CustomKeepAliveStrategy extends DefaultConnectionKeepAliveStrategy {

        private CustomKeepAliveStrategy() {
            super();
        }

        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            long keepAliveDuration = super.getKeepAliveDuration(response, context);
            // <0 无限期keepalive，替换成一个默认时间，默认为10分钟
            if (keepAliveDuration < 0) {
                return TimeUnit.MINUTES.toMillis(keepAlive);
            }
            return keepAliveDuration;
        }
    }

    private CredentialsProvider buildCredentialsProvider() {
        if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
            return null;
        }
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        return credentialsProvider;
    }

    private SSLContext buildSSLContext(String filePath) {
        if (Strings.isNullOrEmpty(filePath)) {
            return null;
        }
        try (InputStream inputStream = getStream(filePath)) {
            if (inputStream == null) {
                return null;
            } else if (filePath.endsWith(PEM_SUFFIX)) {
                return buildSSLContextCrt(inputStream);
            } 
//            else if (filePath.endsWith(PKCS_SUFFIX)) {
//                return buildSSLContextPkcs(inputStream);
//            } else if (filePath.endsWith(JKS_SUFFIX)) {
//                return buildSSLContextJks(inputStream);
//            }
        } catch (IOException e) {
            log.error("[ElasticsearchClient] occurs error when exec buildSSLContext method, message: {}", e.getMessage(), e);
        }
        return null;
    }

    //
    private InputStream getStream(String filePath) {
        Path caCertificatePath = Paths.get(filePath);
//        return ResourceUtil.getStream(filePath);
        try {
            return Files.newInputStream(caCertificatePath);
        } catch (IOException e) {
            log.info("Load certificate file failed, reason: {}", e.getMessage());
        }
        return null;
    }

    /**
     * pem模式，crt后缀
     */
    private SSLContext buildSSLContextCrt(InputStream is) {
        try {
            String certificateType = "X.509";
            String keystoreType = "pkcs12";
            String alias = "ca";
            CertificateFactory factory = CertificateFactory.getInstance(certificateType);
            Certificate trustedCa = factory.generateCertificate(is);
            KeyStore trustStore = KeyStore.getInstance(keystoreType);
            trustStore.load(null, null);
            trustStore.setCertificateEntry(alias, trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException | KeyManagementException e) {
            log.error("[ElasticsearchClient] occurs error when exec buildSSLContextCrt method, message: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * pkcs12模式，p12后缀
     */
//    private SSLContext buildSSLContextPkcs(InputStream is) {
//        try {
//            String keystoreType = "pkcs12";
//            KeyStore keyStore = KeyStore.getInstance(keystoreType);
//            keyStore.load(is, esKeyStorePass.toCharArray());
//            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
//                    .loadTrustMaterial(keyStore, null);
//            return sslContextBuilder.build();
//        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException | KeyManagementException e) {
//            log.error("[ElasticsearchClient] occurs error when exec buildSSLContextPkcs method, message: {}", e.getMessage(), e);
//        }
//        return null;
//    }

    /**
     * jks模式，keystore后缀
     */
//    private SSLContext buildSSLContextJks(InputStream is) {
//        try {
//            String keystoreType = "jks";
//            KeyStore keyStore = KeyStore.getInstance(keystoreType);
//            keyStore.load(is, esKeyStorePass.toCharArray());
//            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
//                    .loadTrustMaterial(keyStore, null);
//            return sslContextBuilder.build();
//        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException | KeyManagementException e) {
//            log.error("[ElasticsearchClient] occurs error when exec buildSSLContextJks method, message: {}", e.getMessage(), e);
//        }
//        return null;
//    }

}
