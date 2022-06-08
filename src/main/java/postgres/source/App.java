package postgres.source;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;

public class App {
    public static void main(String[] args) throws Exception {
        new App().run();
    }

    private void run() throws MalformedURLException, PulsarClientException {
        String serviceUrl = "https://gcp-useast4.o-ll0io.sn3.dev";
        String issuerUrl = "https://auth.sncloud-stg.dev/";
        String credentialsUrl = "file:///Users/kayjohansen/service-account/o-ll0io-admin.json";
        String audience = "urn:sn:pulsar:o-ll0io:shared";

        PulsarAdmin admin = pulsarClient(serviceUrl, issuerUrl, credentialsUrl, audience);
        System.out.println("Created Pulsar admin client");
    }

    private PulsarAdmin pulsarClient(String serviceUrl, String issuerUrl, String credentialsUrl, String audience)
            throws PulsarClientException, MalformedURLException {

        final Authentication credentials = getOauthCredentials(issuerUrl, credentialsUrl, audience);

        return PulsarAdmin.builder()
                .serviceHttpUrl(serviceUrl)
                .authentication(credentials)
                .build();
    }

    private Authentication getOauthCredentials(String issuerUrl, String credentialsUrl, String audience)
            throws MalformedURLException {

        return AuthenticationFactoryOAuth2.clientCredentials(
                new URL(issuerUrl),
                new URL(credentialsUrl),
                audience);
    }
}
