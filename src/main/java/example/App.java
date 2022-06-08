package example;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class App {
    public static void main(String[] args) throws Exception {
        new App().run();
    }

    private void run() throws MalformedURLException, PulsarClientException, SQLException, PulsarAdminException {
        String hostName = "localhost";
        int port = 5432;
        String dbName = "postgres";
        String user = "postgres";
        String password = "PUT_PASSWORD_HERE";

        Connection cn = postgresConnection(hostName, port, dbName, user, password);
        System.out.println("Created Postgres connection");
        doSomePostgresQueries(cn);

        String serviceUrl = "https://gcp-useast4.o-ll0io.sn3.dev";
        String issuerUrl = "https://auth.sncloud-stg.dev/";
        String credentialsUrl = "file:///Users/kayjohansen/service-account/o-ll0io-admin.json";
        String audience = "urn:sn:pulsar:o-ll0io:shared";

        PulsarAdmin admin = pulsarClient(serviceUrl, issuerUrl, credentialsUrl, audience);
        System.out.println("Created Pulsar admin client");
        List<String> namespaces = admin.namespaces().getNamespaces("public");
        for (String namespace : namespaces) {
            System.out.println(namespace);
        }
        admin.close();
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

    private Connection postgresConnection(String hostName, int port, String dbName, String user, String password)
            throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s", hostName, port, dbName);
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        System.out.println(props);
        return DriverManager.getConnection(url, props);
    }

    private void doSomePostgresQueries(Connection cn) throws SQLException {
        Statement st = cn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id, name FROM books");
        while (rs.next())
        {
            System.out.print("Column 2 returned ");
            System.out.println(rs.getString(2));
        }
        rs.close();
        st.close();

        PreparedStatement pst = cn.prepareStatement("INSERT INTO books VALUES(?, ?)");
        pst.setInt(1, 6);
        pst.setString(2, "Accelerate");
        int rowsInserted = pst.executeUpdate();
        System.out.println(rowsInserted + " rows inserted");
        pst.close();
    }
}
