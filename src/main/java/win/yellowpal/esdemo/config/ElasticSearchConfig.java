package win.yellowpal.esdemo.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class ElasticSearchConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {

        String ip = "127.0.0.1";
        InetAddress address = InetAddress.getByName(ip);
        TransportAddress node1 = new TransportAddress(address,9300);

        Settings settings = Settings.builder().put("cluster.name","Yellow-test").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node1);

        return  client;
    }
}
