
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by ahmadjawid on 6/13/17.
 */

public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {

        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")).build();

        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();

        while (it.hasNext()) {
            HttpEntity entity = new NStringEntity(
                    new String(it.next().message()), ContentType.APPLICATION_JSON);

            try {
                Response indexResponse = restClient.performRequest(
                        "POST",
                        "/weather/luftdaten/",
                        Collections.<String, String>emptyMap(),
                        entity);

                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));



            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
