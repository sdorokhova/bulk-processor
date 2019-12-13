import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;

public class BulkProcessorTest {

  private RestHighLevelClient client;

  @Before
  public void before() {
    client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
  }

  @Test
  public void test() {
//    BulkProcessor bulkProcessor = new BulkProcessor.Builder()
  }

}
