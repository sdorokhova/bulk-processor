import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;

public class BulkProcessorTest {

  private static final Logger logger = LogManager.getLogger(BulkProcessorTest.class);

  private RestHighLevelClient client;
  private BulkProcessor bulkProcessor;

  @Before
  public void before() {
    client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));


    BulkProcessor.Listener listener = new BulkProcessor.Listener() {

      @Override
      public void beforeBulk(long l, BulkRequest bulkRequest) {
        logger.info("beforeBulk l: " + l + ", bulkRequest: " + bulkRequest);
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        logger.info("afterBulk l: " + l + ", bulkRequest: " + bulkRequest + ", bulkResponse: " + bulkResponse);

      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, java.lang.Throwable throwable) {
        logger.info("afterBulk l: " + l + ", bulkRequest: " + bulkRequest + ", throwable: " + throwable);
      }
    };

    BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkRequestActionListenerBiConsumer = (request, bulkListener) ->
    {
      try {
        client.bulk(request, RequestOptions.DEFAULT);
      } catch (Exception e) {
        logger.error(e.getMessage());
        throw new EsRejectedExecutionException(e.getMessage());
      }
    };
    BackoffPolicy infiniteRetry = new BackoffPolicy() {
      @Override
      public Iterator<TimeValue> iterator() {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public TimeValue next() {
            return TimeValue.timeValueMillis(500);
          }
        };
      }
    };
    bulkProcessor =
        BulkProcessor.builder(bulkRequestActionListenerBiConsumer, listener)
            .setBulkActions(10)
//            .setBackoffPolicy(infiniteRetry)
            .build();

  }

  @Test
  public void test() throws InterruptedException {
    for (int i = 0; i<10; i++) {
      bulkProcessor.add(new IndexRequest("test").id(String.valueOf(i)).source("{\"var" + i + "\":\"value " + i + "\"}",
          XContentType.JSON));
    }
    for (int i = 10; i<20; i++) {
      bulkProcessor.add(new IndexRequest("test")
          .id(String.valueOf(i))
          .type("_doc")
          .source("{\"var" + i + "\":\"value " + i + "\"}", XContentType.JSON));
    }
    bulkProcessor.awaitClose(5, TimeUnit.SECONDS);
  }

}
