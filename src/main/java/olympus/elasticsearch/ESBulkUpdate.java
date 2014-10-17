package olympus.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by shailesh on 23/09/14.
 */
public class ESBulkUpdate {
    private String index;
    private String type;
    private String query;
    private int bulkSize;
    private int scrollTimeoutInMillis;
    private ESUpdateStrategy esUpdateStrategy;
    private Client esClient;
    private SearchResponse scrollResp;

    public ESBulkUpdate(String hostname, int port, String index, String type, String query, int bulkSize,
                        int scrollTimeoutInMillis, ESUpdateStrategy esUpdateStrategy) {
        this.index = index;
        this.type = type;
        this.query = query;
        this.bulkSize = bulkSize;
        this.scrollTimeoutInMillis = scrollTimeoutInMillis;
        this.esUpdateStrategy = esUpdateStrategy;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();
        this.esClient = new TransportClient(settings).addTransportAddress(
                new InetSocketTransportAddress(hostname, port));
    }

    public void execute() {
        scrollResp = esClient.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.queryString(query))
                .setSize(bulkSize)
                .setFetchSource(esUpdateStrategy.getFetchList(), null)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(scrollTimeoutInMillis))
                .execute()
                .actionGet();

        System.out.println("Total records : " + scrollResp.getHits().getTotalHits());

        while (true) {
            scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(scrollTimeoutInMillis))
                    .execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
            bulkUpdate();
        }

    }

    private void bulkUpdate() {
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();

        for (SearchHit hit : scrollResp.getHits()) {
            UpdateRequestBuilder updateRequestBuilder = esClient.prepareUpdate(hit.index(), hit.type(), hit.id());

            esUpdateStrategy.update(updateRequestBuilder, hit);

            bulkRequest.add(updateRequestBuilder);
            if (bulkRequest.numberOfActions() % bulkSize == 0) {
                executeBulk(bulkRequest);
                bulkRequest = esClient.prepareBulk();
            }
        }
        executeBulk(bulkRequest);
    }


    private void executeBulk(BulkRequestBuilder bulkRequest) {
        if (bulkRequest.numberOfActions() <= 0)
            return;
        System.out.println("Executing bulk of " + bulkRequest.numberOfActions());
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures())
            System.out.println(bulkResponse.buildFailureMessage());
    }

    class Builder {
        private static final int DEFAULT_SCROLL_TIMEOUT_MILLIS = 60000;
        private static final int DEFAULT_PORT = 9300;
        private static final int DEFAULT_BULK_SIZE = 200;

        private String hostname;
        private Integer port;
        private String index;
        private String type;
        private String query;
        private Integer bulkSize;
        private Integer scrollTimeoutInMillis;
        private ESUpdateStrategy esUpdateStrategy;

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder bulkSize(Integer bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        public Builder scrollTimeoutInMillis(Integer scrollTimeoutInMillis) {
            this.scrollTimeoutInMillis = scrollTimeoutInMillis;
            return this;
        }

        public Builder esUpdateParamSetter(ESUpdateStrategy esUpdateStrategy) {
            this.esUpdateStrategy = esUpdateStrategy;
            return this;
        }

        public ESBulkUpdate build() {
            if (null == scrollTimeoutInMillis)
                scrollTimeoutInMillis = DEFAULT_SCROLL_TIMEOUT_MILLIS;
            if (null == port)
                port = DEFAULT_PORT;
            if (null == bulkSize)
                bulkSize = DEFAULT_BULK_SIZE;
            return new ESBulkUpdate(hostname, port, index, type, query, bulkSize, scrollTimeoutInMillis,
                    esUpdateStrategy);
        }

    }

}
