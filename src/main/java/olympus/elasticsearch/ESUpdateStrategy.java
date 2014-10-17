package olympus.elasticsearch;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Created by shailesh on 13/10/14.
 */
public abstract class ESUpdateStrategy {
    public abstract UpdateRequestBuilder update(UpdateRequestBuilder updateRequestBuilder, SearchHit searchHit);
    public abstract String[] getFetchList();
}
