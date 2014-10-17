package olympus.elasticsearch;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Created by shailesh on 13/10/14.
 */
public class ESDocUpdateStrategy extends ESUpdateStrategy {
    private String[] updateFields;
    private ESDocUpdater esDocUpdater;

    public ESDocUpdateStrategy(String[] updateFields, ESDocUpdater esDocUpdater) {
        this.updateFields = updateFields;
        this.esDocUpdater = esDocUpdater;
    }

    @Override
    public UpdateRequestBuilder update(UpdateRequestBuilder updateRequestBuilder, SearchHit searchHit) {
        updateRequestBuilder.setDoc(esDocUpdater.update(searchHit.getSource()));
        return updateRequestBuilder;
    }

    @Override
    public String[] getFetchList() {
        return updateFields;
    }
}
