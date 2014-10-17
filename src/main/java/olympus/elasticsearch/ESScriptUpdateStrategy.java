package olympus.elasticsearch;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.search.SearchHit;

/**
 * Created by shailesh on 13/10/14.
 */
public class ESScriptUpdateStrategy extends ESUpdateStrategy {
    private String script;

    public ESScriptUpdateStrategy(String script) {
        this.script = script;
    }

    @Override
    public UpdateRequestBuilder update(UpdateRequestBuilder updateRequestBuilder, SearchHit searchHit) {
        updateRequestBuilder.setScript(script);
        return updateRequestBuilder;
    }

    @Override
    public String[] getFetchList() {
        return null;
    }
}
