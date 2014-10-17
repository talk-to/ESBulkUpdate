package olympus.elasticsearch;

import java.util.Map;

/**
 * Created by shailesh on 13/10/14.
 */
public interface ESDocUpdater {
    Map<String, Object> update(Map<String, Object> document);
}
