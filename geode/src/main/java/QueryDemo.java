package org.apache.geode_examples.queries;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;


public class QueryDemo {
    static String REGIONNAME = "hello";
    static String QUERY1 = "SELECT DISTINCT * FROM /" + REGIONNAME;


    public static void main(String[] args) throws NameResolutionException, TypeMismatchException,
            QueryInvocationTargetException, FunctionDomainException {
        // connect to the locator using default port 10334
        ClientCache cache = new ClientCacheFactory().addPoolLocator("127.0.0.1", 10334)
                .set("log-level", "WARN").create();

        // create a region on the server
        Region<String, String> region =
                cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
                        .create(REGIONNAME);

        // count the values in the region
        int inserted = region.keySetOnServer().size();
        System.out.println(String.format("Counted %d keys in region %s", inserted, region.getName()));

        // fetch and print all values in the region (without using a query)
        region.keySetOnServer().forEach(key -> System.out.println(region.get(key)));

        // do a set of queries, printing the results of each query
        doQueries(cache);

        cache.close();
    }

    // Demonstrate querying using the API by doing 3 queries.
    public static void doQueries(ClientCache cache) throws NameResolutionException,
            TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
        QueryService queryService = cache.getQueryService();

        // Query for every entry in the region, and print query results.
        System.out.println("\nExecuting query: " + QUERY1);
        SelectResults<String> results =
                (SelectResults<String>) queryService.newQuery(QUERY1).execute();
        printSetOfEmployees(results);

    }

    private static void printSetOfEmployees(SelectResults<String> results) {
        System.out.println("Query returned " + results + " results.");
    }
}