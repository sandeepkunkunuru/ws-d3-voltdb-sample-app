/*
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it allows a single client with a small amount of
 * threads to flood VoltDB with requests, guaranteeing blazing throughput
 * performance.
 *
 * Note that this benchmark focuses on throughput performance and
 * not low latency performance.  This benchmark will likely 'firehose'
 * the database cluster (if the cluster is too slow or has too few CPUs)
 * and as a result, queue a significant amount of requests on the server
 * to maximize throughput measurement. To test VoltDB latency, run the
 * SyncBenchmark client, also found in the reviewer sample directory.
 */

package reviewer;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import reviewer.common.BookReviewsGenerator;
import reviewer.common.Constants;
import reviewer.common.ReviewerConfig;

import java.util.concurrent.CountDownLatch;

public class AsyncBenchmark extends Benchmark {
    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(ReviewerConfig config) {
        super(config);
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     */
    class ReviewerCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                long resultCode = response.getResults()[0].asScalarLong();
                if (resultCode == reviewer.procedures.Review.ERR_INVALID_BOOK) {
                    badBookReviews.incrementAndGet();
                } else if (resultCode == reviewer.procedures.Review.ERR_REVIEWER_OVER_REVIEW_LIMIT) {
                    badReviewCountReviews.incrementAndGet();
                } else {
                    assert (resultCode == reviewer.procedures.Review.REVIEW_SUCCESSFUL);
                    acceptedReviews.incrementAndGet();
                }
            } else {
                failedReviews.incrementAndGet();
            }
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        System.out.print(Constants.HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(Constants.HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        // initialize using synchronous call
        System.out.println("\nPopulating Static Tables\n");
        client.callProcedure("Initialize", config.books, Constants.BOOK_NAMES_CSV);

        System.out.print(Constants.HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(Constants.HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            // Get the next phone call
            BookReviewsGenerator.Review call = reviewsGenerator.receive();

            // asynchronously call the "Review" procedure
            client.callProcedure(new NullCallback(),
                    "Review",
                    call.email,
                    call.bookId,
                    config.maxreviews);
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();


        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            // Get the next phone call
            BookReviewsGenerator.Review call = reviewsGenerator.receive();

            // asynchronously call the "Review" procedure
            client.callProcedure(new ReviewerCallback(),
                    "Review",
                    call.email,
                    call.bookId,
                    config.maxreviews);
        }

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        client.drain();

        // print the summary results
        printResults();

        // close down the client connections
        client.close();
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     *                syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link ReviewerConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        ReviewerConfig config = new ReviewerConfig();
        config.parse(AsyncBenchmark.class.getName(), args);

        AsyncBenchmark benchmark = new AsyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
