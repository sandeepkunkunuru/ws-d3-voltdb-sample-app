/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package reviewer;

import org.voltdb.client.ClientResponse;
import reviewer.common.BookReviewsGenerator;
import reviewer.common.Constants;
import reviewer.common.ReviewerConfig;

import java.util.concurrent.CountDownLatch;

public class SyncBenchmark extends Benchmark {

    public SyncBenchmark(ReviewerConfig config) {
        super(config);
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     */
    class ReviewerThread implements Runnable {

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Get the next phone call
                BookReviewsGenerator.Review call = reviewsGenerator.receive();

                // synchronously call the "Review" procedure
                try {
                    client.callProcedure("Review", call.email,
                            call.bookId, config.maxreviews);
                } catch (Exception e) {
                }
            }

            while (benchmarkComplete.get() == false) {
                // Get the next phone call
                BookReviewsGenerator.Review call = reviewsGenerator.receive();

                // synchronously call the "Review" procedure
                try {
                    ClientResponse response = client.callProcedure("Review",
                            call.email, call.review,
                            call.bookId,
                            config.maxreviews);

                    long resultCode = response.getResults()[0].asScalarLong();
                    if (resultCode == reviewer.procedures.Review.ERR_INVALID_BOOK) {
                        badBookReviews.incrementAndGet();
                    } else if (resultCode == reviewer.procedures.Review.ERR_REVIEWER_OVER_REVIEW_LIMIT) {
                        badReviewCountReviews.incrementAndGet();
                    } else {
                        assert (resultCode == reviewer.procedures.Review.REVIEW_SUCCESSFUL);
                        acceptedReviews.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedReviews.incrementAndGet();
                }
            }

        }

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
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
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

        // create/start the requested number of threads
        Thread[] reviewrThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            reviewrThreads[i] = new Thread(new ReviewerThread());
            reviewrThreads[i].start();
        }

        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);

        // signal to threads to end the warmup phase
        warmupComplete.set(true);

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested warmup time
        System.out.println("\nRunning benchmark...");
        Thread.sleep(1000l * config.duration);

        // stop the threads
        benchmarkComplete.set(true);

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        client.drain();

        // join on the threads
        for (Thread t : reviewrThreads) {
            t.join();
        }

        // print the summary results
        printResults();

        // close down the client connections
        client.close();
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
        config.parse(SyncBenchmark.class.getName(), args);

        SyncBenchmark benchmark = new SyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
