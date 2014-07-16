/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the VoltDB JDBC client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package reviewer;

import org.voltdb.client.ProcCallException;
import org.voltdb.jdbc.IVoltDBConnection;
import reviewer.common.BookReviewsGenerator;
import reviewer.common.Constants;
import reviewer.common.ReviewerConfig;

import java.io.IOException;
import java.sql.*;

public class JDBCBenchmark extends Benchmark {
    // Reference to the database connection we will use
    Connection client;

    /**
     * Constructor for benchmark instance. Configures VoltDB client and prints
     * configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public JDBCBenchmark(ReviewerConfig config) {
        super(config);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     *                syntax (where :port is optional).
     * @throws InterruptedException   if anything bad happens with the threads.
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    void connect(String servers) throws InterruptedException,
            ClassNotFoundException, SQLException {
        System.out.println("Connecting to VoltDB...");

        // We need only do this once, to "hot cache" the JDBC driver reference
        // so the JVM may realize it's there.
        Class.forName("org.voltdb.jdbc.Driver");

        // Prepare the JDBC URL for the VoltDB driver
        String url = "jdbc:voltdb://" + config.servers;

        client = DriverManager.getConnection(url, "", "");

        periodicStatsContext = ((IVoltDBConnection) client)
                .createStatsContext();
        fullStatsContext = ((IVoltDBConnection) client).createStatsContext();
    }


    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     */
    class ReviewerThread implements Runnable {

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Get the next review
                BookReviewsGenerator.Review call = reviewsGenerator.receive();

                // synchronously call the "Review" procedure
                try {
                    final PreparedStatement reviewCS = client
                            .prepareCall("{call Review(?,?,?,?)}");
                    reviewCS.setString(1, call.email);
                    reviewCS.setString(2, call.review);
                    reviewCS.setInt(3, call.bookId);
                    reviewCS.setLong(4, config.maxreviews);
                } catch (Exception e) {
                }
            }

            while (benchmarkComplete.get() == false) {
                // Get the next review
                BookReviewsGenerator.Review call = reviewsGenerator.receive();

                // synchronously call the "Review" procedure
                try {

                    final PreparedStatement reviewCS = client
                            .prepareCall("{call Review(?,?,?,?)}");
                    reviewCS.setString(1, call.email);
                    reviewCS.setString(2, call.review);
                    reviewCS.setInt(3, call.bookId);
                    reviewCS.setLong(4, config.maxreviews);

                    try {
                        reviewCS.executeUpdate();
                        acceptedReviews.incrementAndGet();
                    } catch (Exception x) {
                        badReviewCountReviews.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedReviews.incrementAndGet();
                }
            }

        }

    }

    /**
     * Core benchmark code. Connect. Initialize. Run the loop. Cleanup. Print
     * Results.
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
        // Initialize the application
        System.out.println("\nPopulating Static Tables\n");
        final PreparedStatement initializeCS = client
                .prepareCall("{call Initialize(?,?)}");
        initializeCS.setInt(1, config.books);
        initializeCS.setString(2, Constants.BOOK_NAMES_CSV);
        initializeCS.executeUpdate();

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
        // client.drain();

        // join on the threads
        for (Thread t : reviewrThreads) {
            t.join();
        }

        // print the summary results
        printResults();

        // close down the client connections
        client.close();
    }

    public void getResults() throws IOException, ProcCallException {
        try {
            final PreparedStatement reviewCS = client
                    .prepareCall("{call Results()}");

            try {
                ResultSet result = reviewCS.executeQuery();

                System.out.println("Book Name\t\tReviews Received");
                while (result.next()) {
                    System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
                }
                System.out.printf("\nThe Winner is: %s\n\n", result.getString(0));
                acceptedReviews.incrementAndGet();
            } catch (Exception x) {
                badReviewCountReviews.incrementAndGet();
            }
        } catch (Exception e) {
            failedReviews.incrementAndGet();
        }
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
        config.parse(JDBCBenchmark.class.getName(), args);

        JDBCBenchmark benchmark = new JDBCBenchmark(config);
        benchmark.runBenchmark();
    }
}
