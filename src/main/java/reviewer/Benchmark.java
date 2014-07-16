/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Sandeep Kunkunuru
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package reviewer;

import org.voltdb.VoltTable;
import org.voltdb.client.*;
import reviewer.common.BookReviewsGenerator;
import reviewer.common.Constants;
import reviewer.common.ReviewerConfig;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sandeep on 7/16/14.
 */
public abstract class Benchmark {

    // Reference to the database connection we will use
    protected Client client;

    // validated command line configuration
    protected ReviewerConfig config;
    protected AtomicLong badBookReviews = new AtomicLong(0);
    protected AtomicLong badReviewCountReviews = new AtomicLong(0);
    // reviewer benchmark state
    protected AtomicLong acceptedReviews = new AtomicLong(0);
    protected AtomicLong failedReviews = new AtomicLong(0);
    // Benchmark start time
    protected long benchmarkStartTS;
    // Timer for periodic stats printing
    protected Timer timer;
    // Email generator
    protected BookReviewsGenerator reviewsGenerator;

    // Flags to tell the worker threads to stop or go
    protected AtomicBoolean warmupComplete = new AtomicBoolean(false);
    protected AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // Statistics manager objects from the client
    protected ClientStatsContext periodicStatsContext;
    protected ClientStatsContext fullStatsContext;

    public Benchmark(ReviewerConfig config) {
        this.config = config;

        reviewsGenerator = new BookReviewsGenerator(config.books);

        System.out.print(Constants.HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(Constants.HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if (config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
    }

    public abstract void runBenchmark() throws Exception;

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                Constants.HORIZONTAL_RULE +
                " Voting Results\n" +
                Constants.HORIZONTAL_RULE +
                "\nA total of %d reviews were received...\n" +
                " - %,9d Accepted\n" +
                " - %,9d Rejected (Invalid Book)\n" +
                " - %,9d Rejected (Maximum Review Count Reached)\n" +
                " - %,9d Failed (Transaction Error)\n\n";
        System.out.printf(display, stats.getInvocationsCompleted(),
                acceptedReviews.get(), badBookReviews.get(),
                badReviewCountReviews.get(), failedReviews.get());

        getResults();

        // 3. Performance statistics
        System.out.print(Constants.HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(Constants.HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        if (this.config.latencyreport) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
            System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
            System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
            System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
            System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
            System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
            System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
            System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

            System.out.print("\n" + Constants.HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(Constants.HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + Constants.HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(Constants.HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }
        // 4. Write stats to file if requested
        if (!"".equals(config.statsfile.trim())) client.writeSummaryCSV(stats, config.statsfile);
    }

    public void getResults() throws IOException, ProcCallException {
        // 2. Voting results
        VoltTable result = client.callProcedure("Results").getResults()[0];

        System.out.println("Book Name\t\tReviews Received");
        while (result.advanceRow()) {
            System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
        }
        System.out.printf("\nThe Winner is: %s\n\n", result.fetchRow(0).getString(0));
    }


    /**
     * Provides a callback to be notified on node failure. This example only
     * logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port,
                                   int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if (!benchmarkComplete.get()) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname,
                        port);
            }
        }
    }

    /**
     * Create a Timer task to display performance data on the Review procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() {
                printStatistics();
            }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                config.displayinterval * 1000,
                config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if (this.config.latencyreport) {
            System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                    stats.kPercentileLatencyAsDouble(0.95));
        }
        System.out.printf("\n");
    }
}
