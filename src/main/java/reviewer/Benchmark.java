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

import common.BookReviewsGenerator;
import common.Constants;
import common.ReviewerConfig;
import org.voltdb.client.*;
import util.StdOut;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sandeep on 7/16/14.
 */
public abstract class Benchmark {
    // validated command line configuration
    public ReviewerConfig config;
    public AtomicLong badBookReviews = new AtomicLong(0);
    public AtomicLong badReviewCountReviews = new AtomicLong(0);
    // reviewer benchmark state
    public AtomicLong acceptedReviews = new AtomicLong(0);
    public AtomicLong failedReviews = new AtomicLong(0);
    // Benchmark start time
    public long benchmarkStartTS;
    // Timer for periodic stats printing
    public Timer timer;
    // Email generator
    public BookReviewsGenerator reviewsGenerator;

    // Flags to tell the worker threads to stop or go
    public AtomicBoolean warmupComplete = new AtomicBoolean(false);
    public AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // Statistics manager objects from the client
    public ClientStatsContext periodicStatsContext;
    public ClientStatsContext fullStatsContext;

    public Benchmark(ReviewerConfig config) {
        this.config = config;

        reviewsGenerator = new BookReviewsGenerator(config.books);

        StdOut.print(Constants.HORIZONTAL_RULE);
        StdOut.println(" Command Line Configuration");
        StdOut.println(Constants.HORIZONTAL_RULE);
        StdOut.println(config.getConfigDumpString());
        if (config.latencyreport) {
            StdOut.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
    }

    public abstract void runBenchmark() throws Exception;


    /**
     * Prints the results of the simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. results and performance statistics
        String display = "\n" +
                Constants.HORIZONTAL_RULE +
                " Results\n" +
                Constants.HORIZONTAL_RULE +
                "\nA total of %d reviews were received...\n" +
                " - %,9d Accepted\n" +
                " - %,9d Rejected (Invalid Book)\n" +
                " - %,9d Rejected (Maximum Review Count Reached)\n" +
                " - %,9d Failed (Transaction Error)\n\n";
        StdOut.printf(display, stats.getInvocationsCompleted(),
                acceptedReviews.get(), badBookReviews.get(),
                badReviewCountReviews.get(), failedReviews.get());

        getResults();

        // 3. Performance statistics
        StdOut.print(Constants.HORIZONTAL_RULE);
        StdOut.println(" Client Workload Statistics");
        StdOut.println(Constants.HORIZONTAL_RULE);

        StdOut.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        if (this.config.latencyreport) {
            StdOut.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            StdOut.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
            StdOut.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
            StdOut.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
            StdOut.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
            StdOut.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
            StdOut.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
            StdOut.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
            StdOut.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
            StdOut.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

            StdOut.print("\n" + Constants.HORIZONTAL_RULE);
            StdOut.println(" System Server Statistics");
            StdOut.println(Constants.HORIZONTAL_RULE);
            StdOut.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            StdOut.print("\n" + Constants.HORIZONTAL_RULE);
            StdOut.println(" Latency Histogram");
            StdOut.println(Constants.HORIZONTAL_RULE);
            StdOut.println(stats.latencyHistoReport());
        }

        getSummaryCSV();
    }

    protected abstract void getSummaryCSV() throws IOException;

    public abstract void getResults() throws IOException, ProcCallException;

    /**
     * Provides a callback to be notified on node failure. This example only
     * logs the event.
     */
    public class StatusListener extends ClientStatusListenerExt {
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

        StdOut.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        StdOut.printf("Throughput %d/s, ", stats.getTxnThroughput());
        StdOut.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if (this.config.latencyreport) {
            StdOut.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                    stats.kPercentileLatencyAsDouble(0.95));
        }
        StdOut.printf("\n");
    }
}
