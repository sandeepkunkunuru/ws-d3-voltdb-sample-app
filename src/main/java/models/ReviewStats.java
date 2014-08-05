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

package models;

import common.Constants;
import util.StdOut;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sandeep on 8/4/14.
 */
public class ReviewStats {
    private AtomicLong badBookReviews = new AtomicLong(0);
    private AtomicLong badReviewCountReviews = new AtomicLong(0);
    private AtomicLong acceptedReviews = new AtomicLong(0);
    private AtomicLong failedReviews = new AtomicLong(0);
    private long benchmarkStartTS;

    public void updateResults(long resultCode) {
        if (resultCode == Constants.ERR_INVALID_BOOK) {
            badBookReviews.incrementAndGet();
        } else if (resultCode == Constants.ERR_REVIEWER_OVER_REVIEW_LIMIT) {
            badReviewCountReviews.incrementAndGet();
        } else {
            assert (resultCode == Constants.REVIEW_SUCCESSFUL);
            acceptedReviews.incrementAndGet();
        }
    }

    public void printResults(long invocationsCompleted) {
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
        StdOut.printf(display, invocationsCompleted,
                acceptedReviews.get(), badBookReviews.get(),
                badReviewCountReviews.get(), failedReviews.get());
    }

    public long getTime(long endTimeStamp) {
        return Math.round((endTimeStamp - benchmarkStartTS) / 1000.0);
    }

    public void incrementFailedReviews() {
        failedReviews.incrementAndGet();
    }

    public void setBenchmarkStartTS(long benchmarkStartTS) {
        this.benchmarkStartTS = benchmarkStartTS;
    }

    @Override
    public String toString() {
        return "Results [benchmark start time=" + benchmarkStartTS
                + ", accepted reviews=" + acceptedReviews
                + ", bad book reviews=" + badBookReviews
                + ", accepted reviews=" + acceptedReviews
                + ", bad review count reviews=" + badReviewCountReviews
                + ", failed reviews=" + failedReviews+ "]";
    }

    public long getBadBookReviews() {
        return badBookReviews.get();
    }

    public void setBadBookReviews(long badBookReviews) {
        this.badBookReviews = new AtomicLong(badBookReviews);
    }

    public long getBadReviewCountReviews() {
        return badReviewCountReviews.get();
    }

    public void setBadReviewCountReviews(long badReviewCountReviews) {
        this.badReviewCountReviews = new AtomicLong(badReviewCountReviews);
    }

    public long getAcceptedReviews() {
        return acceptedReviews.get();
    }

    public void setAcceptedReviews(long acceptedReviews) {
        this.acceptedReviews = new AtomicLong(acceptedReviews);
    }

    public long getFailedReviews() {
        return failedReviews.get();
    }

    public void setFailedReviews(long failedReviews) {
        this.failedReviews = new AtomicLong(failedReviews);
    }

    public long getBenchmarkStartTS() {
        return benchmarkStartTS;
    }

}
