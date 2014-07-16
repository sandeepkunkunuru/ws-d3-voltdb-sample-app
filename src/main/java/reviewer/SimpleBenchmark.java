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

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import reviewer.common.BookReviewsGenerator;
import reviewer.common.Constants;
import reviewer.common.ReviewerConfig;

import java.io.IOException;

public class SimpleBenchmark extends Benchmark {
    private final static int TXNS = 10000;
    public String[] args;

    public SimpleBenchmark() {
        super(new ReviewerConfig());
    }

    public static void main(String[] args) {
        SimpleBenchmark bm = new SimpleBenchmark();
        bm.args = args;
        bm.runBenchmark();
    }

    public void runBenchmark() {
        System.out.println("Running Simple Benchmark which invokes default REVIEWS.insert stored procedure");

        try {
            final Client client = ClientFactory.createClient();

            for (String s : args) {
                client.createConnection(s, Client.VOLTDB_SERVER_PORT);
            }

            System.out.print(Constants.HORIZONTAL_RULE);
            System.out.println(" Setup & Initialization");
            System.out.println(Constants.HORIZONTAL_RULE);


            // initialize using synchronous call
            System.out.println("\nPopulating Static Tables\n");
            client.callProcedure("Initialize", config.books, Constants.BOOK_NAMES_CSV);

            BookReviewsGenerator gen = new BookReviewsGenerator(10000);

            for (int i = 0; i < SimpleBenchmark.TXNS; i++) {
                BookReviewsGenerator.Review review = gen.receive();
                ClientResponse response =
                        client.callProcedure("REVIEWS.insert", review.email, review.review, review.bookId);

                if (response.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException(response.getStatusString());
                }

                if (i % 1000 == 0) {
                    System.out.printf(".");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ProcCallException e) {
            throw new RuntimeException(e);
        }

        System.out.println(" completed " + SimpleBenchmark.TXNS + " transactions.");
    }
}
