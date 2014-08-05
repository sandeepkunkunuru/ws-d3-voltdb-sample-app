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

package services;

import models.ReviewStats;

import javax.json.Json;
import javax.json.JsonObject;
import javax.websocket.*;
import java.io.StringReader;

/**
 * Created by sandeep on 8/4/14.
 */
public class ReviewStatsCodec implements Encoder.Text<ReviewStats>, Decoder.Text<ReviewStats> {
    @Override
    public String encode(final ReviewStats reviewStats) throws EncodeException {
        return Json.createObjectBuilder()
                .add("benchmarkStartTS", reviewStats.getBenchmarkStartTS())
                .add("acceptedReviews", reviewStats.getAcceptedReviews())
                .add("badBookReviews", reviewStats.getBadBookReviews())
                .add("badReviewCountReviews", reviewStats.getBadReviewCountReviews())
                .add("failedReviews", reviewStats.getFailedReviews())
                .toString();
    }

    @Override
    public ReviewStats decode(final String textMessage) throws DecodeException {
        ReviewStats reviewStats = new ReviewStats();
        JsonObject obj = Json.createReader(new StringReader(textMessage)).readObject();

        reviewStats.setBenchmarkStartTS(Long.parseLong(obj.getString("benchmarkStartTS")));
        reviewStats.setAcceptedReviews(Long.parseLong(obj.getString("acceptedReviews")));
        reviewStats.setBadBookReviews(Long.parseLong(obj.getString("badBookReviews")));
        reviewStats.setBadReviewCountReviews(Long.parseLong(obj.getString("badReviewCountReviews")));
        reviewStats.setFailedReviews(Long.parseLong(obj.getString("failedReviews")));

        return reviewStats;
    }

    @Override
    public boolean willDecode(final String s) {
        return true;
    }

    @Override
    public void init(EndpointConfig endpointConfig) {

    }

    @Override
    public void destroy() {

    }

}
