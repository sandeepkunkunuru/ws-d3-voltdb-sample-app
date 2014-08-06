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

import models.Stats;

import javax.json.Json;
import javax.json.JsonObject;
import javax.websocket.*;
import java.io.StringReader;

/**
 * Created by sandeep on 8/4/14.
 */
public class ReviewStatsCodec implements Encoder.Text<Stats>, Decoder.Text<Stats> {
    @Override
    public String encode(final Stats stats) throws EncodeException {
        return Json.createObjectBuilder()
                .add("benchmarkStartTS", stats.getStartTS())
                .add("currentTS", stats.getTime())
                .add("invocations", stats.getInvocations())
                .add("acceptedReviews", stats.getAccepted())
                .add("badBookReviews", stats.getInvalidEntity())
                .add("badReviewCountReviews", stats.getInvalid())
                .add("failedReviews", stats.getFailed())
                .add("throughput", stats.getThroughput())
                .add("aborts", stats.getAborts())
                .add("errors", stats.getErrors())
                .add("latency", stats.getLatency())
                .add("latency_95", stats.getLatency_95())
                .toString();
    }

    @Override
    public Stats decode(final String textMessage) throws DecodeException {
        Stats stats = new Stats();
        JsonObject obj = Json.createReader(new StringReader(textMessage)).readObject();
        stats.setStartTS(Long.parseLong(obj.getString("benchmarkStartTS")));
        stats.setEndTS(Long.parseLong(obj.getString("currentTS")));
        stats.setInvocations(Long.parseLong(obj.getString("invocations")));
        stats.setThroughput(Long.parseLong(obj.getString("throughput")));
        stats.setAborts(Long.parseLong(obj.getString("aborts")));
        stats.setErrors(Long.parseLong(obj.getString("errors")));
        stats.setLatency(Long.parseLong(obj.getString("latency")));
        stats.setLatency_95(Long.parseLong(obj.getString("latency_95")));
        stats.setAccepted(Long.parseLong(obj.getString("acceptedReviews")));
        stats.setInvalidEntity(Long.parseLong(obj.getString("badBookReviews")));
        stats.setInvalid(Long.parseLong(obj.getString("badReviewCountReviews")));
        stats.setFailed(Long.parseLong(obj.getString("failedReviews")));

        return stats;
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
