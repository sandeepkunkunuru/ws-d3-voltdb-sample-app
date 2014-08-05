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
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.util.HashMap;
import java.util.logging.Logger;

@ServerEndpoint(value = "/review/{book}", encoders = ReviewStatsCodec.class, decoders = ReviewStatsCodec.class)
public class Reviewing {
	private final Logger log = Logger.getLogger(getClass().getName());
    private static final HashMap<String, Json> book_prefs = new HashMap<>(); //proxy for voltdb

    static{
        Stats pre1 = new Stats();
        Stats pre2 = new Stats();

        //book_prefs.put("book1", Json.)
    }
	@OnOpen
	public void open(final Session session, @PathParam("book") final String book) {
		log.info("session openend and bound to book: " + book);
		session.getUserProperties().put("book", book);
	}

	@OnMessage
	public void onMessage(final Session session, final Stats preferences) {
		String book = (String) session.getUserProperties().get("book");

/*		try {


		} catch (IOException | EncodeException e) {
			log.log(Level.WARNING, "onMessage failed", e);
		}*/
	}
}
