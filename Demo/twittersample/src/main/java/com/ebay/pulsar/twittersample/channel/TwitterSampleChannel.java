/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.twittersample.channel;


import org.springframework.context.ApplicationEvent;

import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.AbstractInboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;

public class TwitterSampleChannel extends AbstractInboundChannel {
    private TwitterStream twitterStream;
    
    @Override
    public void afterPropertiesSet() throws Exception {
        
    }

    @Override
    public void close() throws EventException {
        super.close();
        shutDown();
    }

    @Override
    public void flush() throws EventException {
        
    }

    @Override
    public ChannelAddress getAddress() {
        return null;
    }

    
    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void open() throws EventException {
        super.open();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            @Override
            public void onStatus(Status status) {
                HashtagEntity[] hashtagEntities = status.getHashtagEntities();
                
                JetstreamEvent event = new JetstreamEvent();
                event.setEventType("TwitterSample");

                Place place = status.getPlace();
                if (place != null) {
                    event.put("country", place.getCountry());
                }
                event.put("ct", status.getCreatedAt().getTime());
                event.put("text", status.getText());
                event.put("lang", status.getLang());
                event.put("user", status.getUser().getName());
                if (hashtagEntities != null && hashtagEntities.length > 0) {
                    StringBuilder s = new StringBuilder();
                    s.append(hashtagEntities[0].getText());
                    
                    for (int i = 1; i < hashtagEntities.length; i++) {
                        s.append(",");
                        s.append(hashtagEntities[i].getText());
                    }
                    
                    event.put("hashtag", s.toString());
                }

                fireSendEvent(event);
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample(); 
    }

    @Override
    public void pause() {
        close();
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        
    }

    @Override
    public void resume() {
        open();
    }

    @Override
    public void shutDown() {
        TwitterStream s = twitterStream;
        if (s != null) {
            twitterStream = null;
            s.cleanUp();
        }
    }

}
