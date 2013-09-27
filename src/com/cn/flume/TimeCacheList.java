package com.cn.flume;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class TimeCacheList<K> implements Serializable{
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public static interface ExpiredCallback<K> extends Serializable{
        public void expire(List<K> key);
    }

    private LinkedList<ArrayList<K>> _buckets;

    private final Integer _lock = new Integer(1);
    private Thread _cleaner;
    private ExpiredCallback<K> _callback;
    
    public TimeCacheList(long expirationSecs, int numBuckets, ExpiredCallback<K> callback) {
        if(numBuckets<2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<ArrayList<K>>();
        //实例化若干数据容器
        for(int i=0; i<numBuckets; i++) {
            _buckets.add(new ArrayList<K>());
        }


        _callback = callback;
        final long expirationMillis = expirationSecs * 1000;
        final long sleepTime = expirationMillis / (numBuckets-1);
        _cleaner = new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        List<K> dead = null;
                        Thread.sleep(sleepTime);
                        synchronized(_lock) {
                            dead = _buckets.removeLast();
                            _buckets.addFirst(new ArrayList<K>());
                        }
                        if(_callback!=null) {
                        	_callback.expire(dead);
                        }
                    }
                } catch (InterruptedException ex) {

                }
            }
        });
        _cleaner.setDaemon(true);
        _cleaner.start();
    }

    public TimeCacheList(int expirationSecs, ExpiredCallback<K> callback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
    }

    public TimeCacheList(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    public TimeCacheList(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }


   

    public void add(K key) {
        synchronized(_lock) {
        	ArrayList<K> list = _buckets.getFirst();
        	list.add(key);
        }
    }
    

    public int size() {
        synchronized(_lock) {
            int size = 0;
            for(ArrayList<K> bucket: _buckets) {
                size+=bucket.size();
            }
            return size;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            _cleaner.interrupt();
        } finally {
            super.finalize();
        }
    }

}
