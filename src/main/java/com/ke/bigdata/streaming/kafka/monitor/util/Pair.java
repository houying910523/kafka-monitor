package com.ke.bigdata.streaming.kafka.monitor.util;

/**
 * @author hy
 * @date 2019/8/1
 * @desc
 */
public class Pair<K, V> {

    private K left;
    private V right;

    public Pair(K k, V v) {
        this.left = k;
        this.right = v;
    }

    public K getLeft() {
        return left;
    }

    public V getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "<" + left + ", " + right + ">";
    }
}
