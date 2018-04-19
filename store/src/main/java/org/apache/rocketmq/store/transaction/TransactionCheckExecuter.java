package org.apache.rocketmq.store.transaction;

/**
 * @author wangc
 * @date 2018/4/18 11:20
 */
public interface TransactionCheckExecuter {
    public void gotoCheck(
            final String  producerGorupName,
            final long tranStateTableOffset,
            final long commitLogOffset,
            final int msgSize
            );
}
