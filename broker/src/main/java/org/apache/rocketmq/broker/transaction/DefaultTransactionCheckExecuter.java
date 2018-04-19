package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.transaction.TransactionCheckExecuter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储层回调此接口，用来主动回查Producer的事务状态
 *
 * @author wangc
 * @date 2018/4/18 13:35
 */
public class DefaultTransactionCheckExecuter implements TransactionCheckExecuter {
    private static final Logger log = LoggerFactory.getLogger(DefaultTransactionCheckExecuter.class);
    private final BrokerController brokerController;

    public DefaultTransactionCheckExecuter(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void gotoCheck(String producerGorupName,
                          long tranStateTableOffset,
                          long commitLogOffset,
                          int msgSize) {
        // 第一步查询生产者
        final ClientChannelInfo clientChannelInfo = this.brokerController.getProducerManager().pickProducerChannelRandomly(producerGorupName);
        if (null == clientChannelInfo) {
            log.warn("check a producer transaction,but not find any channel of this group[{}]", producerGorupName);
            return;
        }

        // 第二步，查询信息
        SelectMappedBufferResult selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(commitLogOffset, msgSize);
        if (null == selectMappedBufferResult) {
            log.warn("check a producer transaction,but not find message by commitLogOffset:{},msgSize",commitLogOffset,msgSize);
            return;
        }

        // 第三部、想Producer发起请求
        final CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        requestHeader.setCommitLogOffset(commitLogOffset);
        requestHeader.setTranStateTableOffset(tranStateTableOffset);
        this.brokerController.getBroker2Client().checkProducerTransactionState(clientChannelInfo.getChannel(),requestHeader,selectMappedBufferResult);
    }
}
