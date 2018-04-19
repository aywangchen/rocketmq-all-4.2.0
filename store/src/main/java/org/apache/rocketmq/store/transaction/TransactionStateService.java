package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 事务服务，存储每条事务消息的状态（Prepared,Commited,Rollbacked）<br>
 *
 * @author wangc
 * @date 2018/4/17 17:03
 */
public class TransactionStateService {
    // 更改事务状态，具体更改位置
    private final static int TS_STATE_POS = 20;

    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;

    private Map<String, DispatchRequest> transactionStateMap;

    private final Timer timer = new Timer("CheckTransactionMessageTimer", true);

    public TransactionStateService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.transactionStateMap = new HashMap<String, DispatchRequest>(16);
    }

    public void start() {
        this.initTimerTask();
    }

    public void shutdown() {
        this.timer.cancel();
    }

    /**
     * 新增事务状态re
     *
     * @return 是否成功
     */
    public boolean appendPreparedTransaction(DispatchRequest dispatchRequest) {
        String producerGroupName = dispatchRequest.getPropertiesMap().get(MessageConst.PROPERTY_PRODUCER_GROUP);
        String key = producerGroupName + String.valueOf(dispatchRequest.getCommitLogOffset());
        transactionStateMap.put(key, dispatchRequest);
        return true;
    }

    /**
     * 更新事务状态
     *
     * @return 是否成功
     */
    public boolean updateTransactionstate(final long tsOffset,
                                          final long clOffset,
                                          final String producerGroupName,
                                          final int state) {
        SelectMappedBufferResult selectMappedBufferResult = this.defaultMessageStore.selectOneMessageByOffset(clOffset);
        if (null == selectMappedBufferResult) {
            return false;
        }
        // 更新事务状态
        selectMappedBufferResult.getByteBuffer().putInt(TS_STATE_POS, state);
        String key = producerGroupName + String.valueOf(tsOffset);
        transactionStateMap.remove(key);
        return true;
    }

    /**
     * 初始化定时任务
     */
    private void initTimerTask() {
        this.addTimerTask(transactionStateMap);
    }

    private void addTimerTask(final Map<String, DispatchRequest> transactionStateMap) {
        this.timer.scheduleAtFixedRate(new TimerTask() {
            private final Map<String, DispatchRequest> map = transactionStateMap;
            private final TransactionCheckExecuter transactionCheckExecuter = TransactionStateService.this.defaultMessageStore.getTransactionCheckExecuter();
            private final boolean slave = TransactionStateService.this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

            @Override
            public void run() {
                // Slave不需要回查事务状态
                if (slave) {
                    return;
                }

                for (Map.Entry<String, DispatchRequest> entry : map.entrySet()) {
                    System.out.println("回查事务状态 start");

                    DispatchRequest request = entry.getValue();
                    String producerGroupName = request.getPropertiesMap().get(MessageConst.PROPERTY_PRODUCER_GROUP);
                    this.transactionCheckExecuter.gotoCheck(producerGroupName,
                            request.getTranStateTableOffset(),
                            request.getCommitLogOffset(),
                            request.getMsgSize());
                }
            }
        }, 1000 * 60, 1000 * 60);
    }
}
