// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgtransfer

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/db/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcclient"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq/kafka"
	"github.com/openimsdk/tools/utils/idutil"
	"github.com/openimsdk/tools/utils/stringutil"
	"google.golang.org/protobuf/proto"
)

const (
	ConsumerMsgs   = 3
	SourceMessages = 4
	MongoMessages  = 5
	ChannelNum     = 100
)

type MsgChannelValue struct {
	uniqueKey  string
	ctx        context.Context
	ctxMsgList []*ContextMsg
}

type TriggerChannelValue struct {
	ctx      context.Context
	cMsgList []*sarama.ConsumerMessage
}

type Cmd2Value struct {
	Cmd   int
	Value any
}
type ContextMsg struct {
	message *sdkws.MsgData
	ctx     context.Context
}

type OnlineHistoryRedisConsumerHandler struct {
	historyConsumerGroup *kafka.MConsumerGroup
	chArrays             [ChannelNum]chan Cmd2Value
	msgDistributionCh    chan Cmd2Value

	// singleMsgSuccessCount      uint64
	// singleMsgFailedCount       uint64
	// singleMsgSuccessCountMutex sync.Mutex
	// singleMsgFailedCountMutex  sync.Mutex

	msgDatabase           controller.CommonMsgDatabase
	conversationRpcClient *rpcclient.ConversationRpcClient
	groupRpcClient        *rpcclient.GroupRpcClient
}

func NewOnlineHistoryRedisConsumerHandler(kafkaConf *config.Kafka, database controller.CommonMsgDatabase,
	conversationRpcClient *rpcclient.ConversationRpcClient, groupRpcClient *rpcclient.GroupRpcClient) (*OnlineHistoryRedisConsumerHandler, error) {
	// ToRedisTopic topic消费端 接收msg模块的消息
	historyConsumerGroup, err := kafka.NewMConsumerGroup(kafkaConf.Build(), kafkaConf.ToRedisGroupID, []string{kafkaConf.ToRedisTopic})
	if err != nil {
		return nil, err
	}
	var och OnlineHistoryRedisConsumerHandler
	och.msgDatabase = database
	// 定义通道接收消息
	och.msgDistributionCh = make(chan Cmd2Value) // no buffer channel
	// 处理msgDistributionCh通道消息
	go och.MessagesDistributionHandle()
	for i := 0; i < ChannelNum; i++ {
		och.chArrays[i] = make(chan Cmd2Value, 50)
		// 并发处理通道chArrays[i]中的有序消息
		go och.Run(i)
	}
	och.conversationRpcClient = conversationRpcClient
	och.groupRpcClient = groupRpcClient
	och.historyConsumerGroup = historyConsumerGroup
	return &och, err
}

func (och *OnlineHistoryRedisConsumerHandler) Run(channelID int) {
	// 处理通道有序消息， 消息导通容量50， 每个消息单元中消息数组数<=1000条
	for cmd := range och.chArrays[channelID] {
		switch cmd.Cmd {
		case SourceMessages:
			msgChannelValue := cmd.Value.(MsgChannelValue)
			ctxMsgList := msgChannelValue.ctxMsgList
			ctx := msgChannelValue.ctx
			log.ZDebug(
				ctx,
				"msg arrived channel",
				"channel id",
				channelID,
				"msgList length",
				len(ctxMsgList),
				"uniqueKey",
				msgChannelValue.uniqueKey,
			)
			// 对发送的消息进行分类
			storageMsgList, notStorageMsgList, storageNotificationList, notStorageNotificationList, modifyMsgList := och.getPushStorageMsgList(
				ctxMsgList,
			)
			log.ZDebug(
				ctx,
				"msg lens",
				"storageMsgList",
				len(storageMsgList),
				"notStorageMsgList",
				len(notStorageMsgList),
				"storageNotificationList",
				len(storageNotificationList),
				"notStorageNotificationList",
				len(notStorageNotificationList),
				"modifyMsgList",
				len(modifyMsgList),
			)
			// 按照规则生成conversation_id -- 直接使用ctxMsgList[0].message(因为发送消息用的key和分类的key都是固定规则生成的，所以在这个消息组中的消息一定属于同一个conversation)
			conversationIDMsg := msgprocessor.GetChatConversationIDByMsg(ctxMsgList[0].message)
			// 按照规则生成conversation_notify_id
			conversationIDNotification := msgprocessor.GetNotificationConversationIDByMsg(ctxMsgList[0].message)
			och.handleMsg(ctx, msgChannelValue.uniqueKey, conversationIDMsg, storageMsgList, notStorageMsgList)
			och.handleNotification(
				ctx,
				msgChannelValue.uniqueKey,
				conversationIDNotification,
				storageNotificationList,
				notStorageNotificationList,
			)
			if err := och.msgDatabase.MsgToModifyMQ(ctx, msgChannelValue.uniqueKey, conversationIDNotification, modifyMsgList); err != nil {
				log.ZError(ctx, "msg to modify mq error", err, "uniqueKey", msgChannelValue.uniqueKey, "modifyMsgList", modifyMsgList)
			}
		}
	}
}

// Get messages/notifications stored message list, not stored and pushed message list.
func (och *OnlineHistoryRedisConsumerHandler) getPushStorageMsgList(
	totalMsgs []*ContextMsg,
) (storageMsgList, notStorageMsgList, storageNotificatoinList, notStorageNotificationList, modifyMsgList []*sdkws.MsgData) {
	isStorage := func(msg *sdkws.MsgData) bool {
		options2 := msgprocessor.Options(msg.Options)
		// 消息中是否包含history option
		if options2.IsHistory() {
			return true
		}
		// if !(!options2.IsSenderSync() && conversationID == msg.MsgData.SendID) {
		// 	return false
		// }
		return false
	}
	// 遍历有序消息，根据消息的option进行分类成不同的消息集合
	for _, v := range totalMsgs {
		options := msgprocessor.Options(v.message.Options)
		if !options.IsNotNotification() {
			// clone msg from notificationMsg
			if options.IsSendMsg() {
				msg := proto.Clone(v.message).(*sdkws.MsgData)
				// message
				if v.message.Options != nil {
					msg.Options = msgprocessor.NewMsgOptions()
				}
				if options.IsOfflinePush() {
					v.message.Options = msgprocessor.WithOptions(
						v.message.Options,
						msgprocessor.WithOfflinePush(false),
					)
					msg.Options = msgprocessor.WithOptions(msg.Options, msgprocessor.WithOfflinePush(true))
				}
				if options.IsUnreadCount() {
					v.message.Options = msgprocessor.WithOptions(
						v.message.Options,
						msgprocessor.WithUnreadCount(false),
					)
					msg.Options = msgprocessor.WithOptions(msg.Options, msgprocessor.WithUnreadCount(true))
				}
				storageMsgList = append(storageMsgList, msg)
			}
			if isStorage(v.message) {
				storageNotificatoinList = append(storageNotificatoinList, v.message)
			} else {
				notStorageNotificationList = append(notStorageNotificationList, v.message)
			}
		} else {
			if isStorage(v.message) {
				storageMsgList = append(storageMsgList, v.message)
			} else {
				notStorageMsgList = append(notStorageMsgList, v.message)
			}
		}
		if v.message.ContentType == constant.ReactionMessageModifier ||
			v.message.ContentType == constant.ReactionMessageDeleter {
			modifyMsgList = append(modifyMsgList, v.message)
		}
	}
	return
}

func (och *OnlineHistoryRedisConsumerHandler) handleNotification(
	ctx context.Context,
	key, conversationID string,
	storageList, notStorageList []*sdkws.MsgData,
) {
	och.toPushTopic(ctx, key, conversationID, notStorageList)
	if len(storageList) > 0 {
		lastSeq, _, err := och.msgDatabase.BatchInsertChat2Cache(ctx, conversationID, storageList)
		if err != nil {
			log.ZError(
				ctx,
				"notification batch insert to redis error",
				err,
				"conversationID",
				conversationID,
				"storageList",
				storageList,
			)
			return
		}
		log.ZDebug(ctx, "success to next topic", "conversationID", conversationID)
		err = och.msgDatabase.MsgToMongoMQ(ctx, key, conversationID, storageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "MsgToMongoMQ error", err)
		}
		och.toPushTopic(ctx, key, conversationID, storageList)
	}
}

func (och *OnlineHistoryRedisConsumerHandler) toPushTopic(ctx context.Context, key, conversationID string, msgs []*sdkws.MsgData) {
	for _, v := range msgs {
		och.msgDatabase.MsgToPushMQ(ctx, key, conversationID, v) // nolint: errcheck
	}
}

func (och *OnlineHistoryRedisConsumerHandler) handleMsg(ctx context.Context, key, conversationID string, storageList, notStorageList []*sdkws.MsgData) {
	// 发送不需要保存的消息组到kafka topic(ToPushTopic) 到push 模块 -- 单条发送
	och.toPushTopic(ctx, key, conversationID, notStorageList)
	if len(storageList) > 0 {
		// 处理需要存储的消息列表：存储消息 / 保存当前conversation max seq / 保存当前conversation发送者已读seq
		lastSeq, isNewConversation, err := och.msgDatabase.BatchInsertChat2Cache(ctx, conversationID, storageList)
		if err != nil && errs.Unwrap(err) != redis.Nil {
			log.ZError(ctx, "batch data insert to redis err", err, "storageMsgList", storageList)
			return
		}
		// 首次建立conversation
		if isNewConversation {
			switch storageList[0].SessionType {
			case constant.ReadGroupChatType:
				log.ZInfo(ctx, "group chat first create conversation", "conversationID",
					conversationID)
				userIDs, err := och.groupRpcClient.GetGroupMemberIDs(ctx, storageList[0].GroupID)
				if err != nil {
					log.ZWarn(ctx, "get group member ids error", err, "conversationID",
						conversationID)
				} else {
					if err := och.conversationRpcClient.GroupChatFirstCreateConversation(ctx,
						storageList[0].GroupID, userIDs); err != nil {
						log.ZWarn(ctx, "single chat first create conversation error", err,
							"conversationID", conversationID)
					}
				}
			case constant.SingleChatType, constant.NotificationChatType:
				// msg-transfer模块通过rpc调用conversation模块创建conversation信息
				if err := och.conversationRpcClient.SingleChatFirstCreateConversation(ctx, storageList[0].RecvID,
					storageList[0].SendID, conversationID, storageList[0].SessionType); err != nil {
					log.ZWarn(ctx, "single chat or notification first create conversation error", err,
						"conversationID", conversationID, "sessionType", storageList[0].SessionType)
				}
			default:
				log.ZWarn(ctx, "unknown session type", nil, "sessionType",
					storageList[0].SessionType)
			}
		}

		log.ZDebug(ctx, "success incr to next topic")
		// 发送需要保存的消息组到kafka(topic: ToMongoTopic), 然后又被当前模块的online_msg_to_mongo消费
		err = och.msgDatabase.MsgToMongoMQ(ctx, key, conversationID, storageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "MsgToMongoMQ error", err)
		}
		// 发送需要保存的消息组到kafka topic(ToPushTopic) 到 push模块 -- 单条发送
		och.toPushTopic(ctx, key, conversationID, storageList)
	}
}

func (och *OnlineHistoryRedisConsumerHandler) MessagesDistributionHandle() {
	for {
		aggregationMsgs := make(map[string][]*ContextMsg, ChannelNum)
		select {
		// 从通道中读取0.1s时间分片处理的消息分组，每组消息条数<=1000
		case cmd := <-och.msgDistributionCh:
			switch cmd.Cmd {
			case ConsumerMsgs:
				triggerChannelValue := cmd.Value.(TriggerChannelValue)
				ctx := triggerChannelValue.ctx
				// 分片消息数组<=1000
				consumerMessages := triggerChannelValue.cMsgList
				// Aggregation map[userid]message list
				log.ZDebug(ctx, "batch messages come to distribution center", "length", len(consumerMessages))
				// 遍历并解析每组分片的1000条消息
				for i := 0; i < len(consumerMessages); i++ {
					ctxMsg := &ContextMsg{}
					msgFromMQ := &sdkws.MsgData{}
					err := proto.Unmarshal(consumerMessages[i].Value, msgFromMQ)
					if err != nil {
						log.ZError(ctx, "msg_transfer Unmarshal msg err", err, string(consumerMessages[i].Value))
						continue
					}
					var arr []string
					for i, header := range consumerMessages[i].Headers {
						arr = append(arr, strconv.Itoa(i), string(header.Key), string(header.Value))
					}
					log.ZInfo(ctx, "consumer.kafka.GetContextWithMQHeader", "len", len(consumerMessages[i].Headers),
						"header", strings.Join(arr, ", "))
					// kafka消息头和消息重新构造消息
					ctxMsg.ctx = kafka.GetContextWithMQHeader(consumerMessages[i].Headers)
					ctxMsg.message = msgFromMQ
					log.ZDebug(
						ctx,
						"single msg come to distribution center",
						"message",
						msgFromMQ,
						"key",
						string(consumerMessages[i].Key),
					)
					// aggregationMsgs[string(consumerMessages[i].Key)] =
					// append(aggregationMsgs[string(consumerMessages[i].Key)], ctxMsg)
					// 用发送的消息的key对1000条消息进行分类map[string][]...
					if oldM, ok := aggregationMsgs[string(consumerMessages[i].Key)]; ok {
						oldM = append(oldM, ctxMsg)
						aggregationMsgs[string(consumerMessages[i].Key)] = oldM
					} else {
						m := make([]*ContextMsg, 0, 100)
						m = append(m, ctxMsg)
						aggregationMsgs[string(consumerMessages[i].Key)] = m
					}
				}
				log.ZDebug(ctx, "generate map list users len", "length", len(aggregationMsgs))
				// 对分类消息进行处理
				for uniqueKey, v := range aggregationMsgs {
					if len(v) >= 0 {
						hashCode := stringutil.GetHashCode(uniqueKey)
						channelID := hashCode % ChannelNum
						newCtx := withAggregationCtx(ctx, v)
						log.ZDebug(
							newCtx,
							"generate channelID",
							"hashCode",
							hashCode,
							"channelID",
							channelID,
							"uniqueKey",
							uniqueKey,
						)
						// 将key对应的分类消息组写入channel id对应的channel中，那么相同key的消息会按照发送顺序进入通道，保证消息有序。（到这里都保证了从发送端过kafka，再从kafka读取到写入channel_id对应的通道都是有序的）
						och.chArrays[channelID] <- Cmd2Value{Cmd: SourceMessages, Value: MsgChannelValue{uniqueKey: uniqueKey, ctxMsgList: v, ctx: newCtx}}
					}
				}
			}
		}
	}
}

func withAggregationCtx(ctx context.Context, values []*ContextMsg) context.Context {
	var allMessageOperationID string
	for i, v := range values {
		if opid := mcontext.GetOperationID(v.ctx); opid != "" {
			if i == 0 {
				allMessageOperationID += opid
			} else {
				allMessageOperationID += "$" + opid
			}
		}
	}
	return mcontext.SetOperationID(ctx, allMessageOperationID)
}

func (och *OnlineHistoryRedisConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (och *OnlineHistoryRedisConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费msg模块发送的ToRedisTopic消息：两个goroutine，一个从kafka读消息负责写入到数组中，另一个在0.1s时从数组中取走消息
func (och *OnlineHistoryRedisConsumerHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error { // a instance in the consumer group
	for {
		if sess == nil {
			log.ZWarn(context.Background(), "sess == nil, waiting", nil)
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	log.ZInfo(context.Background(), "online new session msg come", "highWaterMarkOffset",
		claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition())

	var (
		split    = 1000
		rwLock   = new(sync.RWMutex)
		messages = make([]*sarama.ConsumerMessage, 0, 1000)
		// 0.1s批量处理一次消息
		ticker = time.NewTicker(time.Millisecond * 100)

		wg      = sync.WaitGroup{}
		running = new(atomic.Bool)
	)
	running.Store(true)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			// 0.1s时间开始处理
			case <-ticker.C:
				// if the buffer is empty and running is false, return loop.
				if len(messages) == 0 {
					if !running.Load() {
						return
					}

					continue
				}

				// 获取lock
				rwLock.Lock()
				// 读取数组消息，复用数组
				buffer := make([]*sarama.ConsumerMessage, 0, len(messages))
				buffer = append(buffer, messages...)

				// reuse slice, set cap to 0
				messages = messages[:0]
				rwLock.Unlock()

				start := time.Now()
				ctx := mcontext.WithTriggerIDContext(context.Background(), idutil.OperationIDGenerator())
				log.ZDebug(ctx, "timer trigger msg consumer start", "length", len(buffer))
				// 将数组中的消息进行分片(每组消息1000条)，并写入msgDistributionCh通道中
				for i := 0; i < len(buffer)/split; i++ {
					och.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
						ctx: ctx, cMsgList: buffer[i*split : (i+1)*split],
					}}
				}
				// 剩余消息写入到msgDistributionCh通道中
				if (len(buffer) % split) > 0 {
					och.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
						ctx: ctx, cMsgList: buffer[split*(len(buffer)/split):],
					}}
				}

				log.ZDebug(ctx, "timer trigger msg consumer end",
					"length", len(buffer), "time_cost", time.Since(start),
				)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for running.Load() {
			select {
			case msg, ok := <-claim.Messages():
				if !ok {
					running.Store(false)
					return
				}

				if len(msg.Value) == 0 {
					continue
				}

				// lock保护
				rwLock.Lock()
				// 读取kafka消息写入到数组中
				messages = append(messages, msg)
				rwLock.Unlock()

				// 标记自动提交
				sess.MarkMessage(msg, "")

			case <-sess.Context().Done():
				running.Store(false)
				return
			}
		}
	}()

	wg.Wait()
	return nil
}
