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
	"encoding/json"
	"errors"

	"github.com/openimsdk/tools/mq"

	"sync"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"
	"github.com/openimsdk/tools/discovery"

	"github.com/go-redis/redis"
	"google.golang.org/protobuf/proto"

	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/tools/batcher"
	"github.com/openimsdk/protocol/constant"
	pbconv "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/stringutil"
)

const (
	size              = 500
	mainDataBuffer    = 500
	subChanBuffer     = 50
	worker            = 50
	interval          = 100 * time.Millisecond
	hasReadChanBuffer = 1000
)

type ContextMsg struct {
	message *sdkws.MsgData
	ctx     context.Context
}

// This structure is used for asynchronously writing the sender’s read sequence (seq) regarding a message into MongoDB.
// For example, if the sender sends a message with a seq of 10, then their own read seq for this conversation should be set to 10.
type userHasReadSeq struct {
	conversationID string
	userHasReadMap map[string]int64
}

type OnlineHistoryRedisConsumerHandler struct {
	redisMessageBatches *batcher.Batcher[ConsumerMessage]

	msgTransferDatabase         controller.MsgTransferDatabase
	conversationUserHasReadChan chan *userHasReadSeq
	wg                          sync.WaitGroup

	groupClient        *rpcli.GroupClient
	conversationClient *rpcli.ConversationClient
}

type ConsumerMessage struct {
	Ctx   context.Context
	Key   string
	Value []byte
	Raw   mq.Message
}

func NewOnlineHistoryRedisConsumerHandler(ctx context.Context, client discovery.Conn, config *Config, database controller.MsgTransferDatabase) (*OnlineHistoryRedisConsumerHandler, error) {
	groupConn, err := client.GetConn(ctx, config.Discovery.RpcService.Group)
	if err != nil {
		return nil, err
	}
	conversationConn, err := client.GetConn(ctx, config.Discovery.RpcService.Conversation)
	if err != nil {
		return nil, err
	}
	var och OnlineHistoryRedisConsumerHandler
	och.msgTransferDatabase = database
	och.conversationUserHasReadChan = make(chan *userHasReadSeq, hasReadChanBuffer)
	och.groupClient = rpcli.NewGroupClient(groupConn)
	och.conversationClient = rpcli.NewConversationClient(conversationConn)
	och.wg.Add(1)

	b := batcher.New[ConsumerMessage](
		batcher.WithSize(size),
		batcher.WithWorker(worker),
		batcher.WithInterval(interval),
		batcher.WithDataBuffer(mainDataBuffer),
		batcher.WithSyncWait(true),
		batcher.WithBuffer(subChanBuffer),
	)
	b.Sharding = func(key string) int {
		hashCode := stringutil.GetHashCode(key)
		return int(hashCode) % och.redisMessageBatches.Worker()
	}
	// 从MQ消息中解析key进行分类合并
	b.Key = func(consumerMessage *ConsumerMessage) string {
		return consumerMessage.Key
	}
	b.Do = och.do
	och.redisMessageBatches = b

	och.redisMessageBatches.OnComplete = func(lastMessage *ConsumerMessage, totalCount int) {
		lastMessage.Raw.Mark()
		lastMessage.Raw.Commit()
	}

	return &och, nil
}

// 处理channel中的消息组
func (och *OnlineHistoryRedisConsumerHandler) do(ctx context.Context, channelID int, val *batcher.Msg[ConsumerMessage]) {
	ctx = mcontext.WithTriggerIDContext(ctx, val.TriggerID())
	ctxMessages := och.parseConsumerMessages(ctx, val.Val())
	ctx = withAggregationCtx(ctx, ctxMessages)
	log.ZInfo(ctx, "msg arrived channel", "channel id", channelID, "msgList length", len(ctxMessages), "key", val.Key())
	// 保存已读序列号到mongo
	och.doSetReadSeq(ctx, ctxMessages)

	// 对消息组中的消息进行再次分类，分为需要存储的消息、不需要存储的消息，需要存储的通知消息，不需要存储的通知消息
	storageMsgList, notStorageMsgList, storageNotificationList, notStorageNotificationList :=
		och.categorizeMessageLists(ctxMessages)
	log.ZDebug(ctx, "number of categorized messages", "storageMsgList", len(storageMsgList), "notStorageMsgList",
		len(notStorageMsgList), "storageNotificationList", len(storageNotificationList), "notStorageNotificationList", len(notStorageNotificationList))

	// 因为前面是按照MQ消息key进行分组的，所以channel中同一个组的消息属于同一个会话，即它们的会话ID相同 （key与会话ID是一一对应的，会话ID是按照一定规则生成的）
	conversationIDMsg := msgprocessor.GetChatConversationIDByMsg(ctxMessages[0].message)
	conversationIDNotification := msgprocessor.GetNotificationConversationIDByMsg(ctxMessages[0].message)
	// 处理消息
	och.handleMsg(ctx, val.Key(), conversationIDMsg, storageMsgList, notStorageMsgList)
	// 处理通知消息
	och.handleNotification(ctx, val.Key(), conversationIDNotification, storageNotificationList, notStorageNotificationList)
}

func (och *OnlineHistoryRedisConsumerHandler) doSetReadSeq(ctx context.Context, msgs []*ContextMsg) {

	// Outer map: conversationID -> (userID -> maxHasReadSeq)
	conversationUserSeq := make(map[string]map[string]int64)

	for _, msg := range msgs {
		if msg.message.ContentType != constant.HasReadReceipt {
			continue
		}
		var elem sdkws.NotificationElem
		if err := json.Unmarshal(msg.message.Content, &elem); err != nil {
			log.ZWarn(ctx, "Unmarshal NotificationElem error", err, "msg", msg)
			continue
		}
		var tips sdkws.MarkAsReadTips
		if err := json.Unmarshal([]byte(elem.Detail), &tips); err != nil {
			log.ZWarn(ctx, "Unmarshal MarkAsReadTips error", err, "msg", msg)
			continue
		}
		if len(tips.ConversationID) == 0 || tips.HasReadSeq < 0 {
			continue
		}

		// Calculate the max seq from tips.Seqs
		for _, seq := range tips.Seqs {
			if tips.HasReadSeq < seq {
				tips.HasReadSeq = seq
			}
		}

		if _, ok := conversationUserSeq[tips.ConversationID]; !ok {
			conversationUserSeq[tips.ConversationID] = make(map[string]int64)
		}
		if conversationUserSeq[tips.ConversationID][tips.MarkAsReadUserID] < tips.HasReadSeq {
			conversationUserSeq[tips.ConversationID][tips.MarkAsReadUserID] = tips.HasReadSeq
		}
	}
	log.ZInfo(ctx, "doSetReadSeq", "conversationUserSeq", conversationUserSeq)

	// persist to db
	for convID, userSeqMap := range conversationUserSeq {
		if err := och.msgTransferDatabase.SetHasReadSeqToDB(ctx, convID, userSeqMap); err != nil {
			log.ZWarn(ctx, "SetHasReadSeqToDB error", err, "conversationID", convID, "userSeqMap", userSeqMap)
		}
	}

}

func (och *OnlineHistoryRedisConsumerHandler) parseConsumerMessages(ctx context.Context, consumerMessages []*ConsumerMessage) []*ContextMsg {
	var ctxMessages []*ContextMsg
	for i := 0; i < len(consumerMessages); i++ {
		ctxMsg := &ContextMsg{}
		msgFromMQ := &sdkws.MsgData{}
		err := proto.Unmarshal(consumerMessages[i].Value, msgFromMQ)
		if err != nil {
			log.ZWarn(ctx, "msg_transfer Unmarshal msg err", err, string(consumerMessages[i].Value))
			continue
		}
		ctxMsg.ctx = consumerMessages[i].Ctx
		ctxMsg.message = msgFromMQ
		log.ZDebug(ctx, "message parse finish", "message", msgFromMQ, "key", consumerMessages[i].Key)
		ctxMessages = append(ctxMessages, ctxMsg)
	}
	return ctxMessages
}

// Get messages/notifications stored message list, not stored and pushed message list.
func (och *OnlineHistoryRedisConsumerHandler) categorizeMessageLists(totalMsgs []*ContextMsg) (storageMsgList,
	notStorageMsgList, storageNotificationList, notStorageNotificationList []*ContextMsg) {
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
				msg.Options = msgprocessor.WithOptions(msg.Options,
					msgprocessor.WithOfflinePush(options.IsOfflinePush()),
					msgprocessor.WithUnreadCount(options.IsUnreadCount()),
				)
				v.message.Options = msgprocessor.WithOptions(
					v.message.Options,
					msgprocessor.WithOfflinePush(false),
					msgprocessor.WithUnreadCount(false),
				)
				ctxMsg := &ContextMsg{
					message: msg,
					ctx:     v.ctx,
				}
				storageMsgList = append(storageMsgList, ctxMsg)
			}
			if options.IsHistory() {
				storageNotificationList = append(storageNotificationList, v)
			} else {
				notStorageNotificationList = append(notStorageNotificationList, v)
			}
		} else {
			if options.IsHistory() {
				storageMsgList = append(storageMsgList, v)
			} else {
				notStorageMsgList = append(notStorageMsgList, v)
			}
		}
	}
	return
}

func (och *OnlineHistoryRedisConsumerHandler) handleMsg(ctx context.Context, key, conversationID string, storageList, notStorageList []*ContextMsg) {
	log.ZInfo(ctx, "handle storage msg")
	for _, storageMsg := range storageList {
		log.ZDebug(ctx, "handle storage msg", "msg", storageMsg.message.String())
	}

	// 首先发送不需要存储的消息到MQ(topic:config.KafkaConfig.ToPushTopic), 这里发送消息的key是之前msg模块发送MQ消息用的key，这样才能保证消息在不同消息Topic之前传输时消息的顺序性。
	// 所以在整个消息发送的通路中，消息不管是在不同topic之前的传递还是对消息进行分组分类合并还是将消息组分片到channel中，为了保证通路中消息的顺序性，用的key必须是最初的同一个key。
	och.toPushTopic(ctx, key, conversationID, notStorageList)
	var storageMessageList []*sdkws.MsgData
	for _, msg := range storageList {
		storageMessageList = append(storageMessageList, msg.message)
	}
	if len(storageMessageList) > 0 {
		msg := storageMessageList[0]
		// 批量插入消息到redis中，因为redis内存数据库快，首先保存到redis，再异步通过MQ持久化到Mongo
		// lastSeq是当前会话的上一次消息的最后序列号
		lastSeq, isNewConversation, userSeqMap, err := och.msgTransferDatabase.BatchInsertChat2Cache(ctx, conversationID, storageMessageList)
		if err != nil && !errors.Is(errs.Unwrap(err), redis.Nil) {
			log.ZWarn(ctx, "batch data insert to redis err", err, "storageMsgList", storageMessageList)
			return
		}
		log.ZInfo(ctx, "BatchInsertChat2Cache end")
		// 标记会话消息组中的消息对发送者而言是已读的（存储到redis，也是因为redis速度快）
		err = och.msgTransferDatabase.SetHasReadSeqs(ctx, conversationID, userSeqMap)
		if err != nil {
			log.ZWarn(ctx, "SetHasReadSeqs error", err, "userSeqMap", userSeqMap, "conversationID", conversationID)
			prommetrics.SeqSetFailedCounter.Inc()
		}
		// 发送者已读存储到redis后，在异步发送到channel处理
		och.conversationUserHasReadChan <- &userHasReadSeq{
			conversationID: conversationID,
			userHasReadMap: userSeqMap,
		}

		// 新会话
		if isNewConversation {
			switch msg.SessionType {
			case constant.ReadGroupChatType:
				log.ZDebug(ctx, "group chat first create conversation", "conversationID",
					conversationID)

				// 从group模块获取群成员ID
				userIDs, err := och.groupClient.GetGroupMemberUserIDs(ctx, msg.GroupID)
				if err != nil {
					log.ZWarn(ctx, "get group member ids error", err, "conversationID",
						conversationID)
				} else {
					log.ZInfo(ctx, "GetGroupMemberIDs end")

					// 调用会话模块conversation创建会话
					if err := och.conversationClient.CreateGroupChatConversations(ctx, msg.GroupID, userIDs); err != nil {
						log.ZWarn(ctx, "single chat first create conversation error", err,
							"conversationID", conversationID)
					}
				}
			case constant.SingleChatType, constant.NotificationChatType:
				req := &pbconv.CreateSingleChatConversationsReq{
					RecvID:           msg.RecvID,
					SendID:           msg.SendID,
					ConversationID:   conversationID,
					ConversationType: msg.SessionType,
				}
				// 调用conversation模块创建会话
				if err := och.conversationClient.CreateSingleChatConversations(ctx, req); err != nil {
					log.ZWarn(ctx, "single chat or notification first create conversation error", err,
						"conversationID", conversationID, "sessionType", msg.SessionType)
				}
			default:
				log.ZWarn(ctx, "unknown session type", nil, "sessionType",
					msg.SessionType)
			}
		}

		log.ZInfo(ctx, "success incr to next topic")
		// 发送会话需要存储的消息组消息和会话的上一次序列号到MQ(topic: config.KafkaConfig.ToMongoTopic)，进行异步持久化 - 这里是一次性批量发送消息到MQ
		err = och.msgTransferDatabase.MsgToMongoMQ(ctx, key, conversationID, storageMessageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "Msg To MongoDB MQ error", err, "conversationID",
				conversationID, "storageList", storageMessageList, "lastSeq", lastSeq)
		}
		log.ZInfo(ctx, "MsgToMongoMQ end")

		// 需要存储的消息发送到MQ后，再次把这些消息发送到MQ(topic:topic:config.KafkaConfig.ToPushTopic - 这里并没有对消息拆分扩散，只是循环一条一条的发送消息到push推送模块)，用于推送到长连接
		och.toPushTopic(ctx, key, conversationID, storageList)
		log.ZInfo(ctx, "toPushTopic end")
	}
}

func (och *OnlineHistoryRedisConsumerHandler) handleNotification(ctx context.Context, key, conversationID string,
	storageList, notStorageList []*ContextMsg) {
	och.toPushTopic(ctx, key, conversationID, notStorageList)
	var storageMessageList []*sdkws.MsgData
	for _, msg := range storageList {
		storageMessageList = append(storageMessageList, msg.message)
	}
	if len(storageMessageList) > 0 {
		lastSeq, _, _, err := och.msgTransferDatabase.BatchInsertChat2Cache(ctx, conversationID, storageMessageList)
		if err != nil {
			log.ZError(ctx, "notification batch insert to redis error", err, "conversationID", conversationID,
				"storageList", storageMessageList)
			return
		}
		log.ZDebug(ctx, "success to next topic", "conversationID", conversationID)
		err = och.msgTransferDatabase.MsgToMongoMQ(ctx, key, conversationID, storageMessageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "Msg To MongoDB MQ error", err, "conversationID",
				conversationID, "storageList", storageMessageList, "lastSeq", lastSeq)
		}
		och.toPushTopic(ctx, key, conversationID, storageList)
	}
}
func (och *OnlineHistoryRedisConsumerHandler) HandleUserHasReadSeqMessages(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.ZPanic(ctx, "HandleUserHasReadSeqMessages Panic", errs.ErrPanic(r))
		}
	}()

	defer och.wg.Done()

	// 读取channel中的消息发送者已读，保存到mongo
	for msg := range och.conversationUserHasReadChan {
		if err := och.msgTransferDatabase.SetHasReadSeqToDB(ctx, msg.conversationID, msg.userHasReadMap); err != nil {
			log.ZWarn(ctx, "set read seq to db error", err, "conversationID", msg.conversationID, "userSeqMap", msg.userHasReadMap)
		}
	}

	log.ZInfo(ctx, "Channel closed, exiting handleUserHasReadSeqMessages")
}
func (och *OnlineHistoryRedisConsumerHandler) Close() {
	close(och.conversationUserHasReadChan)
	och.wg.Wait()
}

func (och *OnlineHistoryRedisConsumerHandler) toPushTopic(ctx context.Context, key, conversationID string, msgs []*ContextMsg) {
	for _, v := range msgs {
		log.ZDebug(ctx, "push msg to topic", "msg", v.message.String())
		if err := och.msgTransferDatabase.MsgToPushMQ(v.ctx, key, conversationID, v.message); err != nil {
			log.ZError(ctx, "msg to push topic error", err, "msg", v.message.String())
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

func (och *OnlineHistoryRedisConsumerHandler) HandlerRedisMessage(msg mq.Message) error { // a instance in the consumer group
	// 把接收到的MQ消息放入到batcher的data通道中(进行异步解耦)
	err := och.redisMessageBatches.Put(msg.Context(), &ConsumerMessage{Ctx: msg.Context(), Key: msg.Key(), Value: msg.Value(), Raw: msg})
	if err != nil {
		log.ZWarn(msg.Context(), "put msg to  error", err, "key", msg.Key(), "value", msg.Value())
	}
	return nil
}
