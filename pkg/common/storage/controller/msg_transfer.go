package controller

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/pkg/common/convert"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/mq"
	"github.com/openimsdk/tools/utils/datautil"
	"google.golang.org/protobuf/proto"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type MsgTransferDatabase interface {
	// BatchInsertChat2DB inserts a batch of messages into the database for a specific conversation.
	BatchInsertChat2DB(ctx context.Context, conversationID string, msgs []*sdkws.MsgData, currentMaxSeq int64) error
	// DeleteMessagesFromCache deletes message caches from Redis by sequence numbers.
	DeleteMessagesFromCache(ctx context.Context, conversationID string, seqs []int64) error

	// BatchInsertChat2Cache increments the sequence number and then batch inserts messages into the cache.
	BatchInsertChat2Cache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (seq int64, isNewConversation bool, userHasReadMap map[string]int64, err error)

	SetHasReadSeqs(ctx context.Context, conversationID string, userSeqMap map[string]int64) error

	SetHasReadSeqToDB(ctx context.Context, conversationID string, userSeqMap map[string]int64) error

	// to mq
	MsgToPushMQ(ctx context.Context, key, conversationID string, msg2mq *sdkws.MsgData) error
	MsgToMongoMQ(ctx context.Context, key, conversationID string, msgs []*sdkws.MsgData, lastSeq int64) error
}

func NewMsgTransferDatabase(msgDocModel database.Msg, msg cache.MsgCache, seqUser cache.SeqUser, seqConversation cache.SeqConversationCache, mongoProducer, pushProducer mq.Producer) (MsgTransferDatabase, error) {
	//conf, err := kafka.BuildProducerConfig(*kafkaConf.Build())
	//if err != nil {
	//	return nil, err
	//}
	//producerToMongo, err := kafka.NewKafkaProducerV2(conf, kafkaConf.Address, kafkaConf.ToMongoTopic)
	//if err != nil {
	//	return nil, err
	//}
	//producerToPush, err := kafka.NewKafkaProducerV2(conf, kafkaConf.Address, kafkaConf.ToPushTopic)
	//if err != nil {
	//	return nil, err
	//}
	return &msgTransferDatabase{
		msgDocDatabase:  msgDocModel,
		msgCache:        msg,
		seqUser:         seqUser,
		seqConversation: seqConversation,
		producerToMongo: mongoProducer,
		producerToPush:  pushProducer,
	}, nil
}

type msgTransferDatabase struct {
	msgDocDatabase  database.Msg
	msgTable        model.MsgDocModel
	msgCache        cache.MsgCache
	seqConversation cache.SeqConversationCache
	seqUser         cache.SeqUser
	producerToMongo mq.Producer
	producerToPush  mq.Producer
}

func (db *msgTransferDatabase) BatchInsertChat2DB(ctx context.Context, conversationID string, msgList []*sdkws.MsgData, currentMaxSeq int64) error {
	if len(msgList) == 0 {
		return errs.ErrArgs.WrapMsg("msgList is empty")
	}
	msgs := make([]any, len(msgList))
	seqs := make([]int64, len(msgList))
	for i, msg := range msgList {
		if msg == nil {
			continue
		}
		seqs[i] = msg.Seq
		if msg.Status == constant.MsgStatusSending {
			msg.Status = constant.MsgStatusSendSuccess
		}
		msgs[i] = convert.MsgPb2DB(msg)
	}
	// msgList[0].Seq就是会话上一次消息组的最后一条消息的序列号
	if err := db.BatchInsertBlock(ctx, conversationID, msgs, updateKeyMsg, msgList[0].Seq); err != nil {
		return err
	}
	//return db.msgCache.DelMessageBySeqs(ctx, conversationID, seqs)
	return nil
}

func (db *msgTransferDatabase) BatchInsertBlock(ctx context.Context, conversationID string, fields []any, key int8, firstSeq int64) error {
	if len(fields) == 0 {
		return nil
	}
	num := db.msgTable.GetSingleGocMsgNum()
	// num = 100
	// 遍历每条消息
	for i, field := range fields { // Check the type of the field
		var ok bool
		switch key {
		case updateKeyMsg:
			var msg *model.MsgDataModel
			msg, ok = field.(*model.MsgDataModel)
			// msg.Seq != firstSeq+int64(i)判断序列号是否连续
			if msg != nil && msg.Seq != firstSeq+int64(i) {
				return errs.ErrInternalServer.WrapMsg("seq is invalid")
			}
		case updateKeyRevoke:
			_, ok = field.(*model.RevokeModel)
		default:
			return errs.ErrInternalServer.WrapMsg("key is invalid")
		}
		if !ok {
			return errs.ErrInternalServer.WrapMsg("field type is invalid")
		}
	}
	// Returns true if the document exists in the database, false if the document does not exist in the database
	updateMsgModel := func(seq int64, i int) (bool, error) {
		var (
			res *mongo.UpdateResult
			err error
		)
		// 因为消息在mongo中是分组（按照数组）存储到mongo的一条记录中 - 这样将消息按照数组存储到mongo记录中，相比单条消息存储一条记录，可以减少mongo索引的开销
		/*
			1. 通过conversation_id和seq计算doc_id，表示放在哪条记录 -> GetDocID
			2. 通过seq计算index，表示放在记录数组中的第几个元素上 -> GetMsgIndex
		*/
		docID := db.msgTable.GetDocID(conversationID, seq)
		index := db.msgTable.GetMsgIndex(seq)
		field := fields[i]
		switch key {
		// 存储消息到mongo
		case updateKeyMsg:
			res, err = db.msgDocDatabase.UpdateMsg(ctx, docID, index, "msg", field)
		case updateKeyRevoke:
			res, err = db.msgDocDatabase.UpdateMsg(ctx, docID, index, "revoke", field)
		}
		if err != nil {
			return false, err
		}
		return res.MatchedCount > 0, nil
	}
	tryUpdate := true
	// 遍历每条消息
	for i := 0; i < len(fields); i++ {
		// 当前消息的序列号
		seq := firstSeq + int64(i) // Current sequence number
		if tryUpdate {
			matched, err := updateMsgModel(seq, i)
			if err != nil {
				return err
			}
			// 存储消息到mongo - 在已有mongo doc上更新存入
			if matched {
				continue // The current data has been updated, skip the current data
			}
		}
		// 存储消息到mongo - 没有匹配的mongo doc，需要新建mongo doc后再存入(即存储到新的mongo doc中)
		doc := model.MsgDocModel{
			DocID: db.msgTable.GetDocID(conversationID, seq),
			Msg:   make([]*model.MsgInfoModel, num),
		}
		var insert int // Inserted data number
		// 在新创建的mongo doc中存入num条消息到消息数组 - 即一个mongo doc最多存储num条消息 - num设置是100
		for j := i; j < len(fields); j++ {
			seq = firstSeq + int64(j)
			// 计算 - 把属于同一个mongo doc的消息塞入到doc消息数组中
			if db.msgTable.GetDocID(conversationID, seq) != doc.DocID {
				break
			}
			insert++
			switch key {
			case updateKeyMsg:
				doc.Msg[db.msgTable.GetMsgIndex(seq)] = &model.MsgInfoModel{
					Msg: fields[j].(*model.MsgDataModel),
				}
			case updateKeyRevoke:
				doc.Msg[db.msgTable.GetMsgIndex(seq)] = &model.MsgInfoModel{
					Revoke: fields[j].(*model.RevokeModel),
				}
			}
		}
		// 遍历计算好的mongo doc消息数组
		for i, msgInfo := range doc.Msg {
			if msgInfo == nil {
				msgInfo = &model.MsgInfoModel{}
				doc.Msg[i] = msgInfo
			}
			if msgInfo.DelList == nil {
				doc.Msg[i].DelList = []string{}
			}
		}
		// 存储到mongo
		if err := db.msgDocDatabase.Create(ctx, &doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				i--              // already inserted
				tryUpdate = true // next block use update mode
				continue
			}
			return err
		}
		tryUpdate = false // The current block is inserted successfully, and the next block is inserted preferentially
		i += insert - 1   // Skip the inserted data
	}
	return nil
}

func (db *msgTransferDatabase) DeleteMessagesFromCache(ctx context.Context, conversationID string, seqs []int64) error {
	return db.msgCache.DelMessageBySeqs(ctx, conversationID, seqs)
}

func (db *msgTransferDatabase) BatchInsertChat2Cache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (seq int64, isNew bool, userHasReadMap map[string]int64, err error) {
	lenList := len(msgs)
	if int64(lenList) > db.msgTable.GetSingleGocMsgNum() {
		return 0, false, nil, errs.New("message count exceeds limit", "limit", db.msgTable.GetSingleGocMsgNum()).Wrap()
	}
	if lenList < 1 {
		return 0, false, nil, errs.New("no messages to insert", "minCount", 1).Wrap()
	}
	// 分配会话序列号seq，从0（currentMaxSeq从0开始）开始（从redis分配 - currentMaxSeq是当前会话的上一次消息的序列号）
	currentMaxSeq, err := db.seqConversation.Malloc(ctx, conversationID, int64(len(msgs)))
	if err != nil {
		log.ZError(ctx, "storage.seq.Malloc", err)
		return 0, false, nil, err
	}
	// 会话序列号0表示是新的会话
	isNew = currentMaxSeq == 0
	lastMaxSeq := currentMaxSeq
	userSeqMap := make(map[string]int64)
	seqs := make([]int64, 0, lenList)
	for _, m := range msgs {
		// 会话序列号连续累加递增，说明会话的序列号是从1开始连续增加的
		currentMaxSeq++
		// 把分配的序列号塞入到消息中
		m.Seq = currentMaxSeq
		// userSeqMap记录消息组中同一个发送者在本次会话中最大的消息序列号，用于标记发送者对本次会话的消息组中的消息序列号小于/等于m.Seq的消息已读。
		userSeqMap[m.SendID] = m.Seq
		seqs = append(seqs, m.Seq)
	}
	msgToDB := func(msg *sdkws.MsgData) *model.MsgInfoModel {
		return &model.MsgInfoModel{
			Msg: convert.MsgPb2DB(msg),
		}
	}
	// 按照会话conversation_id和消息seq作为redis key一条一条的保存到redis（string）
	if err := db.msgCache.SetMessageBySeqs(ctx, conversationID, datautil.Slice(msgs, msgToDB)); err != nil {
		return 0, false, nil, err
	}
	return lastMaxSeq, isNew, userSeqMap, nil
}

func (db *msgTransferDatabase) SetHasReadSeqs(ctx context.Context, conversationID string, userSeqMap map[string]int64) error {
	for userID, seq := range userSeqMap {
		if err := db.seqUser.SetUserReadSeq(ctx, conversationID, userID, seq); err != nil {
			return err
		}
	}
	return nil
}

func (db *msgTransferDatabase) SetHasReadSeqToDB(ctx context.Context, conversationID string, userSeqMap map[string]int64) error {
	for userID, seq := range userSeqMap {
		if err := db.seqUser.SetUserReadSeqToDB(ctx, conversationID, userID, seq); err != nil {
			return err
		}
	}
	return nil
}

func (db *msgTransferDatabase) MsgToPushMQ(ctx context.Context, key, conversationID string, msg2mq *sdkws.MsgData) error {
	data, err := proto.Marshal(&pbmsg.PushMsgDataToMQ{MsgData: msg2mq, ConversationID: conversationID})
	if err != nil {
		return err
	}
	if err := db.producerToPush.SendMessage(ctx, key, data); err != nil {
		log.ZError(ctx, "MsgToPushMQ", err, "key", key, "conversationID", conversationID)
		return err
	}
	return nil
}

func (db *msgTransferDatabase) MsgToMongoMQ(ctx context.Context, key, conversationID string, messages []*sdkws.MsgData, lastSeq int64) error {
	if len(messages) > 0 {
		data, err := proto.Marshal(&pbmsg.MsgDataToMongoByMQ{LastSeq: lastSeq, ConversationID: conversationID, MsgData: messages})
		if err != nil {
			return err
		}
		if err := db.producerToMongo.SendMessage(ctx, key, data); err != nil {
			log.ZError(ctx, "MsgToMongoMQ", err, "key", key, "conversationID", conversationID, "lastSeq", lastSeq)
			return err
		}
	}
	return nil
}
