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

package msg

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/constant"
	pbconversation "github.com/openimsdk/protocol/conversation"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/protocol/wrapperspb"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/stringutil"
)

/*
	心跳处理：
		1. 前端发送ping,服务端回pong
		2. 后端发送ping，前端回pong
		3. 心跳异常时，连接主动断开是前端执行的，这样心跳管理更为简单
*/

// 接收msg-gate的消息调用

/*
	https://cloud.baidu.com/article/293728
	https://www.cnblogs.com/OpenIM/p/15067721.html
	https://juejin.cn/post/7072593747438993444

	在v2.3.3版本中，采用的是写扩散方案：
	对于私聊，消息拆分 1—>2，方便写入各自的收件箱
	对于群发，获取群用户列表后，sendMsgToGroupOptimization 将群发消息1->N条后进行单条转发(通过对群成员分组并发执行-20个user一组启动一个goroutine)
	私聊和群聊拆分后的单条消息，发送到kafka的消息key都是单个的user_id（而且v2.3.3中整个系统的kafka消息发送过程中，消息发送的key都是一样的。）
	消息发送到msg-transfer后，采用收件箱的方案存储消息到mongo document，document结构如下：
	type UserChat struct {
		UID string
		Msg []MsgInfo
	}
	type MsgInfo struct {
		SendTime int64
		Msg      []byte // 一条消息内容
	}
	从getSeqUid该函数可知，为每个uid（每个群/发送者）设置了5000个收件箱（但是好像seq_uid_next一直是insert，这样会超过5000， 那么不应该是5000个收件箱；
	只是这些不同的document中uid字段可能相同。v2.3.3中通过定时任务清理需要过期的收件箱消息），通过getSeqUid函数生成索引标识来定位seq属于哪个收件箱，每个收件箱是一个msg info数组。
	一个收件箱大小是5000条msg
*/
// 注意在v2.3.3中所有的conversation好像是共用的seq，即redis中存储的user max seq是对所有的conversation而言的！！！(这是写扩散收件箱模式)（应该还是要做成单独conversation对应单独的seq，这样查询消息时通过conversation_id和seq进行拉取 -- 这是读扩散模式）
/*
v2.3.3 msg-transfer服务批量插入mongo逻辑如下：
给每个用户设置了一个收件箱组（5000个收件箱，收件箱0 ~ 收件箱4999），限制了每次保存的消息量不超过5000条。 --- 好像没有限制收件箱大小为5000个docs，只是显示了每个doc存储5000条消息
1. 计算mongo document中数组剩余容量（根据last max seq计算），具体如下：
blk0 := uint64(GetSingleGocMsgNum() - 1) // document中数组下标从0开始
if currentMaxSeq < uint64(GetSingleGocMsgNum()) {
// 计算第一个收件箱（收件箱0）余量
// currentMaxSeq指的是上一次结束的max seq(因为发送过程中是保存到redis成功后再发送消息到mongo保存，所以currentMaxSeq最小是1)
remain = blk0 - currentMaxSeq //1
} else {
// 计算收件箱组中非收件箱0的第一个未填满的收件箱的余量
excludeBlk0 := currentMaxSeq - blk0 //=1
//(5000-1)%5000 == 4999
remain = (uint64(GetSingleGocMsgNum()) - (excludeBlk0 % uint64(GetSingleGocMsgNum()))) % uint64(GetSingleGocMsgNum())
}
2. 遍历消息列表，把每条消息放入到document数组中，如果当前收件箱余量被填满用完，则把多的消息填入到新的document数组中，代码如下：
if insertCounter < remain {
// 上一个未填满的收件箱
msgListToMongo = append(msgListToMongo, sMsg)
insertCounter++
 //seq_uid = string(user_id:currentMaxSeq/5000), 定位到这条消息应该哪个收件箱（收件箱0 ~ 收件箱4999），
 //（注意这里的currentMaxSeq在计算时与conversation_id无关，即在往收件箱存储消息时，无关conversation只要seq_uid计算结果相同就存在一个document）
 //  这里的user_id指的是kafka消息发送的key指，可能是group_id/sender_id（sender_receiver_id）
 //  seq_uid对应的就是UserChat模型中的uid。
 //  那么if这个分支中的所有msg的seq_uid是相同的；同理下面else分支中的所有的msg的seq_uid_next也是相同的。
 //  既然seq_uid的作用是定位到具体的收件箱，那么保存消息时把消息列表追加/插入到对应的document中。
 //  由于每次批量保存的消息量限制不超过5000条，那么每次最多同时更新两个document（msgListToMongo/msgListToMongoNext）。
seqUid = getSeqUid(userID, uint32(currentMaxSeq))
log.Debug(operationID, "msgListToMongo ", seqUid, m.MsgData.Seq, m.MsgData.ClientMsgID, insertCounter, remain, "userID: ", userID)
} else {
// 需要一个新的收件箱
msgListToMongoNext = append(msgListToMongoNext, sMsg)
// seq_uid_next对应的一定是不存在的document记录，所以这种情况只需要执行insert操作
seqUidNext = getSeqUid(userID, uint32(currentMaxSeq))
log.Debug(operationID, "msgListToMongoNext ", seqUidNext, m.MsgData.Seq, m.MsgData.ClientMsgID, insertCounter, remain, "userID: ", userID)
}

3. 依次判断seq_uid和seq_uid_next是否为空，若对应的seq_uid不是空，则保存对应的document数组。
如果seq_uid不为空，则对应的document记录存在性不确定，则首先通过FindAndUpdate进行消息列表追加，如果是记录不存在错误，则执行Insert操作。
如果seq_uid_next不为空，则说明对应的document记录一定不存在，只需要执行Insert操作。
*/

// 不管是私聊还是群聊都是基于会话的前提

func (m *msgServer) SendMsg(ctx context.Context, req *pbmsg.SendMsgReq) (*pbmsg.SendMsgResp, error) {
	if req.MsgData != nil {
		m.encapsulateMsgData(req.MsgData)
		switch req.MsgData.SessionType {
		case constant.SingleChatType:
			return m.sendMsgSingleChat(ctx, req)
		case constant.NotificationChatType:
			return m.sendMsgNotification(ctx, req)
		case constant.ReadGroupChatType:
			return m.sendMsgSuperGroupChat(ctx, req)
		default:
			return nil, errs.ErrArgs.WrapMsg("unknown sessionType")
		}
	}
	return nil, errs.ErrArgs.WrapMsg("msgData is nil")
}

func (m *msgServer) sendMsgSuperGroupChat(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	if err = m.messageVerification(ctx, req); err != nil {
		prommetrics.GroupChatMsgProcessFailedCounter.Inc()
		return nil, err
	}

	if err = m.webhookBeforeSendGroupMsg(ctx, &m.config.WebhooksConfig.BeforeSendGroupMsg, req); err != nil {
		return nil, err
	}
	if err := m.webhookBeforeMsgModify(ctx, &m.config.WebhooksConfig.BeforeMsgModify, req); err != nil {
		return nil, err
	}
	err = m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForGroup(req.MsgData.GroupID), req.MsgData)
	if err != nil {
		return nil, err
	}
	if req.MsgData.ContentType == constant.AtText {
		go m.setConversationAtInfo(ctx, req.MsgData)
	}

	m.webhookAfterSendGroupMsg(ctx, &m.config.WebhooksConfig.AfterSendGroupMsg, req)
	prommetrics.GroupChatMsgProcessSuccessCounter.Inc()
	resp = &pbmsg.SendMsgResp{}
	resp.SendTime = req.MsgData.SendTime
	resp.ServerMsgID = req.MsgData.ServerMsgID
	resp.ClientMsgID = req.MsgData.ClientMsgID
	return resp, nil
}

func (m *msgServer) setConversationAtInfo(nctx context.Context, msg *sdkws.MsgData) {
	log.ZDebug(nctx, "setConversationAtInfo", "msg", msg)
	ctx := mcontext.NewCtx("@@@" + mcontext.GetOperationID(nctx))
	var atUserID []string
	conversation := &pbconversation.ConversationReq{
		ConversationID:   msgprocessor.GetConversationIDByMsg(msg),
		ConversationType: msg.SessionType,
		GroupID:          msg.GroupID,
	}
	tagAll := datautil.Contain(constant.AtAllString, msg.AtUserIDList...)
	if tagAll {
		memberUserIDList, err := m.GroupLocalCache.GetGroupMemberIDs(ctx, msg.GroupID)
		if err != nil {
			log.ZWarn(ctx, "GetGroupMemberIDs", err)
			return
		}
		atUserID = stringutil.DifferenceString([]string{constant.AtAllString}, msg.AtUserIDList)
		if len(atUserID) == 0 { // just @everyone
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll}
		} else { // @Everyone and @other people
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAllAtMe}
			err = m.Conversation.SetConversations(ctx, atUserID, conversation)
			if err != nil {
				log.ZWarn(ctx, "SetConversations", err, "userID", atUserID, "conversation", conversation)
			}
			memberUserIDList = stringutil.DifferenceString(atUserID, memberUserIDList)
		}
		conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll}
		err = m.Conversation.SetConversations(ctx, memberUserIDList, conversation)
		if err != nil {
			log.ZWarn(ctx, "SetConversations", err, "userID", memberUserIDList, "conversation", conversation)
		}
	}
	conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtMe}
	err := m.Conversation.SetConversations(ctx, msg.AtUserIDList, conversation)
	if err != nil {
		log.ZWarn(ctx, "SetConversations", err, msg.AtUserIDList, conversation)
	}
}

func (m *msgServer) sendMsgNotification(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	if err := m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil {
		return nil, err
	}
	resp = &pbmsg.SendMsgResp{
		ServerMsgID: req.MsgData.ServerMsgID,
		ClientMsgID: req.MsgData.ClientMsgID,
		SendTime:    req.MsgData.SendTime,
	}
	return resp, nil
}

func (m *msgServer) sendMsgSingleChat(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	if err := m.messageVerification(ctx, req); err != nil {
		return nil, err
	}
	isSend := true
	isNotification := msgprocessor.IsNotificationByMsg(req.MsgData)
	if !isNotification {
		// 判断是否发送
		isSend, err = m.modifyMessageByUserMessageReceiveOpt(
			ctx,
			req.MsgData.RecvID,
			conversationutil.GenConversationIDForSingle(req.MsgData.SendID, req.MsgData.RecvID),
			constant.SingleChatType,
			req,
		)
		if err != nil {
			return nil, err
		}
	}
	if !isSend {
		prommetrics.SingleChatMsgProcessFailedCounter.Inc()
		return nil, nil
	} else {
		// 消息需要发送
		if err = m.webhookBeforeSendSingleMsg(ctx, &m.config.WebhooksConfig.BeforeSendSingleMsg, req); err != nil {
			return nil, err
		}
		if err := m.webhookBeforeMsgModify(ctx, &m.config.WebhooksConfig.BeforeMsgModify, req); err != nil {
			return nil, err
		}

		// msg模块通过kafka发送消息到 msg-transfer (topic: ToRedisTopic)， GenConversationUniqueKeyForSingle构造消息发送的key:send_id 和 receive_id（这和后面构建conversation_id:msgprocessor.GetChatConversationIDByMsg(ctxMsgList[0].message)相对应）
		if err := m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil {
			prommetrics.SingleChatMsgProcessFailedCounter.Inc()
			return nil, err
		}
		m.webhookAfterSendSingleMsg(ctx, &m.config.WebhooksConfig.AfterSendSingleMsg, req)
		prommetrics.SingleChatMsgProcessSuccessCounter.Inc()
		return &pbmsg.SendMsgResp{
			ServerMsgID: req.MsgData.ServerMsgID,
			ClientMsgID: req.MsgData.ClientMsgID,
			SendTime:    req.MsgData.SendTime,
		}, nil
	}
}
