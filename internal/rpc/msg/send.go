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

// 接收msg-gate的消息调用

/*
	https://cloud.baidu.com/article/293728
	在v2.3.3版本中，群发采用的是写扩散方案：
	对于群发，获取群用户列表后，sendMsgToGroupOptimization 将群发消息1->N条后进行单条转发(通过对群成员分组并发执行-20个user一组启动一个goroutine)
	消息发送到msg-transfer后，采用收件箱的方案存储消息到mongo document，document结构如下：
	type UserChat struct {
		UID string
		Msg []MsgInfo
	}
	type MsgInfo struct {
		SendTime int64
		Msg      []byte // 一条消息内容
	}
	从getSeqUid该函数可知，一个会话的收件箱大小是5000条msg（即一个会话只存储5000条消息），每条消息通过seq和会话id计算得到seqUid作为document的uid更新对应msg数组
*/

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

		// msg模块通过kafka发送消息到 msg-transfer (topic: ToRedisTopic)， GenConversationUniqueKeyForSingle构造消息发送的key:send_id 和 receive_id
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
