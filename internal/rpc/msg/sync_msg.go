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
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/timeutil"

	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/log"
)

/*
消息SQLite本地存储
消息可靠性解决方案：https://zhuanlan.zhihu.com/p/403828360
消息增加一条序列号，为每条消息分配一个消息ID（OpenIM便是使用的这种方案），对于每个用户而言，在自己的消息收件箱中，
消息的收取队列中seq应该是连续的，如果客户端层面，或者网络原因造成消息没能够实时的到达，客户端可以在应用层增加逻辑采取某种策略通过seq对比，
本地的seq和服务器的seq之间的差值去拉取没能正确交付到客户端的消息，这种方案相比于ACK消息来说，不用每一条消息都需要server判定ACK来确认消
息是否已经到达客户端，消息的获取获取逻辑更加简单，但是消息的拉取依赖于客户端，如果pull拉取频率过于高，其流量的损耗的也是不低的，
所以OpenIM在客户端断网重连，消息拉取策略上做了优化处理以确保消息拉取频率在一个合适的范围。

消息同步&对齐seq https://zhuanlan.zhihu.com/p/394077398
由于网络的波动以及负责的网络环境，导致消息推送存在不确定性。OpenIM采用local seq和server seq消息对齐，同时结合拉取和推送的方式，
简单高效地解决了消息的可靠性问题。这里分两种场景进行表述：
（1）客户端接收推送消息时，比如客户端收到推送消息的seq为100，如果local seq为99，因为seq递增且连续，所以消息正常显示即可。
	如果local seq大于100，说明重复推送了消息，抛弃此消息即可。如果local seq小于99，说明中间有历史消息丢失，拉取(local seq+1, 100)的消息，进行补齐即可；
（2）用户在登录、或者断网重连时，客户端会从服务端拉取最大seq(max seq)，读取客户端本地seq(local seq)，如果local seq 小于 max seq，
	说明存在历史消息未同步的情况，调用接口同步自身收件箱[local seq+1, max seq]的数据完成消息对齐。
*/

/*
v2.3.3
消息同步PullMessageBySeqList过程如下：
type PullMessageBySeqsReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserID    string      `protobuf:"bytes,1,opt,name=userID,proto3" json:"userID"`
	SeqRanges []*SeqRange `protobuf:"bytes,2,rep,name=seqRanges,proto3" json:"seqRanges"`
	Order     PullOrder   `protobuf:"varint,3,opt,name=order,proto3,enum=openim.sdkws.PullOrder" json:"order"`
}
type SeqRange struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConversationID string `protobuf:"bytes,1,opt,name=conversationID,proto3" json:"conversationID"`
	Begin          int64  `protobuf:"varint,2,opt,name=begin,proto3" json:"begin"`
	End            int64  `protobuf:"varint,3,opt,name=end,proto3" json:"end"`
	Num            int64  `protobuf:"varint,4,opt,name=num,proto3" json:"num"`
}
1. 根据发送的请求，获取user_id多个会话的消息，每个conversation_id设置了seq开始结束及条数
2. 调用msg服务，msg调用conversation服务获取对应的conversation信息，并保存在msg服务内存中
3. 从redis中获取conversation对应的max/min seq及msg(先从redis中获取消息，未获取到的再从mongo中获取)
4. 返回结果map[conversation_id]*msg

注意v2.3.3从mongo查询消息的过程如下GetMsgBySeqListMongo2：
// 从收件箱存储可知，查询时也只需关系用户对应的所有seq（不关conversation）
m := func(uid string, seqList []uint32) map[string][]uint32 {
		t := make(map[string][]uint32)
		for i := 0; i < len(seqList); i++ {
			seqUid := getSeqUid(uid, seqList[i])
			if value, ok := t[seqUid]; !ok {
				var temp []uint32
				t[seqUid] = append(temp, seqList[i])
			} else {
				t[seqUid] = append(value, seqList[i])
			}
		}
		return t
	}(uid, seqList)

	// 遍历每个seq_uid
	for seqUid, value := range m {
		if err = c.FindOne(ctx, bson.M{"uid": seqUid}).Decode(&sChat); err != nil {
			log.NewError(operationID, "not find seqUid", seqUid, value, uid, seqList, err.Error())
			continue
		}
		singleCount = 0
		for i := 0; i < len(sChat.Msg); i++ {
			msg := new(open_im_sdk.MsgData)
			if err = proto.Unmarshal(sChat.Msg[i].Msg, msg); err != nil {
				log.NewError(operationID, "Unmarshal err", seqUid, value, uid, seqList, err.Error())
				return nil, err
			}
			if isContainInt32(msg.Seq, value) {
				seqMsg = append(seqMsg, msg)
				hasSeqList = append(hasSeqList, msg.Seq)
				singleCount++
				if singleCount == len(value) {
					break
				}
			}
		}
	}
*/

func (m *msgServer) PullMessageBySeqs(ctx context.Context, req *sdkws.PullMessageBySeqsReq) (*sdkws.PullMessageBySeqsResp, error) {
	resp := &sdkws.PullMessageBySeqsResp{}
	resp.Msgs = make(map[string]*sdkws.PullMsgs)
	resp.NotificationMsgs = make(map[string]*sdkws.PullMsgs)
	for _, seq := range req.SeqRanges {
		if !msgprocessor.IsNotification(seq.ConversationID) {
			conversation, err := m.ConversationLocalCache.GetConversation(ctx, req.UserID, seq.ConversationID)
			if err != nil {
				log.ZError(ctx, "GetConversation error", err, "conversationID", seq.ConversationID)
				continue
			}
			minSeq, maxSeq, msgs, err := m.MsgDatabase.GetMsgBySeqsRange(ctx, req.UserID, seq.ConversationID,
				seq.Begin, seq.End, seq.Num, conversation.MaxSeq)
			if err != nil {
				log.ZWarn(ctx, "GetMsgBySeqsRange error", err, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			var isEnd bool
			switch req.Order {
			case sdkws.PullOrder_PullOrderAsc:
				isEnd = maxSeq <= seq.End
			case sdkws.PullOrder_PullOrderDesc:
				isEnd = seq.Begin <= minSeq
			}
			if len(msgs) == 0 {
				log.ZWarn(ctx, "not have msgs", nil, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			resp.Msgs[seq.ConversationID] = &sdkws.PullMsgs{Msgs: msgs, IsEnd: isEnd}
		} else {
			var seqs []int64
			for i := seq.Begin; i <= seq.End; i++ {
				seqs = append(seqs, i)
			}
			minSeq, maxSeq, notificationMsgs, err := m.MsgDatabase.GetMsgBySeqs(ctx, req.UserID, seq.ConversationID, seqs)
			if err != nil {
				log.ZWarn(ctx, "GetMsgBySeqs error", err, "conversationID", seq.ConversationID, "seq", seq)

				continue
			}
			var isEnd bool
			switch req.Order {
			case sdkws.PullOrder_PullOrderAsc:
				isEnd = maxSeq <= seq.End
			case sdkws.PullOrder_PullOrderDesc:
				isEnd = seq.Begin <= minSeq
			}
			if len(notificationMsgs) == 0 {
				log.ZWarn(ctx, "not have notificationMsgs", nil, "conversationID", seq.ConversationID, "seq", seq)

				continue
			}
			resp.NotificationMsgs[seq.ConversationID] = &sdkws.PullMsgs{Msgs: notificationMsgs, IsEnd: isEnd}
		}
	}
	return resp, nil
}

/*
v2.3.3
获取一个用户的max_seq/min_seq流程如下：
1. msg RPC调用conversation获取user_id对应的conversation_ids，本在本地local lru(服务内存中)中存储这些conversation_ids
2. 从redis中获取conversation_id对应的max_seq (因为set max seq是在msg-transfer服务中执行的，但这里又直接访问的redis，所以这两个服务共用了redis)
*/

func (m *msgServer) GetMaxSeq(ctx context.Context, req *sdkws.GetMaxSeqReq) (*sdkws.GetMaxSeqResp, error) {
	if err := authverify.CheckAccessV3(ctx, req.UserID, m.config.Share.IMAdminUserID); err != nil {
		return nil, err
	}
	// 从conversation cache中获取当前user id对应的所有conversation ids
	conversationIDs, err := m.ConversationLocalCache.GetConversationIDs(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	for _, conversationID := range conversationIDs {
		conversationIDs = append(conversationIDs, conversationutil.GetNotificationConversationIDByConversationID(conversationID))
	}
	conversationIDs = append(conversationIDs, conversationutil.GetSelfNotificationConversationID(req.UserID))
	log.ZDebug(ctx, "GetMaxSeq", "conversationIDs", conversationIDs)
	maxSeqs, err := m.MsgDatabase.GetMaxSeqs(ctx, conversationIDs)
	if err != nil {
		log.ZWarn(ctx, "GetMaxSeqs error", err, "conversationIDs", conversationIDs, "maxSeqs", maxSeqs)
		return nil, err
	}
	resp := new(sdkws.GetMaxSeqResp)
	resp.MaxSeqs = maxSeqs
	return resp, nil
}

func (m *msgServer) SearchMessage(ctx context.Context, req *msg.SearchMessageReq) (resp *msg.SearchMessageResp, err error) {
	var chatLogs []*sdkws.MsgData
	var total int32
	resp = &msg.SearchMessageResp{}
	if total, chatLogs, err = m.MsgDatabase.SearchMessage(ctx, req); err != nil {
		return nil, err
	}

	var (
		sendIDs  []string
		recvIDs  []string
		groupIDs []string
		sendMap  = make(map[string]string)
		recvMap  = make(map[string]string)
		groupMap = make(map[string]*sdkws.GroupInfo)
	)
	for _, chatLog := range chatLogs {
		if chatLog.SenderNickname == "" {
			sendIDs = append(sendIDs, chatLog.SendID)
		}
		switch chatLog.SessionType {
		case constant.SingleChatType, constant.NotificationChatType:
			recvIDs = append(recvIDs, chatLog.RecvID)
		case constant.WriteGroupChatType, constant.ReadGroupChatType:
			groupIDs = append(groupIDs, chatLog.GroupID)
		}
	}
	// Retrieve sender and receiver information
	if len(sendIDs) != 0 {
		sendInfos, err := m.UserLocalCache.GetUsersInfo(ctx, sendIDs)
		if err != nil {
			return nil, err
		}
		for _, sendInfo := range sendInfos {
			sendMap[sendInfo.UserID] = sendInfo.Nickname
		}
	}
	if len(recvIDs) != 0 {
		recvInfos, err := m.UserLocalCache.GetUsersInfo(ctx, recvIDs)
		if err != nil {
			return nil, err
		}
		for _, recvInfo := range recvInfos {
			recvMap[recvInfo.UserID] = recvInfo.Nickname
		}
	}

	// Retrieve group information including member counts
	if len(groupIDs) != 0 {
		groupInfos, err := m.GroupLocalCache.GetGroupInfos(ctx, groupIDs)
		if err != nil {
			return nil, err
		}
		for _, groupInfo := range groupInfos {
			groupMap[groupInfo.GroupID] = groupInfo
			// Get actual member count
			memberIDs, err := m.GroupLocalCache.GetGroupMemberIDs(ctx, groupInfo.GroupID)
			if err == nil {
				groupInfo.MemberCount = uint32(len(memberIDs)) // Update the member count with actual number
			}
		}
	}
	// Construct response with updated information
	for _, chatLog := range chatLogs {
		pbchatLog := &msg.ChatLog{}
		datautil.CopyStructFields(pbchatLog, chatLog)
		pbchatLog.SendTime = chatLog.SendTime
		pbchatLog.CreateTime = chatLog.CreateTime
		if chatLog.SenderNickname == "" {
			pbchatLog.SenderNickname = sendMap[chatLog.SendID]
		}
		switch chatLog.SessionType {
		case constant.SingleChatType, constant.NotificationChatType:
			pbchatLog.RecvNickname = recvMap[chatLog.RecvID]
		case constant.WriteGroupChatType, constant.ReadGroupChatType:
			groupInfo := groupMap[chatLog.GroupID]
			pbchatLog.SenderFaceURL = groupInfo.FaceURL
			pbchatLog.GroupMemberCount = groupInfo.MemberCount // Reflects actual member count
			pbchatLog.RecvID = groupInfo.GroupID
			pbchatLog.GroupName = groupInfo.GroupName
			pbchatLog.GroupOwner = groupInfo.OwnerUserID
			pbchatLog.GroupType = groupInfo.GroupType
		}
		resp.ChatLogs = append(resp.ChatLogs, pbchatLog)
	}
	resp.ChatLogsNum = total
	return resp, nil
}

func (m *msgServer) GetServerTime(ctx context.Context, _ *msg.GetServerTimeReq) (*msg.GetServerTimeResp, error) {
	return &msg.GetServerTimeResp{ServerTime: timeutil.GetCurrentTimestampByMill()}, nil
}
