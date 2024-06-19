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

package msggateway

import (
	"context"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcclient"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/push"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/utils/jsonutil"
	"google.golang.org/protobuf/proto"
)

/*
消息体结构说明
type Req struct {
	// 请求类型：发送消息 / 获取最新seq(max_seq) / 获取消息seq list
	ReqType int32
	Token         string
	SendID        string
	// 消息体，不同ReqIdentifier对应的消息体不一样
	Data []byte
}

// Data -> 获取最新seq(max_seq)
type GetMaxSeqReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	UserID string `protobuf:"bytes,1,opt,name=userID,proto3" json:"userID"`
}

// Data -> 发送消息
type MsgData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	SendID           string           `protobuf:"bytes,1,opt,name=sendID,proto3" json:"sendID"`
	RecvID           string           `protobuf:"bytes,2,opt,name=recvID,proto3" json:"recvID"`
	GroupID          string           `protobuf:"bytes,3,opt,name=groupID,proto3" json:"groupID"`
	// 生成规则 md5(纳秒字时间戳符串+send_id+随机纳秒时间戳字符串)
	ClientMsgID      string           `protobuf:"bytes,4,opt,name=clientMsgID,proto3" json:"clientMsgID"`
	// 生成规则 在msg服务 md5(date格式字符串 + "-" + sendID + "-" + strconv.Itoa(rand.Int()))
	ServerMsgID      string           `protobuf:"bytes,5,opt,name=serverMsgID,proto3" json:"serverMsgID"`
	// ios/andorid / web
	SenderPlatformID int32            `protobuf:"varint,6,opt,name=senderPlatformID,proto3" json:"senderPlatformID"`
	SenderNickname   string           `protobuf:"bytes,7,opt,name=senderNickname,proto3" json:"senderNickname"`
	SenderFaceURL    string           `protobuf:"bytes,8,opt,name=senderFaceURL,proto3" json:"senderFaceURL"`
	// 私聊/群聊/通知
	SessionType      int32            `protobuf:"varint,9,opt,name=sessionType,proto3" json:"sessionType"`
	// 用户消息/系统消息
	MsgFrom          int32            `protobuf:"varint,10,opt,name=msgFrom,proto3" json:"msgFrom"`
	// text文本 / picture图片 / file文件 / 系统通知消息 / 合并消息
	// 备注消息功能包括：多端同步 消息撤回 @功能 已读 本地消息搜索 历史消息
	ContentType      int32            `protobuf:"varint,11,opt,name=contentType,proto3" json:"contentType"`
	// 不同的content type对应的content结构不同
	Content          []byte           `protobuf:"bytes,12,opt,name=content,proto3" json:"content"`
	// 生成规则 在msg-transfer服务批量插入redis/mongo时进行++， seq是从1开始的（
	插入mongo是重新根据之前的last max seq++，以便和查询redis时++产生的seq对比，进行redis消息删除）
	Seq              int64            `protobuf:"varint,14,opt,name=seq,proto3" json:"seq"`
	// 在msg服务生成毫秒时间戳
	SendTime         int64            `protobuf:"varint,15,opt,name=sendTime,proto3" json:"sendTime"`
	CreateTime       int64            `protobuf:"varint,16,opt,name=createTime,proto3" json:"createTime"`
	// 是否删除
	Status           int32            `protobuf:"varint,17,opt,name=status,proto3" json:"status"`
	IsRead           bool             `protobuf:"varint,18,opt,name=isRead,proto3" json:"isRead"`
	// 消息的设置项：如isHistory
	Options          map[string]bool  `protobuf:"bytes,19,rep,name=options,proto3" json:"options,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	OfflinePushInfo  *OfflinePushInfo `protobuf:"bytes,20,opt,name=offlinePushInfo,proto3" json:"offlinePushInfo"`
	AtUserIDList     []string         `protobuf:"bytes,21,rep,name=atUserIDList,proto3" json:"atUserIDList"`
	AttachedInfo     string           `protobuf:"bytes,22,opt,name=attachedInfo,proto3" json:"attachedInfo"`
	Ex               string           `protobuf:"bytes,23,opt,name=ex,proto3" json:"ex"`
}

// Content消息结构定义在open im sdk core代码的sdk_struct中，部分结构如下
// 文本消息
type TextElem struct {
	Content string `json:"content"`
}

// 图片消息Content
type PictureBaseInfo struct {
	UUID   string `json:"uuid,omitempty"`
	Type   string `json:"type,omitempty"`
	Size   int64  `json:"size"`
	Width  int32  `json:"width"`
	Height int32  `json:"height"`
	Url    string `json:"url,omitempty"`
}

type PictureElem struct {
	SourcePath      string           `json:"sourcePath,omitempty"`
	// 原图
	SourcePicture   *PictureBaseInfo `json:"sourcePicture,omitempty"`
	// 大图
	BigPicture      *PictureBaseInfo `json:"bigPicture,omitempty"`
	// 小图/快照/缩略图
	SnapshotPicture *PictureBaseInfo `json:"snapshotPicture,omitempty"`
}

// 文件消息Content
type FileElem struct {
	FilePath  string `json:"filePath,omitempty"`
	UUID      string `json:"uuid,omitempty"`
	SourceURL string `json:"sourceUrl,omitempty"`
	FileName  string `json:"fileName,omitempty"`
	FileSize  int64  `json:"fileSize"`
	FileType  string `json:"fileType,omitempty"`
}

// @消息Content
type AtTextElem struct {
	Text         string     `json:"text,omitempty"`
	AtUserList   []string   `json:"atUserList,omitempty"`
	AtUsersInfo  []*AtInfo  `json:"atUsersInfo,omitempty"`
	QuoteMessage *MsgStruct `json:"quoteMessage,omitempty"`
	IsAtSelf     bool       `json:"isAtSelf"`
}

type SendMsgReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MsgData *sdkws.MsgData `protobuf:"bytes,3,opt,name=msgData,proto3" json:"msgData"`
}
*/

type Req struct {
	// 请求类型：发送消息 / 获取seq
	ReqIdentifier int32  `json:"reqIdentifier" validate:"required"`
	Token         string `json:"token"`
	SendID        string `json:"sendID"        validate:"required"`
	// 类似trace id？
	OperationID string `json:"operationID"   validate:"required"`
	MsgIncr     string `json:"msgIncr"       validate:"required"`
	// 消息体，不同ReqIdentifier对应的消息体不一样
	Data []byte `json:"data"`
}

func (r *Req) String() string {
	var tReq Req
	tReq.ReqIdentifier = r.ReqIdentifier
	tReq.Token = r.Token
	tReq.SendID = r.SendID
	tReq.OperationID = r.OperationID
	tReq.MsgIncr = r.MsgIncr
	return jsonutil.StructToJsonString(tReq)
}

var reqPool = sync.Pool{
	New: func() any {
		return new(Req)
	},
}

func getReq() *Req {
	req := reqPool.Get().(*Req)
	req.Data = nil
	req.MsgIncr = ""
	req.OperationID = ""
	req.ReqIdentifier = 0
	req.SendID = ""
	req.Token = ""
	return req
}

func freeReq(req *Req) {
	reqPool.Put(req)
}

type Resp struct {
	ReqIdentifier int32  `json:"reqIdentifier"`
	MsgIncr       string `json:"msgIncr"`
	OperationID   string `json:"operationID"`
	ErrCode       int    `json:"errCode"`
	ErrMsg        string `json:"errMsg"`
	Data          []byte `json:"data"`
}

func (r *Resp) String() string {
	var tResp Resp
	tResp.ReqIdentifier = r.ReqIdentifier
	tResp.MsgIncr = r.MsgIncr
	tResp.OperationID = r.OperationID
	tResp.ErrCode = r.ErrCode
	tResp.ErrMsg = r.ErrMsg
	return jsonutil.StructToJsonString(tResp)
}

type MessageHandler interface {
	GetSeq(context context.Context, data *Req) ([]byte, error)
	SendMessage(context context.Context, data *Req) ([]byte, error)
	SendSignalMessage(context context.Context, data *Req) ([]byte, error)
	PullMessageBySeqList(context context.Context, data *Req) ([]byte, error)
	UserLogout(context context.Context, data *Req) ([]byte, error)
	SetUserDeviceBackground(context context.Context, data *Req) ([]byte, bool, error)
}

var _ MessageHandler = (*GrpcHandler)(nil)

type GrpcHandler struct {
	msgRpcClient *rpcclient.MessageRpcClient
	pushClient   *rpcclient.PushRpcClient
	validate     *validator.Validate
}

func NewGrpcHandler(validate *validator.Validate, client discovery.SvcDiscoveryRegistry, rpcRegisterName *config.RpcRegisterName) *GrpcHandler {
	msgRpcClient := rpcclient.NewMessageRpcClient(client, rpcRegisterName.Msg)
	pushRpcClient := rpcclient.NewPushRpcClient(client, rpcRegisterName.Push)
	return &GrpcHandler{
		msgRpcClient: &msgRpcClient,
		pushClient:   &pushRpcClient, validate: validate,
	}
}

func (g GrpcHandler) GetSeq(ctx context.Context, data *Req) ([]byte, error) {
	req := sdkws.GetMaxSeqReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.WrapMsg(err, "GetSeq: error unmarshaling request", "action", "unmarshal", "dataType", "GetMaxSeqReq")
	}
	if err := g.validate.Struct(&req); err != nil {
		return nil, errs.WrapMsg(err, "GetSeq: validation failed", "action", "validate", "dataType", "GetMaxSeqReq")
	}
	// rpc调用msg模块获取该user id对应的所有conversation的max/min seq，用于同步数据
	resp, err := g.msgRpcClient.GetMaxSeq(ctx, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.WrapMsg(err, "GetSeq: error marshaling response", "action", "marshal", "dataType", "GetMaxSeqResp")
	}
	return c, nil
}

// SendMessage handles the sending of messages through gRPC. It unmarshals the request data,
// validates the message, and then sends it using the message RPC client.
func (g GrpcHandler) SendMessage(ctx context.Context, data *Req) ([]byte, error) {
	// 解析ws消息到msg data
	var msgData sdkws.MsgData
	if err := proto.Unmarshal(data.Data, &msgData); err != nil {
		return nil, errs.WrapMsg(err, "SendMessage: error unmarshaling message data", "action", "unmarshal", "dataType", "MsgData")
	}

	// 消息校验
	if err := g.validate.Struct(&msgData); err != nil {
		return nil, errs.WrapMsg(err, "SendMessage: message data validation failed", "action", "validate", "dataType", "MsgData")
	}

	req := msg.SendMsgReq{MsgData: &msgData}
	// msg-gate调用rcp发送消息到msg server
	resp, err := g.msgRpcClient.SendMsg(ctx, &req)
	if err != nil {
		return nil, err
	}

	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.WrapMsg(err, "SendMessage: error marshaling response", "action", "marshal", "dataType", "SendMsgResp")
	}

	return c, nil
}

func (g GrpcHandler) SendSignalMessage(context context.Context, data *Req) ([]byte, error) {
	resp, err := g.msgRpcClient.SendMsg(context, nil)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.WrapMsg(err, "error marshaling response", "action", "marshal", "dataType", "SendMsgResp")
	}
	return c, nil
}

func (g GrpcHandler) PullMessageBySeqList(context context.Context, data *Req) ([]byte, error) {
	req := sdkws.PullMessageBySeqsReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.WrapMsg(err, "error unmarshaling request", "action", "unmarshal", "dataType", "PullMessageBySeqsReq")
	}
	if err := g.validate.Struct(data); err != nil {
		return nil, errs.WrapMsg(err, "validation failed", "action", "validate", "dataType", "PullMessageBySeqsReq")
	}
	resp, err := g.msgRpcClient.PullMessageBySeqList(context, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.WrapMsg(err, "error marshaling response", "action", "marshal", "dataType", "PullMessageBySeqsResp")
	}
	return c, nil
}

func (g GrpcHandler) UserLogout(context context.Context, data *Req) ([]byte, error) {
	req := push.DelUserPushTokenReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.WrapMsg(err, "error unmarshaling request", "action", "unmarshal", "dataType", "DelUserPushTokenReq")
	}
	resp, err := g.pushClient.DelUserPushToken(context, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.WrapMsg(err, "error marshaling response", "action", "marshal", "dataType", "DelUserPushTokenResp")
	}
	return c, nil
}

func (g GrpcHandler) SetUserDeviceBackground(_ context.Context, data *Req) ([]byte, bool, error) {
	req := sdkws.SetAppBackgroundStatusReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, false, errs.WrapMsg(err, "error unmarshaling request", "action", "unmarshal", "dataType", "SetAppBackgroundStatusReq")
	}
	if err := g.validate.Struct(data); err != nil {
		return nil, false, errs.WrapMsg(err, "validation failed", "action", "validate", "dataType", "SetAppBackgroundStatusReq")
	}
	return nil, req.IsBackground, nil
}
