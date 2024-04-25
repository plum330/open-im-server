package cache

import (
	"context"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/utils/stringutil"
	"github.com/redis/go-redis/v9"
)

// 序列号 seq redis缓存管理使用的结构是String

type SeqCache interface {
	SetMaxSeq(ctx context.Context, conversationID string, maxSeq int64) error
	GetMaxSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error)
	GetMaxSeq(ctx context.Context, conversationID string) (int64, error)
	SetMinSeq(ctx context.Context, conversationID string, minSeq int64) error
	SetMinSeqs(ctx context.Context, seqs map[string]int64) error
	GetMinSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error)
	GetMinSeq(ctx context.Context, conversationID string) (int64, error)
	GetConversationUserMinSeq(ctx context.Context, conversationID string, userID string) (int64, error)
	GetConversationUserMinSeqs(ctx context.Context, conversationID string, userIDs []string) (map[string]int64, error)
	SetConversationUserMinSeq(ctx context.Context, conversationID string, userID string, minSeq int64) error
	// seqs map: key userID value minSeq
	SetConversationUserMinSeqs(ctx context.Context, conversationID string, seqs map[string]int64) (err error)
	// seqs map: key conversationID value minSeq
	SetUserConversationsMinSeqs(ctx context.Context, userID string, seqs map[string]int64) error
	// has read seq
	SetHasReadSeq(ctx context.Context, userID string, conversationID string, hasReadSeq int64) error
	// k: user, v: seq
	SetHasReadSeqs(ctx context.Context, conversationID string, hasReadSeqs map[string]int64) error
	// k: conversation, v :seq
	UserSetHasReadSeqs(ctx context.Context, userID string, hasReadSeqs map[string]int64) error
	GetHasReadSeqs(ctx context.Context, userID string, conversationIDs []string) (map[string]int64, error)
	GetHasReadSeq(ctx context.Context, userID string, conversationID string) (int64, error)
}

func NewSeqCache(rdb redis.UniversalClient) SeqCache {
	return &seqCache{rdb: rdb}
}

// seq redis缓存管理使用的是普通的redis client， 所以几乎所有的seq读写操作都完全依赖redis
type seqCache struct {
	rdb redis.UniversalClient
}

// 当前会话max seq key
func (c *seqCache) getMaxSeqKey(conversationID string) string {
	return maxSeq + conversationID
}

// 当前会话min seq key
func (c *seqCache) getMinSeqKey(conversationID string) string {
	return minSeq + conversationID
}

// 用户单签会话已读seq key
func (c *seqCache) getHasReadSeqKey(conversationID string, userID string) string {
	return hasReadSeq + userID + ":" + conversationID
}

// 用户当前会话min seq key
func (c *seqCache) getConversationUserMinSeqKey(conversationID, userID string) string {
	return conversationUserMinSeq + conversationID + "u:" + userID
}

// 保存当前会话seq key
func (c *seqCache) setSeq(ctx context.Context, conversationID string, seq int64, getkey func(conversationID string) string) error {
	return errs.Wrap(c.rdb.Set(ctx, getkey(conversationID), seq, 0).Err())
}

// 获取当前会话seq key
func (c *seqCache) getSeq(ctx context.Context, conversationID string, getkey func(conversationID string) string) (int64, error) {
	val, err := c.rdb.Get(ctx, getkey(conversationID)).Int64()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return val, nil
}

func (c *seqCache) getSeqs(ctx context.Context, items []string, getkey func(s string) string) (m map[string]int64, err error) {
	m = make(map[string]int64, len(items))
	for i, v := range items {
		res, err := c.rdb.Get(ctx, getkey(v)).Result()
		if err != nil && err != redis.Nil {
			return nil, errs.Wrap(err)
		}
		val := stringutil.StringToInt64(res)
		if val != 0 {
			m[items[i]] = val
		}
	}

	return m, nil
}

// 保存当前会话max seq (redis string)
func (c *seqCache) SetMaxSeq(ctx context.Context, conversationID string, maxSeq int64) error {
	return c.setSeq(ctx, conversationID, maxSeq, c.getMaxSeqKey)
}

// 获取多个会话对应的max seq
func (c *seqCache) GetMaxSeqs(ctx context.Context, conversationIDs []string) (m map[string]int64, err error) {
	return c.getSeqs(ctx, conversationIDs, c.getMaxSeqKey)
}

// 获取一个会话对应的max seq
func (c *seqCache) GetMaxSeq(ctx context.Context, conversationID string) (int64, error) {
	return c.getSeq(ctx, conversationID, c.getMaxSeqKey)
}

// 保存当前会话min seq
func (c *seqCache) SetMinSeq(ctx context.Context, conversationID string, minSeq int64) error {
	return c.setSeq(ctx, conversationID, minSeq, c.getMinSeqKey)
}

// 设置多个会话对应的seq(redis string)
func (c *seqCache) setSeqs(ctx context.Context, seqs map[string]int64, getkey func(key string) string) error {
	for conversationID, seq := range seqs {
		if err := c.rdb.Set(ctx, getkey(conversationID), seq, 0).Err(); err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (c *seqCache) SetMinSeqs(ctx context.Context, seqs map[string]int64) error {
	return c.setSeqs(ctx, seqs, c.getMinSeqKey)
}

func (c *seqCache) GetMinSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error) {
	return c.getSeqs(ctx, conversationIDs, c.getMinSeqKey)
}

func (c *seqCache) GetMinSeq(ctx context.Context, conversationID string) (int64, error) {
	return c.getSeq(ctx, conversationID, c.getMinSeqKey)
}

// 获取用户当前会话min seq
func (c *seqCache) GetConversationUserMinSeq(ctx context.Context, conversationID string, userID string) (int64, error) {
	val, err := c.rdb.Get(ctx, c.getConversationUserMinSeqKey(conversationID, userID)).Int64()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return val, nil
}

// 获取多个用户当前会话min seq
func (c *seqCache) GetConversationUserMinSeqs(ctx context.Context, conversationID string, userIDs []string) (m map[string]int64, err error) {
	return c.getSeqs(ctx, userIDs, func(userID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

// 保存用户当前会话min seq
func (c *seqCache) SetConversationUserMinSeq(ctx context.Context, conversationID string, userID string, minSeq int64) error {
	return errs.Wrap(c.rdb.Set(ctx, c.getConversationUserMinSeqKey(conversationID, userID), minSeq, 0).Err())
}

// 保存当前会话多个用户min seq
func (c *seqCache) SetConversationUserMinSeqs(ctx context.Context, conversationID string, seqs map[string]int64) (err error) {
	return c.setSeqs(ctx, seqs, func(userID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

// 保存用户多个会话min seq
func (c *seqCache) SetUserConversationsMinSeqs(ctx context.Context, userID string, seqs map[string]int64) (err error) {
	return c.setSeqs(ctx, seqs, func(conversationID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

// 保存用户当前会话已读seq
func (c *seqCache) SetHasReadSeq(ctx context.Context, userID string, conversationID string, hasReadSeq int64) error {
	return errs.Wrap(c.rdb.Set(ctx, c.getHasReadSeqKey(conversationID, userID), hasReadSeq, 0).Err())
}

// 保存当前会话多个用户已读seq
func (c *seqCache) SetHasReadSeqs(ctx context.Context, conversationID string, hasReadSeqs map[string]int64) error {
	return c.setSeqs(ctx, hasReadSeqs, func(userID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

// 保存用户多个会话已读seq
func (c *seqCache) UserSetHasReadSeqs(ctx context.Context, userID string, hasReadSeqs map[string]int64) error {
	return c.setSeqs(ctx, hasReadSeqs, func(conversationID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

// 获取当前用户多个会话已读seq
func (c *seqCache) GetHasReadSeqs(ctx context.Context, userID string, conversationIDs []string) (map[string]int64, error) {
	return c.getSeqs(ctx, conversationIDs, func(conversationID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

// 获取用户当前会话已读seq
func (c *seqCache) GetHasReadSeq(ctx context.Context, userID string, conversationID string) (int64, error) {
	val, err := c.rdb.Get(ctx, c.getHasReadSeqKey(conversationID, userID)).Int64()
	if err != nil {
		return 0, err
	}
	return val, nil
}
