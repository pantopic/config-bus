package icarus

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/logbn/icarus/internal"
)

// Controller operates background processes like epoch advancement and election notices
type Controller interface {
	Start(agent *zongzi.Agent, shard zongzi.Shard, client zongzi.ShardClient) (err error)
}

type controller struct {
	agent     *zongzi.Agent
	client    zongzi.ShardClient
	clock     clock.Clock
	ctx       context.Context
	ctxCancel context.CancelFunc
	index     uint64
	isLeader  bool
	log       *slog.Logger
	mutex     sync.RWMutex
	shard     zongzi.Shard
	term      uint64
	termSet   bool
	wg        sync.WaitGroup
}

func NewController(ctx context.Context, log *slog.Logger) *controller {
	return &controller{
		clock: clock.New(),
		ctx:   ctx,
		log:   log,
	}
}

func (c *controller) Start(agent *zongzi.Agent, shard zongzi.Shard, client zongzi.ShardClient) (err error) {
	c.agent = agent
	c.shard = shard
	c.client = client
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.ctx, c.ctxCancel = context.WithCancel(context.Background())
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := c.clock.Ticker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.ctx.Done():
				c.log.Info("Controller manager stopped")
				return
			case <-t.C:
				c.tick()
			}
		}
	}()
	return
}

func (c *controller) tick() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var hadErr bool
	var index uint64
	if c.isLeader {
		if !c.termSet && c.term > 0 {
			cmd, _ := proto.Marshal(&internal.TermRequest{Term: c.term})
			if _, _, err := c.client.Apply(c.ctx, append(cmd, CMD_INTERNAL_TERM)); err != nil {
				c.log.Warn("Error setting term", "err", err.Error())
			}
			c.termSet = true
		}
		req := &internal.TickRequest{Term: c.term}
		cmd, err := proto.Marshal(req)
		if err != nil {
			c.log.Error("Error marshaling proto", "err", err.Error())
		}
		index, _, err = c.client.Apply(c.ctx, append(cmd, CMD_INTERNAL_TICK))
		if err != nil {
			c.log.Error("Error applying tick", "err", err.Error())
			hadErr = true
		}
	}
	if !hadErr && index > c.index {
		c.log.Debug("Controller finished processing", "shard", c.shard.Name, "index", index)
		c.index = index
	}
	return
}

func (c *controller) LeaderUpdated(info zongzi.LeaderInfo) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.shard.ID == 0 || info.ShardID != c.shard.ID {
		return
	}
	c.isLeader = info.LeaderID == info.ReplicaID
	if c.isLeader && c.term != info.Term {
		req := &internal.TermRequest{
			Term: info.Term,
		}
		cmd, err := proto.Marshal(req)
		if err != nil {
			c.log.Error("Error marshaling proto", "err", err.Error())
		}
		_, _, err = c.client.Apply(c.ctx, append(cmd, CMD_INTERNAL_TERM))
		if err != nil {
			c.log.Error("Error applying term", "err", err.Error())
		}
		c.termSet = true
	}
	c.term = info.Term
}

func (c *controller) Stop() {
	defer c.log.Info("Stopped icarus controller", "name", c.shard.Name)
	if c.ctxCancel != nil {
		c.ctxCancel()
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.index = 0
}
