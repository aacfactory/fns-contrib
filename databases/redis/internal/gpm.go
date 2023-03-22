package internal

import (
	"context"
	"fmt"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/logs"
	rds "github.com/redis/go-redis/v9"
	"sync"
	"time"
)

type GlobalPipeline struct {
	id       string
	pipeline rds.Pipeliner
	times    int
	expireAT time.Time
}

type globalPipelineManagementOptions struct {
	log              logs.Logger
	maxAliveDuration time.Duration
}

func newGlobalPipelineManagement(options globalPipelineManagementOptions) *globalPipelineManagement {
	transactionMaxAliveDuration := options.maxAliveDuration
	if transactionMaxAliveDuration < 1*time.Millisecond {
		transactionMaxAliveDuration = 10 * time.Second
	}
	v := &globalPipelineManagement{
		log:              options.log.With("redis", "gpm"),
		maxAliveDuration: transactionMaxAliveDuration,
		txMap:            &sync.Map{},
		closeCh:          make(chan struct{}, 1),
		stopCh:           make(chan struct{}, 1),
	}
	v.checkup()
	return v
}

type globalPipelineManagement struct {
	log              logs.Logger
	maxAliveDuration time.Duration
	txMap            *sync.Map
	closeCh          chan struct{}
	stopCh           chan struct{}
}

func (gtm *globalPipelineManagement) checkup() {
	go func(gtm *globalPipelineManagement) {
		stop := false
		for {
			select {
			case <-gtm.closeCh:
				stop = true
				break
			case <-time.After(gtm.maxAliveDuration * 10):
				now := time.Now()
				timeouts := make(map[string]*GlobalPipeline)
				gtm.txMap.Range(func(_, value interface{}) bool {
					gt := value.(*GlobalPipeline)
					if gt.expireAT.After(now) {
						timeouts[gt.id] = gt
					}
					return true
				})
				for key, transaction := range timeouts {
					_, has := gtm.txMap.LoadAndDelete(key)
					if has {
						transaction.pipeline.Discard()
						if gtm.log.DebugEnabled() {
							gtm.log.Debug().Caller().With("tid", key).Message("gtm: clean db transaction")
						}
					}
				}
			}
			if stop {
				close(gtm.stopCh)
				break
			}
		}
	}(gtm)
}

func (gtm *globalPipelineManagement) Create(ctx context.Context, db Client, tx bool) (err error) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = fmt.Errorf("gpm: can not get request in context")
		return
	}
	id := request.Id()
	var gt *GlobalPipeline
	value, has := gtm.txMap.Load(id)
	if !has {
		var pipe rds.Pipeliner
		if tx {
			pipe = db.TxPipeline()
		} else {
			pipe = db.Pipeline()
		}
		gt = &GlobalPipeline{
			id:       id,
			pipeline: pipe,
			times:    1,
			expireAT: time.Now().Add(10 * time.Second),
		}
		gtm.txMap.Store(id, gt)
	} else {
		gt = value.(*GlobalPipeline)
		gt.times = gt.times + 1
	}
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message(fmt.Sprintf("gpm: create pipeliner %d times", gt.times))
	}
	return
}

func (gtm *globalPipelineManagement) Get(ctx context.Context) (pipe rds.Pipeliner, has bool) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalPipeline)
	pipe = gt.pipeline
	has = true
	return
}

func (gtm *globalPipelineManagement) Exec(ctx context.Context) (finished bool, cmders []rds.Cmder, err error) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		err = fmt.Errorf("gpm: can not get request in context")
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		err = fmt.Errorf("gpm: exec failed for no pipeline in context")
		return
	}
	gt := value.(*GlobalPipeline)
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message(fmt.Sprintf("pipeline has created %d times", gt.times))
	}
	gt.times = gt.times - 1
	if gt.times == 0 {
		if gtm.log.DebugEnabled() {
			gtm.log.Debug().With("requestId", id).Message("begin to commit transaction")
		}
		cmders, err = gt.pipeline.Exec(ctx)
		gtm.txMap.Delete(id)
		if gtm.log.DebugEnabled() {
			_, has := gtm.txMap.Load(id)
			if has {
				gtm.log.Debug().With("requestId", id).Message("pipeline has be removed failed")
			} else {
				gtm.log.Debug().With("requestId", id).Message("pipeline has be removed succeed")
			}
		}
		finished = true
	}
	return
}

func (gtm *globalPipelineManagement) Discard(ctx context.Context) {
	request, hasRequest := service.GetRequest(ctx)
	if !hasRequest {
		return
	}
	id := request.Id()
	value, ok := gtm.txMap.Load(id)
	if !ok {
		return
	}
	gt := value.(*GlobalPipeline)
	if gtm.log.DebugEnabled() {
		gtm.log.Debug().With("requestId", id).Message("begin to discard pipeline")
	}
	gt.pipeline.Discard()
	gtm.txMap.Delete(id)
	if gtm.log.DebugEnabled() {
		_, has := gtm.txMap.Load(id)
		if has {
			gtm.log.Debug().With("requestId", id).Message("pipeline has be removed failed")
		} else {
			gtm.log.Debug().With("requestId", id).Message("pipeline has be removed succeed")
		}
	}
	return
}

func (gtm *globalPipelineManagement) Close() {
	close(gtm.closeCh)
	select {
	case <-gtm.stopCh:
		break
	case <-time.After(1 * time.Second):
		break
	}
	gtm.txMap.Range(func(_, value interface{}) bool {
		gt := value.(*GlobalPipeline)
		gt.pipeline.Discard()
		return true
	})
}
