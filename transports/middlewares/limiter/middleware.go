package limiter

import (
	"github.com/aacfactory/configures"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/caches/lru"
	"github.com/aacfactory/fns/transports"
	"github.com/aacfactory/json"
	"github.com/aacfactory/logs"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
	"net/http"
	"runtime"
	"time"
)

var (
	ErrDeviceId   = errors.New(http.StatusNotAcceptable, "***NOT ACCEPTABLE**", "fns: X-Fns-Device-Id is required")
	ErrNotAllowed = errors.New(http.StatusTooManyRequests, "***TOO MANY REQUESTS***", "fns: too many requests")
)

type Options struct {
	alarm Alarm
}

type Option func(options *Options)

func WithAlarm(alarm Alarm) Option {
	return func(options *Options) {
		options.alarm = alarm
	}
}

func New(options ...Option) transports.Middleware {
	opt := Options{}
	for _, option := range options {
		option(&opt)
	}
	return &middleware{
		alarm: opt.alarm,
	}
}

type middleware struct {
	log           logs.Logger
	enable        bool
	hasAlarm      bool
	alarm         Alarm
	alarms        *rate.Limiter
	requests      *rate.Limiter
	deviceEnabled bool
	devices       *lru.ARCCache[string, *rate.Limiter]
	deviceEvery   time.Duration
	deviceBurst   int
	group         *singleflight.Group
}

func (middle *middleware) Name() string {
	return "limiter"
}

func (middle *middleware) Construct(options transports.MiddlewareOptions) (err error) {
	middle.log = options.Log
	config := Config{}
	configErr := options.Config.As(&config)
	if configErr != nil {
		err = errors.Warning("fns: construct limiter middleware failed").WithCause(configErr)
		return
	}
	if config.Enable {
		middle.enable = true
		// request
		everySeconds := config.EverySeconds
		if everySeconds < 1 {
			everySeconds = 10
		}
		burst := config.Burst
		if burst < 1 {
			burst = runtime.NumCPU() * 1024 * 4
		}
		middle.requests = rate.NewLimiter(rate.Every(time.Duration(everySeconds)*time.Second), burst)
		// device
		if config.Device.Enable {
			deviceEverySeconds := config.Device.EverySeconds
			if deviceEverySeconds < 1 {
				deviceEverySeconds = 10
			}
			middle.deviceEvery = time.Duration(deviceEverySeconds) * time.Second
			deviceBurst := config.Device.Burst
			if deviceBurst < 1 {
				deviceBurst = 10
			}
			middle.deviceBurst = deviceBurst
			maxDevice := config.Device.CacheSize
			if maxDevice < 1 {
				maxDevice = 4096
			}
			middle.devices, err = lru.NewARC[string, *rate.Limiter](maxDevice)
			if err != nil {
				err = errors.Warning("fns: construct limiter middleware failed").WithCause(err)
				return
			}
			middle.deviceEnabled = true
		}
		// alarm
		if middle.alarm != nil {
			if len(config.Alarm.Options) == 0 {
				config.Alarm.Options = json.EmptyObjectBytes
			}
			alarmConfig, alarmConfigErr := configures.NewJsonConfig(config.Alarm.Options)
			if alarmConfigErr != nil {
				err = errors.Warning("fns: construct limiter middleware failed").WithCause(alarmConfigErr)
				return
			}
			err = middle.alarm.Construct(AlarmOptions{
				Log:    middle.log.With("alarm", "limiter"),
				Config: alarmConfig,
			})
			if err != nil {
				err = errors.Warning("fns: construct limiter middleware failed").WithCause(err)
				return
			}
			alarmEverySeconds := config.EverySeconds
			if alarmEverySeconds < 1 {
				alarmEverySeconds = 10
			}
			alarmBurst := config.Burst
			if alarmBurst < 1 {
				alarmBurst = 10
			}
			middle.alarms = rate.NewLimiter(rate.Every(time.Duration(alarmEverySeconds)*time.Second), alarmBurst)
			middle.hasAlarm = true
		}
		middle.group = new(singleflight.Group)
	}
	return
}

func (middle *middleware) Handler(next transports.Handler) transports.Handler {
	if middle.enable {
		return transports.HandlerFunc(func(w transports.ResponseWriter, r transports.Request) {
			deviceId := r.Header().Get(transports.DeviceIdHeaderName)
			if len(deviceId) == 0 {
				w.Failed(ErrDeviceId)
				return
			}
			requestAllowed := middle.requests.Allow()
			if !requestAllowed {
				if middle.hasAlarm {
					alarmAllowed := middle.alarms.Allow()
					if !alarmAllowed && middle.alarm != nil {
						alarmErr := middle.alarm.Handle(r)
						if alarmErr != nil && middle.log.WarnEnabled() {
							middle.log.Warn().With("middleware", middle.Name()).With("component", "alarm").Cause(alarmErr).Message("limiter: alarm failed")
						}
					}
				}
				w.Failed(ErrNotAllowed)
				return
			}
			if middle.deviceEnabled {
				deviceLimiter := middle.getDeviceLimiter(deviceId)
				deviceAllowed := deviceLimiter.Allow()
				if !deviceAllowed {
					w.Failed(ErrNotAllowed)
					return
				}
			}
			next.Handle(w, r)
		})
	}
	return next
}

func (middle *middleware) Close() (err error) {
	if middle.alarm != nil {
		middle.alarm.Shutdown()
	}
	return
}

func (middle *middleware) getDeviceLimiter(deviceId []byte) (limiter *rate.Limiter) {
	id := bytex.ToString(deviceId)
	v, _, _ := middle.group.Do(id, func() (v interface{}, err error) {
		has := false
		limiter, has = middle.devices.Get(id)
		if !has {
			limiter = rate.NewLimiter(rate.Every(middle.deviceEvery), middle.deviceBurst)
			middle.devices.Add(id, limiter)
		}
		v = limiter
		return
	})
	limiter = v.(*rate.Limiter)
	return
}
