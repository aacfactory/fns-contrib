package limiter

type Config struct {
	Enable       bool `json:"enable"`
	EverySeconds int  `json:"everySeconds"`
	Burst        int  `json:"burst"`
	MaxDevice    int  `json:"maxDevice"`
}
