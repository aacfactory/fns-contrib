package dal

type Option func(*Options)

type Options struct {
	dialect Dialect
}

func defaultOptions() *Options {
	return &Options{
		dialect: "",
	}
}

func WithDialect(dialect Dialect) Option {
	return func(options *Options) {
		options.dialect = dialect
	}
}
