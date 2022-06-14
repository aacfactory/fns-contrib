package sql

type transactionRegistration struct {
	Id string `json:"id"`
}

type transactionStatus struct {
	Finished bool `json:"finished"`
}

type queryArgument struct {
	Query string `json:"query"`
	Args  *Tuple `json:"args"`
}

type executeArgument struct {
	Query string `json:"query"`
	Args  *Tuple `json:"args"`
}

type ExecuteResult struct {
	Affected     int64 `json:"affected"`
	LastInsertId int64 `json:"lastInsertId"`
}
