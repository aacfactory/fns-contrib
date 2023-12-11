package redis

import "github.com/redis/rueidis"

const (
	getCN = "GET"
	setCN = "SET"
)

type Command struct {
	Name   string   `json:"name"`
	Params []string `json:"params"`
}

func (cmd *Command) as(client rueidis.Client) (v rueidis.Completed) {
	switch cmd.Name {
	case getCN:
		v = client.B().Get().Key(cmd.Params[0]).Build()
		break
	case setCN:
		break
	default:
		tokens := make([]string, 0, len(cmd.Params)+1)
		tokens = append(tokens, cmd.Name)
		for _, param := range cmd.Params {
			tokens = append(tokens, param)
		}
		v = client.B().Arbitrary(tokens...).Build()
		break
	}
	return
}

type IncompleteCommand interface {
	Build() (cmd Command)
}
