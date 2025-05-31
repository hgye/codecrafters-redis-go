package redis

import "strings"

type Prefix string

const (
	ArrayPrefix        Prefix = "*"
	BulkStringPrefix   Prefix = "$"
	SimpleStringPrefix Prefix = "+"
	ErrorPrefix        Prefix = "-"
	IntegerPrefix      Prefix = ":"
)

func (p Prefix) String() string {
	return string(p)
}

func (p Prefix) IsPrefix(line string) bool {
	return strings.HasPrefix(line, p.String())
}
