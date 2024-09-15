package mr


// 工作阶段的定义,似乎放到一个独立的文件更加合适
type jobPhase string

const (
	MAP = "Map"
	REDUCE = "Reduce"
	EXIT = "Exit"
)
