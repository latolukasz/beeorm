package beeorm

func (c *contextImplementation) Flush() {
	if len(c.trackedEntities) == 0 {
		return
	}
	for _, val := range c.trackedEntities {
		switch val.FlushType() {
		case Insert:
		case Delete:
		case Update:
		}
	}
}

func (c *contextImplementation) FlushLazy() {
	//TODO
}
