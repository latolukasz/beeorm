package beeorm

func (c *contextImplementation) Flush() {
	for _, val := range c.trackedEntities {
	}
}

func (c *contextImplementation) FlushLazy() {
	//TODO
}
