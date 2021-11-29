package eventqueue

type SubscriptionOption struct {
	entityType string
	changeMask ChangeType
	filterFn   func(Change) bool
}

func Topic(entityType string, changeMask ChangeType) SubscriptionOption {
	return SubscriptionOption{
		entityType: entityType,
		changeMask: changeMask,
	}
}

func FilteredTopic(entityType string, changeMask ChangeType, filterFn func(Change) bool) SubscriptionOption {
	opt := Topic(entityType, changeMask)
	opt.filterFn = filterFn
	return opt
}
