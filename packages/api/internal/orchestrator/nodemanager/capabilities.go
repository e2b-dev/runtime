package nodemanager

// Capabilities are what the orchestrator build can do, reported beside its
// labels and never mixed into them.
func (n *Node) setCapabilities(capabilities []string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.capabilities = capabilities
}

func (n *Node) Capabilities() []string {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	return n.capabilities
}
