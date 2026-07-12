module nats

// Subscription represents an active subscription to a NATS subject.
// Use `next_msg()` on the Client to receive messages, or call `unsubscribe()` to stop listening.
pub struct Subscription {
pub:
	subject string
	queue   string
	sid     string
}
