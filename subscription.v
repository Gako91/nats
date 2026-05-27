module nats

pub struct Subscription {
pub:
	subject string
	queue   string
	sid     string
}
