module nats

pub struct ServerInfo {
pub mut:
	server_id     string @[json: server_id]
	server_name   string @[json: server_name]
	version       string
	proto         int
	go_version    string @[json: go]
	host          string
	port          int
	headers       bool
	max_payload   int    @[json: max_payload]
	client_id     u64    @[json: client_id]
	client_ip     string @[json: client_ip]
	auth_required bool   @[json: auth_required]
	tls_required  bool   @[json: tls_required]
}
