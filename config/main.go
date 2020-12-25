package config

var RMQ c

type c struct {
	Username string
	Password string
	Host     string
	Port     string
}

func init() {
	RMQ.Username = "guest"
	RMQ.Password = "guest"
	RMQ.Host = "localhost"
	RMQ.Port = "5672"
}
