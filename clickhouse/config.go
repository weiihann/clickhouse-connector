package clickhouse

type Config struct {
	// DSN is the data source name for ClickHouse in the format:
	// clickhouse+http://username:password@host:port/database?protocol=https
	DSN string
}
