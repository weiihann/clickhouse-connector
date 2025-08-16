package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse/v2" // Import mailru driver for chhttp
)

type Client struct {
	conn   *sql.DB
	ctx    context.Context
	config *Config
}

func New(config *Config) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) Start(ctx context.Context) error {
	c.ctx = ctx

	dsn := c.config.DSN
	originalDSN := dsn

	if strings.HasPrefix(dsn, "clickhouse+https://") {
		dsn = strings.TrimPrefix(dsn, "clickhouse+") // Becomes https://...
	} else if strings.HasPrefix(dsn, "clickhouse+http://") {
		// Check if port or params indicate HTTPS despite the http prefix
		if strings.Contains(originalDSN, ":443") || strings.Contains(originalDSN, "protocol=https") {
			dsn = "https" + strings.TrimPrefix(dsn, "clickhouse+http") // Becomes https://...
		} else {
			dsn = strings.TrimPrefix(dsn, "clickhouse+") // Becomes http://...
		}
	}

	dsnParams := url.Values{}
	dsnParams.Add("read_timeout", "200s") // Add 's' unit
	dsnParams.Add("write_timeout", "30s") // Add 's' unit

	paramStr := dsnParams.Encode()
	if paramStr != "" {
		if strings.Contains(dsn, "?") {
			dsn += "&" + paramStr
		} else {
			dsn += "?" + paramStr
		}
	}

	conn, err := sql.Open("chhttp", dsn) // Use "chhttp" driver name
	if err != nil {
		return err
	}

	// Configure connection pool settings
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	// Test connection with a ping
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // Add timeout to ping
	defer cancel()

	if err := conn.PingContext(pingCtx); err != nil {
		conn.Close() // Close pool if ping fails

		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	c.conn = conn

	return nil
}

func (c *Client) Stop() error {
	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := c.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}

func (c *Client) GetMaxBlock(ctx context.Context) (uint64, error) {
	query := `
		SELECT max(last_access_block) FROM default.accounts_last_access
	`

	row := c.QueryRow(ctx, query)
	var maxBlock uint64
	if err := row.Scan(&maxBlock); err != nil {
		return 0, err
	}

	return maxBlock, nil
}

func (c *Client) ExecOnExpiredAccounts(ctx context.Context, startBlock, endBlock uint64, execFn func(address string)) error {
	query := `
		SELECT address FROM default.accounts_last_access FINAL
		WHERE last_access_block >= ? AND last_access_block < ?
	`

	rows, err := c.Query(ctx, query, startBlock, endBlock)
	if err != nil {
		return err
	}

	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return err
		}

		execFn(address)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (c *Client) ExecOnExpiredSlots(ctx context.Context, startBlock, endBlock uint64, execFn func(address, slot string)) error {
	query := `
		SELECT address, slot_key FROM default.storage_last_access FINAL
		WHERE last_access_block >= ? AND last_access_block < ? AND is_deleted = false
	`

	rows, err := c.Query(ctx, query, startBlock, endBlock)
	if err != nil {
		return err
	}

	for rows.Next() {
		var address, slot string
		if err := rows.Scan(&address, &slot); err != nil {
			return err
		}

		execFn(address, slot)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}
