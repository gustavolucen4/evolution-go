package whatsmeow_service

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// StartClient runs on every connect and every reconnect. Opening a new
// sqlstore.Container each time created a new, unbounded *sql.DB that was never
// closed, so Postgres connections piled up until "too many clients already".
// The container is now opened once per DSN and shared by all clients.
var (
	sqlstoreContainersMu sync.Mutex
	sqlstoreContainers   = map[string]*sqlstore.Container{}
)

func sharedSQLStoreContainer(ctx context.Context, dialect, address string, log waLog.Logger) (*sqlstore.Container, error) {
	key := dialect + "|" + address

	sqlstoreContainersMu.Lock()
	defer sqlstoreContainersMu.Unlock()

	if container, ok := sqlstoreContainers[key]; ok {
		return container, nil
	}

	if dialect != "postgres" {
		container, err := sqlstore.New(ctx, dialect, address, log)
		if err != nil {
			return nil, err
		}
		sqlstoreContainers[key] = container
		return container, nil
	}

	db, err := sql.Open(dialect, address)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)

	container := sqlstore.NewWithDB(db, dialect, log)
	if err := container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("failed to upgrade database: %w", err)
	}

	sqlstoreContainers[key] = container
	return container, nil
}
