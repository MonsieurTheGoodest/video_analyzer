package healthcheck

import (
	"context"
	"fmt"
	"log"
	"time"

	"orchestrator/db"
)

const checkInterval = 30 * time.Second

func HealthCheck(database *db.DataBase) error {
	ctx := context.Background()
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rows, err := database.Pool.Query(ctx, `
				SELECT path 
				FROM current_processes 
				WHERE change_time < NOW() - INTERVAL '30 seconds'
			`)
			if err != nil {
				log.Printf("healthCheck ERR: selecting outdated processes ERR: %v", err)
				continue
			}
			defer rows.Close()

			var outdatedPaths []string
			for rows.Next() {
				var path string
				if err := rows.Scan(&path); err != nil {
					return fmt.Errorf("healthCheck ERR: scanning path ERR: %v", err)
				}
				outdatedPaths = append(outdatedPaths, path)
			}
			rows.Close()

			if len(outdatedPaths) > 0 {
				log.Printf("healthCheck: %d outdated process(es), resetting...", len(outdatedPaths))
				for _, path := range outdatedPaths {
					if err := database.Reset(ctx, path); err != nil {
						return fmt.Errorf("healthCheck ERR: reset for path %s ERR: %v", path, err)
					}
				}
			}

		case <-ctx.Done():
			return nil
		}
	}
}
