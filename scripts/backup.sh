#!/bin/bash
#
# ROI-GEN Database Backup Script
# Run this before shutting down for the day or on a schedule.
#
# pg_dump covers EVERYTHING — pgvector embeddings are ordinary Postgres table
# data and the `CREATE EXTENSION vector` statement is included in the dump
# (the pgvector/pgvector:pg17 image ships the extension, so restores just work).
#
# Usage: ./scripts/backup.sh
#        ./scripts/backup.sh restore <backup_file>
#        ./scripts/backup.sh list
#

set -e

# Configuration (matches docker-compose.yml service/container names)
BACKUP_DIR="${BACKUP_DIR:-./backups}"
CONTAINER_NAME="roigen-db"
DB_NAME="roigen"
DB_USER="postgres"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/roigen_${TIMESTAMP}.sql.gz"
KEEP_DAYS=30 # Keep backups for 30 days

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

backup() {
    log_info "Starting backup of ROI-GEN database..."

    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Database container '${CONTAINER_NAME}' is not running!"
        log_info "Start it with: docker compose up -d db"
        exit 1
    fi

    # Create backup
    log_info "Creating backup: ${BACKUP_FILE}"
    docker exec -t "$CONTAINER_NAME" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

    # Verify backup was created
    if [[ -f "$BACKUP_FILE" ]] && [[ -s "$BACKUP_FILE" ]]; then
        SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
        log_info "Backup completed successfully! Size: ${SIZE}"
        log_info "Location: ${BACKUP_FILE}"
    else
        log_error "Backup file is empty or not created!"
        exit 1
    fi

    # Clean up old backups
    cleanup_old_backups
}

restore() {
    local restore_file="$1"

    if [[ -z "$restore_file" ]]; then
        log_error "Please specify a backup file to restore"
        log_info "Usage: $0 restore <backup_file>"
        log_info ""
        log_info "Available backups:"
        ls -la "$BACKUP_DIR"/*.sql.gz 2>/dev/null || log_warn "No backups found in ${BACKUP_DIR}"
        exit 1
    fi

    if [[ ! -f "$restore_file" ]]; then
        log_error "Backup file not found: ${restore_file}"
        exit 1
    fi

    log_warn "This will OVERWRITE the current database!"
    read -p "Are you sure? (yes/no): " confirm

    if [[ "$confirm" != "yes" ]]; then
        log_info "Restore cancelled."
        exit 0
    fi

    log_info "Restoring database from: ${restore_file}"

    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Database container '${CONTAINER_NAME}' is not running!"
        exit 1
    fi

    # Stop services holding connections so DROP DATABASE succeeds cleanly
    log_warn "Stop api/engine first if running: docker compose stop api engine"

    # Drop and recreate database (FORCE kills lingering connections, PG13+)
    log_info "Recreating database..."
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -c "DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE);"
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -c "CREATE DATABASE ${DB_NAME};"

    # Restore from backup (includes CREATE EXTENSION vector + all data)
    log_info "Restoring data..."
    gunzip -c "$restore_file" | docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" "$DB_NAME"

    log_info "Restore completed successfully!"
    log_info "Restart the backend services to reconnect: docker compose restart api engine"
}

cleanup_old_backups() {
    log_info "Cleaning up backups older than ${KEEP_DAYS} days..."

    find "$BACKUP_DIR" -name "roigen_*.sql.gz" -type f -mtime +${KEEP_DAYS} -delete

    # Count remaining backups
    count=$(find "$BACKUP_DIR" -name "roigen_*.sql.gz" -type f | wc -l | tr -d ' ')
    log_info "Keeping ${count} backup(s)"
}

list_backups() {
    log_info "Available backups in ${BACKUP_DIR}:"
    echo ""

    if ls "$BACKUP_DIR"/roigen_*.sql.gz 1> /dev/null 2>&1; then
        ls -lh "$BACKUP_DIR"/roigen_*.sql.gz | awk '{print $9, "-", $5}'
    else
        log_warn "No backups found"
    fi
}

# Main
case "${1:-backup}" in
    backup)
        backup
        ;;
    restore)
        restore "$2"
        ;;
    list)
        list_backups
        ;;
    *)
        echo "Usage: $0 {backup|restore <file>|list}"
        echo ""
        echo "Commands:"
        echo "  backup          Create a new backup (default)"
        echo "  restore <file>  Restore from a backup file"
        echo "  list            List available backups"
        exit 1
        ;;
esac
