package main

const (
	schema = `
CREATE TABLE IF NOT EXISTS model_config (
	id INTEGER PRIMARY KEY AUTOINCREMENT, 
	key TEXT, 
	value TEXT, 
	UNIQUE(key)
);
	
CREATE TABLE IF NOT EXISTS wal (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	type TEXT, 
	context_name TEXT, 
	context_id INTEGER, 
	created_at DATETIME
);

CREATE TRIGGER IF NOT EXISTS insert_model_config_trigger
AFTER INSERT ON model_config FOR EACH ROW
BEGIN
	INSERT INTO wal (type, context_name, context_id, created_at) VALUES ("c", "model_config", NEW.id, CURRENT_TIMESTAMP);
END;

CREATE TRIGGER IF NOT EXISTS update_model_config_trigger
AFTER UPDATE ON model_config FOR EACH ROW
BEGIN
	INSERT INTO wal (type, context_name, context_id, created_at) VALUES ("u", "model_config", OLD.id, CURRENT_TIMESTAMP);
END;

CREATE TRIGGER IF NOT EXISTS delete_model_config_trigger
AFTER DELETE ON model_config FOR EACH ROW
BEGIN
	INSERT INTO wal (type, context_name, context_id, created_at) VALUES ("d", "model_config", OLD.id, CURRENT_TIMESTAMP);
END;
`

	// model_config queries
	query         = "SELECT id, key, value FROM model_config WHERE key = ?"
	queryMultiple = "SELECT id, key, value FROM model_config WHERE id IN (%?)"
	queryAll      = "SELECT id, key, value FROM model_config"
	update        = "INSERT OR REPLACE INTO model_config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value"
	remove        = "DELETE FROM model_config WHERE key = ?"

	// wal_watcher queries
	watch = "SELECT id, type, context_name, context_id, created_at FROM wal WHERE id>? AND created_at>=? ORDER BY id ASC"
)
