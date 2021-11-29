package main

const (
	schema = `
CREATE TABLE IF NOT EXISTS model_config (
	id INTEGER PRIMARY KEY AUTOINCREMENT, 
	key TEXT, 
	value TEXT, 
	UNIQUE(key)
);
	
CREATE TABLE IF NOT EXISTS change_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	type INTEGER, 
	entity_type TEXT, 
	entity_id INTEGER, 
	created_at DATETIME
);

CREATE TRIGGER IF NOT EXISTS insert_model_config_trigger
AFTER INSERT ON model_config FOR EACH ROW
BEGIN
	INSERT INTO change_log (type, entity_type, entity_id, created_at) VALUES (1, "model_config", NEW.id, DATETIME('now'));
END;

CREATE TRIGGER IF NOT EXISTS update_model_config_trigger
AFTER UPDATE ON model_config FOR EACH ROW
BEGIN
	INSERT INTO change_log (type, entity_type, entity_id, created_at) VALUES (2, "model_config", OLD.id, DATETIME('now'));
END;

CREATE TRIGGER IF NOT EXISTS delete_model_config_trigger
AFTER DELETE ON model_config FOR EACH ROW
BEGIN
	INSERT INTO change_log (type, entity_type, entity_id, created_at) VALUES (4, "model_config", OLD.id, DATETIME('now'));
END;
`
)
