# sp_housekeeping_custom.py
sess = shell.get_session()   # use current connection

# Create necessary indexes
sess.run_sql("""
CREATE INDEX idx_history_clock ON history ( clock );
CREATE INDEX idx_history_log_clock ON history_log ( clock );
CREATE INDEX idx_history_str_clock ON history_str ( clock );
CREATE INDEX idx_history_text_clock ON history_text ( clock );
CREATE INDEX idx_history_uint_clock ON history_uint ( clock );
CREATE INDEX idx_trends_clock ON trends ( clock );
CREATE INDEX idx_trends_uint_clock ON trends_uint ( clock );
""")


# Recreate housekeeping logging table 
sess.run_sql("DROP TABLE IF EXISTS zabbix.housekeeping_log")

sess.run_sql("""
CREATE TABLE IF NOT EXISTS zabbix.housekeeping_log (
    id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    schema_name   VARCHAR(128)     NOT NULL,
    table_name    VARCHAR(128)     NOT NULL,
    deleted_rows  BIGINT UNSIGNED NOT NULL,
    deleted_by    VARCHAR(128)    NOT NULL,
    started_at      TIMESTAMP     NOT NULL, 
    finished_at     TIMESTAMP     NOT NULL,
    duration_us     BIGINT UNSIGNED NULL,
    reason          VARCHAR(1024)    NULL,
    sql_statement   VARCHAR(4096)    NULL
);
""")

# Recreate procedure (MySQL 8.4 does NOT support CREATE OR REPLACE PROCEDURE)
sess.run_sql("DROP PROCEDURE IF EXISTS zabbix.sp_housekeeping_history_trends")
sess.run_sql("""
CREATE PROCEDURE zabbix.sp_housekeeping_history_trends (
    IN  p_schema_name   VARCHAR(128),
    IN  p_table_name    VARCHAR(128),
    IN  p_retention     BIGINT UNSIGNED,
    IN  p_reason        VARCHAR(1024)
)
COMMENT 'version 1.2 - 08-april-2026'
BEGIN
    DECLARE v_start        DATETIME(6);
    DECLARE v_end          DATETIME(6);
    DECLARE v_us           BIGINT UNSIGNED;
    DECLARE v_retention_ts BIGINT UNSIGNED;

    DECLARE v_rows_deleted BIGINT DEFAULT 0;
    DECLARE v_total_deleted BIGINT DEFAULT 0;

    -- ------------------------------------------------
    -- Safety checks
    -- ------------------------------------------------
    IF p_schema_name IS NULL OR p_schema_name = '' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_schema_name must not be empty';
    END IF;

    IF p_table_name IS NULL OR p_table_name = '' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_table_name must not be empty';
    END IF;

    IF p_retention IS NULL OR p_retention < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_retention must be >= 1';
    END IF;

    -- Create deterministic time for group replication
    SET v_retention_ts = UNIX_TIMESTAMP(NOW() - INTERVAL p_retention DAY);

    SET v_start = NOW(6);

    -- Batched delete loop
    REPEAT
        SET @sql = CONCAT(
            'DELETE FROM `', REPLACE(p_schema_name,'`','``'),
            '`.`', REPLACE(p_table_name,'`','``'),
            '` WHERE clock < ',
            v_retention_ts,
            ' ORDER BY itemid, clock ',
            ' LIMIT 300000'
        );

        PREPARE stmt FROM @sql;
        EXECUTE stmt;

        SET v_rows_deleted = ROW_COUNT();
        SET v_total_deleted = v_total_deleted + v_rows_deleted;

        DEALLOCATE PREPARE stmt;

        COMMIT;

    UNTIL v_rows_deleted = 0
    END REPEAT;

    SET v_end = NOW(6);
    SET v_us = TIMESTAMPDIFF(MICROSECOND, v_start, v_end);

    -- Log housekeeping run
    INSERT INTO zabbix.housekeeping_log (
        schema_name,
        table_name,
        deleted_rows,
        duration_us,
        deleted_by,
        started_at,
        finished_at,
        reason,
        sql_statement
    )
    VALUES (
        p_schema_name,
        p_table_name,
        v_total_deleted,
        v_us,
        CURRENT_USER(),
        v_start,
        v_end,
        p_reason,
        CONCAT( @sql, ' (looped)' )
    );
END;
""")

# Recreate procedure (MySQL 8.4 does NOT support CREATE OR REPLACE PROCEDURE)
sess.run_sql("DROP PROCEDURE IF EXISTS zabbix.sp_housekeeping_audit")
sess.run_sql("""
CREATE PROCEDURE zabbix.sp_housekeeping_audit (
    IN  p_schema_name   VARCHAR(128),
    IN  p_table_name    VARCHAR(128),
    IN  p_retention     BIGINT UNSIGNED,
    IN  p_reason        VARCHAR(1024)
)
COMMENT "version 1.2 - 08-april-2026"
BEGIN
    DECLARE v_start   DATETIME(6);
    DECLARE v_end     DATETIME(6);
    DECLARE v_deleted BIGINT DEFAULT 0;
    DECLARE v_us      BIGINT UNSIGNED;
    DECLARE v_retention_ts    BIGINT UNSIGNED;

    -- Safety checks (optional but recommended)
    IF p_schema_name IS NULL OR p_schema_name = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_schema_name must not be empty';
    END IF;

    IF p_table_name IS NULL OR p_table_name = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_table_name must not be empty';
    END IF;

    IF p_retention IS NULL OR p_retention < 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_rentention must not >= 1';
    END IF;

    -- Create deterministic time for group replication
    SET v_retention_ts = UNIX_TIMESTAMP(NOW() - INTERVAL p_retention DAY);    

    -- Build dynamic DELETE safely: escape backticks in identifiers 
    SET @sql = CONCAT(
        'DELETE FROM `', REPLACE(p_schema_name,'`','``'),
        '`.`', REPLACE(p_table_name,'`','``'),
        '` WHERE clock < ',
        v_retention_ts,
        ' ORDER BY auditid, clock ',
        ' LIMIT 300000'
    );

    -- Start timer
    SET v_start = NOW(6);

    -- Execute dynamic DELETE
    PREPARE stmt FROM @sql;
    EXECUTE stmt;

    -- Rows affected by the DELETE we just executed
    SET v_deleted = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    -- End timer
    SET v_end = NOW(6);

    -- Compute duration in microseconds
    SET v_us = TIMESTAMPDIFF(MICROSECOND, v_start, v_end);

    -- Log the housekeeping run
    INSERT INTO zabbix.housekeeping_log (
        schema_name,
        table_name,
        deleted_rows,
        duration_us,
        deleted_by,
        started_at,
        finished_at,
        reason,
        sql_statement
    )
    VALUES (
        p_schema_name,
        p_table_name,
        v_deleted,
        v_us,
        CURRENT_USER(),
        v_start,
        v_end,
        p_reason,
        @sql
    );
             
    COMMIT;
END
""")

# Recreate procedure (MySQL 8.4 does NOT support CREATE OR REPLACE PROCEDURE)
sess.run_sql("DROP EVENT IF EXISTS zabbix.ev_housekeeping_custom")

sess.run_sql("""
CREATE EVENT zabbix.ev_housekeeping_custom
    ON SCHEDULE              
        EVERY 1 HOUR
        STARTS TIMESTAMP(
            CURRENT_DATE + INTERVAL (HOUR(NOW()) + 1) HOUR,
            '00:00:00'
        )
    COMMENT "version 1.1 - 2-april-2026" 
    DO
        BEGIN
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'history', 30, 'Automatic hourly housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'history_uint', 30, 'Automatic hourly housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'history_str', 30, 'Automatic hourly housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'history_text', 30, 'Automatic hourly housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'history_log', 30, 'Automatic hourly housekeeping via scheduler' );
        
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'trends', 365, 'Automatic hourly housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_history_trends('zabbix', 'trends_uint', 365, 'Automatic hourly housekeeping via scheduler' );
            
            CALL zabbix.sp_housekeeping_audit('zabbix', 'auditlog', 90, 'Automatic hourly housekeeping via scheduler' );
        END
""")

sess.run_sql("""
ALTER EVENT zabbix.ev_housekeeping_custom DISABLE;
""")

print("Procedure sp_housekeeping_custom created successfully on MySQL 8.4.")
