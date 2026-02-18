# sp_housekeeping_custom.py
sess = shell.get_session()   # use current connection


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
sess.run_sql("DROP PROCEDURE IF EXISTS zabbix.sp_housekeeping_custom")

sess.run_sql("""
CREATE PROCEDURE zabbix.sp_housekeeping_custom (
    IN  p_schema_name   VARCHAR(128),
    IN  p_table_name    VARCHAR(128),
    IN  p_retention     BIGINT UNSIGNED,
    IN  p_reason        VARCHAR(1024)
)
BEGIN
    DECLARE v_start   DATETIME(6);
    DECLARE v_end     DATETIME(6);
    DECLARE v_deleted BIGINT DEFAULT 0;
    DECLARE v_us      BIGINT UNSIGNED;

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

    -- Build dynamic DELETE safely: escape backticks in identifiers
    SET @sql = CONCAT(
        'DELETE FROM `', REPLACE(p_schema_name,'`','``'),
        '`.`', REPLACE(p_table_name,'`','``'),
        '` WHERE clock < UNIX_TIMESTAMP(NOW() - INTERVAL ',
        p_retention,
        ' DAY)'
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
END
""")

# Recreate procedure (MySQL 8.4 does NOT support CREATE OR REPLACE PROCEDURE)
sess.run_sql("DROP EVENT IF EXISTS zabbix.ev_housekeeping_custom")

sess.run_sql("""
CREATE EVENT zabbix.ev_housekeeping_custom
    ON SCHEDULE EVERY 1 DAY
    STARTS TIMESTAMP(CURRENT_DATE, '22:00:00')
    DO
        BEGIN
            CALL zabbix.sp_housekeeping_custom('zabbix', 'history_log', 26, 'daily housekeeping via scheduler' );
            CALL zabbix.sp_housekeeping_custom('zabbix', 'trends', 160, 'daily housekeeping via scheduler' );
        END
""")

print("Procedure sp_housekeeping_custom created successfully on MySQL 8.4.")
