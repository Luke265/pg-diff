CREATE TABLE post (
    id           INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id    INTEGER,
    content      TEXT,
    is_custom    BOOLEAN                                         DEFAULT TRUE,
    is_visible   BOOLEAN                                         DEFAULT TRUE,
    required_col   BOOLEAN                                        ,
    computed_col NUMERIC(16, 6)
                 GENERATED ALWAYS AS (author_id) STORED NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE               NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMP WITH TIME ZONE,
    updated_at   TIMESTAMP WITH TIME ZONE               NOT NULL DEFAULT NOW()
);

CREATE
    OR REPLACE FUNCTION tg_test(
)
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE
    SECURITY DEFINER
AS
$BODY$
BEGIN
    -- DO NOTHING
    RETURN NEW;
END;
$BODY$;

CREATE TRIGGER _110_reset_action_triggers
    AFTER UPDATE OF is_custom
    ON post
    FOR EACH ROW
    WHEN ( NEW.is_custom <> OLD.is_custom )
EXECUTE PROCEDURE tg_test();