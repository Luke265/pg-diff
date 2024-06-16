CREATE TABLE post (
    id                 INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id           INTEGER,
    content             TEXT,
    is_custom BOOLEAN                           DEFAULT TRUE,
    is_visible BOOLEAN                           DEFAULT TRUE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at         TIMESTAMP WITH TIME ZONE,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
