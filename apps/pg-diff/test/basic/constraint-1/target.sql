CREATE TABLE post (
  id                  INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  first_name          TEXT    NOT NULL,
  last_name           TEXT    NOT NULL,
  phone               TEXT,
  birthday            DATE    NOT NULL,
  gender              INTEGER NOT NULL CHECK ( gender IN (1, 2) ),
  address             TEXT    NOT NULL,
  disability_doc_no   TEXT,
  participation_level INTEGER
    CONSTRAINT between_0_and_100 CHECK ( participation_level BETWEEN 0 AND 100 AND participation_level % 5 = 0 ),
  represent_type      TEXT
    CONSTRAINT court_or_attorney CHECK ( represent_type IN ('court', 'attorney') ),
  bank_name           TEXT,
  bank_account        TEXT
    CONSTRAINT validate_iban CHECK ( bank_account IS NULL ),
  is_active           BOOLEAN
); 
