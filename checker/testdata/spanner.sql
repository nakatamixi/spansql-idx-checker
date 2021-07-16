CREATE TABLE test (
  pk_first    STRING(36) NOT NULL,
  pk_second   STRING(36) NOT NULL,
  idx_first   STRING(36) NOT NULL,
  idx_second  STRING(36) NOT NULL,
  not_idx     STRING(36) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
) PRIMARY KEY(pk_first, pk_second);
CREATE  INDEX idx ON test(idx_first, idx_second);
