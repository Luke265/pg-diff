import { Domain } from '../../catalog/database-objects';
import { declaration, dependency, stmt } from '../../stmt';

export function generateDropDomainScript(type: Domain) {
  return stmt`DROP DOMAIN ${type.fullName};`;
}

export function generateCreateDomainScript(schema: Domain) {
  return stmt`CREATE DOMAIN ${declaration(
    schema.id,
    schema.fullName
  )} AS ${dependency(schema.type.fullName, schema.type.id)} ${schema.check};`;
}

export function generateChangeDomainOwnerScript(type: Domain, owner: string) {
  return stmt`ALTER DOMAIN ${dependency(
    type.fullName,
    type.id
  )} OWNER TO ${owner};`;
}

export function generateChangeDomainCheckScript(type: Domain) {
  return stmt`ALTER DOMAIN DROP CONSTRAINT ${
    type.constraintName
  };\nALTER DOMAIN ${dependency(type.fullName, type.id)} ADD CONSTRAINT ${
    type.constraintName
  } ${type.check};`;
}
