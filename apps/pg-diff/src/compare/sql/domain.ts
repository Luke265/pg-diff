import { Domain } from '../../catalog/database-objects.js';
import { statement } from '../stmt.js';

export function generateDropDomainScript(type: Domain) {
  return statement({
    sql: `DROP DOMAIN ${type.fullName};`,
    before: [type.id],
  });
}

export function generateCreateDomainScript(schema: Domain) {
  return statement({
    sql: `CREATE DOMAIN ${schema.fullName} AS ${schema.type.fullName} ${schema.check};`,
    declarations: [schema.id],
    dependencies: [schema.type.id],
  });
}

export function generateChangeDomainOwnerScript(type: Domain, owner: string) {
  return statement({
    sql: `ALTER DOMAIN ${type.fullName} OWNER TO ${owner};`,
    dependencies: [type.id],
  });
}

export function generateChangeDomainCheckScript(type: Domain) {
  return statement({
    sql: `ALTER DOMAIN DROP CONSTRAINT ${
      type.constraintName
    };\nALTER DOMAIN ${type.fullName} ADD CONSTRAINT ${
      type.constraintName
    } ${type.check};`,
    dependencies: [type.id],
  });
}
