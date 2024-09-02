export type RawValue = string | Sql | null;
export type Id = number | string;

export class Sql {
  constructor(
    public readonly dependencies: Id[],
    public readonly before: Id[],
    public readonly declarations: Id[],
    public readonly content: string,
    public readonly weight: number,
  ) {}

  toString() {
    return this.content;
  }
}

export function statement(options: {
  sql: string | Sql | (Sql | string)[];
  dependencies?: Id[];
  declarations?: Id[];
  before?: Id[];
  weight?: number;
}) {
  const dependencies: Id[][] = [];
  const declarations: Id[][] = [];
  const before: Id[][] = [];
  let out = '';
  if (typeof options.sql === 'string') {
    out = options.sql;
  } else if (options.sql instanceof Sql) {
    out = options.sql.toString();
  } else {
    out = options.sql
      .map((s) => {
        if (s === '' || !s) {
          return '';
        }
        if (s instanceof Sql) {
          dependencies.push(s.dependencies);
          before.push(s.before);
          declarations.push(s.declarations);
        }
        return s.toString();
      })
      .join('');
  }
  if (options.dependencies) {
    dependencies.push(options.dependencies);
  }
  if (options.before) {
    before.push(options.before);
  }
  if (options.declarations) {
    declarations.push(options.declarations);
  }
  return new Sql(
    dependencies.flat(),
    before.flat(),
    declarations.flat(),
    out,
    options.weight ?? 0,
  );
}

export function joinStmt(
  sql: (string | Sql)[],
  strings: readonly (Sql | string)[],
  separator: string,
) {
  if (strings.length === 0) {
    return null;
  }
  let i = 0;
  let last = strings.length - 1;
  for (const s of strings) {
    sql.push(s);
    if (i !== last) {
      sql.push(separator);
    }
    i++;
  }
  return sql;
}
