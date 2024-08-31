export type RawValue = string | Sql | SqlRef | Declaration | null;

export type Id = number | string;
interface Dependency {
  id: Id;
  reverse: boolean;
}
export class Sql {
  constructor(
    public readonly dependencies: Dependency[],
    public readonly declarations: Id[],
    public readonly content: string,
    public readonly weight: number,
  ) {}

  toString() {
    return this.content;
  }
}

export class SqlTag extends Sql {
  constructor(rawStrings: readonly string[], rawValues: readonly RawValue[]) {
    if (rawStrings.length - 1 !== rawValues.length) {
      if (rawStrings.length === 0) {
        throw new TypeError('Expected at least 1 string');
      }

      throw new TypeError(
        `Expected ${rawStrings.length} strings to have ${
          rawStrings.length - 1
        } values`,
      );
    }
    let i = 0;
    let p = 0;
    const _content = new Array(rawStrings.length + rawValues.length);
    _content[p++] = rawStrings[0];
    const dependencies = [];
    const declarations = [];
    while (i < rawValues.length) {
      let child = rawValues[i++];
      const rawString = rawStrings[i];
      if (child) {
        _content[p++] = child;
      }
      _content[p++] = rawString;
      if (child instanceof SqlRef) {
        dependencies.push(...child.dependencies);
        child = child.value;
      }
      if (child instanceof Declaration) {
        declarations.push(child.id);
        child = child.value;
      }
      if (child instanceof Sql) {
        dependencies.push(...child.dependencies);
      }
    }
    const content = _content.join('');
    super(dependencies, declarations, content, 0);
  }

  toString() {
    return this.content;
  }
}

export class SqlRef {
  constructor(
    public readonly value: string | Sql | null,
    public readonly dependencies: { id: number | string; reverse: boolean }[],
  ) {}
  toString() {
    return this.value;
  }
}

class Declaration {
  constructor(
    public readonly id: number,
    public readonly value: RawValue,
  ) {}
  toString() {
    return this.value;
  }
}

export function statement(options: {
  sql: string | Sql | (Sql | string)[];
  dependencies?: Id[];
  declarations?: Id[];
  reverse?: Id[];
  weight?: number;
}) {
  const dependencies: Dependency[] = [];
  const declarations: Id[] = [];
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
          dependencies.push(...s.dependencies);
          declarations.push(...s.declarations);
        }
        return s.toString() + ' ';
      })
      .join('');
  }
  if (options.dependencies) {
    dependencies.push(
      ...options.dependencies.map((id) => ({ id, reverse: false })),
    );
  }
  if (options.reverse) {
    dependencies.push(...options.reverse.map((id) => ({ id, reverse: true })));
  }
  if (options.declarations) {
    declarations.push(...options.declarations);
  }
  return new Sql(dependencies, declarations, out, options.weight ?? 0);
}

export function stmt(
  strings: readonly string[],
  ...values: readonly RawValue[]
) {
  return new SqlTag(strings, values);
}

export function join(strings: readonly Sql[], separator: string) {
  if (strings.length === 0) {
    return null;
  }
  return new SqlTag(
    ['', ...Array(strings.length - 1).fill(separator), ''],
    strings,
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

export function dependency(
  value: string | Sql | null,
  ids: number | string,
): SqlRef;
export function dependency(
  value: string | Sql | null,
  ids: (number | string)[],
): SqlRef;
export function dependency(
  value: string | Sql | null,
  ...ids: (number | string | (number | string)[])[]
): SqlRef;
export function dependency(value: string | Sql | null, ...ids: any) {
  return new SqlRef(
    value,
    ids.flat().map((id: any) => ({ id, reverse: false })),
  );
}

export function reverseDependency(
  value: string | Sql | null,
  ids: number | string,
): SqlRef;
export function reverseDependency(
  value: string | Sql | null,
  ids: (number | string)[],
): SqlRef;
export function reverseDependency(
  value: string | Sql | null,
  ...ids: (number | string | (number | string)[])[]
): SqlRef;
export function reverseDependency(value: string | Sql | null, ...ids: any) {
  return new SqlRef(
    value,
    ids.flat().map((id: any) => ({ id, reverse: true })),
  );
}

export function declaration(id: number, value: string | Sql) {
  return new Declaration(id, value);
}
