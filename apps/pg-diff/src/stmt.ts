export type RawValue = string | Sql | SqlRef | Declaration;
export class Sql {
  readonly dependencies: (number | string)[] = [];
  readonly declarations: number[] = [];
  readonly content: string;
  weight = 0;

  constructor(rawStrings: readonly string[], rawValues: readonly RawValue[]) {
    if (rawStrings.length - 1 !== rawValues.length) {
      if (rawStrings.length === 0) {
        throw new TypeError('Expected at least 1 string');
      }

      throw new TypeError(
        `Expected ${rawStrings.length} strings to have ${
          rawStrings.length - 1
        } values`
      );
    }
    let i = 0;
    let p = 0;
    const _content = new Array(rawStrings.length + rawValues.length);
    _content[p++] = rawStrings[0];
    while (i < rawValues.length) {
      let child = rawValues[i++];
      const rawString = rawStrings[i];
      if (child) {
        _content[p++] = child;
      }
      _content[p++] = rawString;
      if (child instanceof SqlRef) {
        this.dependencies.push(...child.dependencies);
        child = child.value;
      }
      if (child instanceof Declaration) {
        this.declarations.push(child.id);
        child = child.value;
      }
      if (child instanceof Sql) {
        this.dependencies.push(...child.dependencies);
      }
    }
    this.content = _content.join('');
  }

  toString() {
    return this.content;
  }
}

export class SqlRef {
  constructor(
    public readonly value: string | Sql,
    public readonly dependencies: (number | string)[]
  ) {}
  toString() {
    return this.value;
  }
}
class Declaration {
  constructor(public readonly id: number, public readonly value: RawValue) {}
  toString() {
    return this.value;
  }
}
export function stmt(
  strings: readonly string[],
  ...values: readonly RawValue[]
) {
  return new Sql(strings, values);
}

export function join(strings: readonly Sql[], separator: string) {
  if (strings.length === 0) {
    return null;
  }
  return new Sql(
    ['', ...Array(strings.length - 1).fill(separator), ''],
    strings
  );
}

export function dependency(id: number | string, value: string | Sql) {
  return new SqlRef(value, [id]);
}

export function declaration(id: number, value: string | Sql) {
  return new Declaration(id, value);
}
