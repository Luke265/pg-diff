export type RawValue = string | Sql | SqlRef | Declaration;
export class Sql {
  readonly values: RawValue[];
  readonly strings: string[];
  readonly dependencies: (number | string)[] = [];
  readonly declarations: number[] = [];
  readonly content: string;

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

    const valuesLength = rawValues.reduce<number>(
      (len, value) => len + (value instanceof Sql ? value.values.length : 1),
      0
    );

    this.values = new Array(valuesLength);
    this.strings = new Array(valuesLength + 1);

    this.strings[0] = rawStrings[0];

    // Iterate over raw values, strings, and children. The value is always
    // positioned between two strings, e.g. `index + 1`.
    let i = 0,
      pos = 0;
    let _content = rawStrings[0];
    while (i < rawValues.length) {
      let child = rawValues[i++];
      const rawString = rawStrings[i];
      if (child === undefined || child === null) {
        _content += rawString;
      } else {
        _content += child + rawString;
      }
      if (child instanceof SqlRef) {
        this.dependencies.push(...child.dependencies);
        child = child.value;
      }
      if (child instanceof Declaration) {
        this.declarations.push(child.id);
        child = child.value;
      }
      // Check for nested `sql` queries.
      if (child instanceof Sql) {
        // Append child prefix text to current string.
        this.strings[pos] += child.strings[0];

        let childIndex = 0;
        while (childIndex < child.values.length) {
          this.values[pos++] = child.values[childIndex++];
          this.strings[pos] = child.strings[childIndex];
        }

        // Append raw string to current string.
        this.strings[pos] += rawString;
        this.dependencies.push(...child.dependencies);
      } else {
        this.values[pos++] = child;
        this.strings[pos] = rawString;
      }
    }
    this.content = _content;
  }

  get text() {
    const len = this.strings.length;
    let i = 1;
    let value = this.strings[0];
    while (i < len) value += `$${i}${this.strings[i++]}`;
    return value;
  }

  get sql() {
    const len = this.strings.length;
    let i = 1;
    let value = this.strings[0];
    while (i < len) value += `?${this.strings[i++]}`;
    return value;
  }

  get statement() {
    const len = this.strings.length;
    let i = 1;
    let value = this.strings[0];
    while (i < len) value += `:${i}${this.strings[i++]}`;
    return value;
  }

  toString() {
    return this.content;
  }

  inspect() {
    return {
      text: this.text,
      sql: this.sql,
      values: this.values,
    };
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

class Join {
  constructor(
    public readonly strings: readonly Sql[],
    public readonly separator: string
  ) {}
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
