export class TableDefinition {
  constructor(
    public readonly tableName: string,
    public readonly tableKeyFields: string[],
    public readonly tableSchema: string,
  ) {}
}
