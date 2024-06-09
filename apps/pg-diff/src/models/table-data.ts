export interface TableData {
  sourceData: {
    records: {
      fields: any[];
      rows: any[];
    };
    sequences: any[];
  };
  targetData: {
    records: {
      fields: any[];
      rows: any[];
    };
    sequences: any[];
  };
}
