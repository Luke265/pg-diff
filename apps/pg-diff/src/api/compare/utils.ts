import { SqlRef } from '../../stmt';

export function commentIsEqual(
  a: string | null | undefined,
  b: string | null | undefined
) {
  return a === b || (!a && !b);
}

export interface ColumnChanges {
  datatype?: any;
  dataTypeID?: any;
  dataTypeCategory?: any;
  precision?: any;
  scale?: any;
  nullable?: any;
  default?: any;
  defaultRef?: SqlRef;
  identity?: any;
  isNewIdentity?: any;
  truncate?: any;
  references?: any;
  trigger?: any;
  select?: any;
  insert?: any;
  update?: any;
  delete?: any;
  execute?: any;
  usage?: any;
}

const typeNameRegex = /"?((\w+)"?\."?(\w+)+)"?/g;
export function* extractTypeNames(sqlFunctionBody: string) {
  let match: RegExpExecArray;
  while ((match = typeNameRegex.exec(sqlFunctionBody)) !== null) {
    yield match[2] + '.' + match[3];
  }
}

const functionCallRegex = /(\w+(\.\w+)+)\s*\(/g;
export function extractFunctionCalls(sqlFunctionBody: string): string[] {
  sqlFunctionBody = sqlFunctionBody.substring(
    sqlFunctionBody.indexOf('RETURNS')
  );
  // Define a regex to match function calls
  let functionCalls = [];
  let match: RegExpExecArray;

  // Use the regex to find all function calls in the SQL function body
  while ((match = functionCallRegex.exec(sqlFunctionBody)) !== null) {
    // match[1] contains the function name (e.g., "app_hidden.on_order_paid")
    functionCalls.push(
      match[1]
        .split('.')
        .map((v) => `"${v}"`)
        .join('.')
    );
  }

  return functionCalls;
}
