export class PatchInfo {
  status: any;
  constructor(
    public readonly filename: string,
    public readonly filepath: string,
    public readonly version: string,
    public readonly name: string,
  ) {}
}
