export class ServerVersion {
  public readonly major: number;
  public readonly minor: number;
  public readonly patch: number;
  constructor(public readonly version: string) {
    const parts = version.split('.');
    this.major = parseInt(parts[0]);
    this.minor = parseInt(parts[1]);
    this.patch = parseInt(parts[2]);
  }
}
