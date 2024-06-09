import { ClientBase } from 'pg';
import { ServerVersion } from './models/server-version';

export async function getServerVersion(
  client: ClientBase
): Promise<ServerVersion> {
  const c = client as any;
  if (c._version) {
    return c._version;
  }
  const queryResult = await client.query<{ current_setting: string | null }>(
    "SELECT current_setting('server_version')"
  );
  const version = queryResult.rows.at(0)?.current_setting;
  if (!version) {
    throw new Error('Failed to retrieve server version');
  }
  c._version = new ServerVersion(version);
  return c._version;
}
