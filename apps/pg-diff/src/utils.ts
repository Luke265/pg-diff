import { ClientBase } from 'pg';
import { ServerVersion } from './models/server-version';
import { Config } from './models/config';
import { Sql } from './stmt';
import EventEmitter from 'events';
import path from 'path';
import fs from 'fs';
import { execSync } from 'child_process';

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

export async function saveSqlScript(
  lines: Sql[],
  config: Config,
  scriptName: string
) {
  if (lines.length <= 0) return null;

  const now = new Date();
  const fileName = `${now
    .toISOString()
    .replace(/[-:.TZ]/g, '')}_${scriptName}.sql`;

  if (typeof config.compareOptions.outputDirectory !== 'string')
    config.compareOptions.outputDirectory = '';

  const scriptPath = path.resolve(
    config.compareOptions.outputDirectory || '',
    fileName
  );
  if (config.compareOptions.getAuthorFromGit) {
    config.compareOptions.author = await getGitAuthor();
  }
  const datetime = now.toISOString();
  const titleLength =
    config.compareOptions.author.length > now.toISOString().length
      ? config.compareOptions.author.length
      : datetime.length;

  return new Promise((resolve, reject) => {
    try {
      const file = fs.createWriteStream(scriptPath);

      file.on('error', reject);

      file.on('finish', () => resolve(scriptPath));

      file.write(`/******************${'*'.repeat(titleLength + 2)}***/\n`);
      file.write(
        `/*** SCRIPT AUTHOR: ${config.compareOptions.author.padEnd(
          titleLength
        )} ***/\n`
      );
      file.write(`/***    CREATED ON: ${datetime.padEnd(titleLength)} ***/\n`);
      file.write(`/******************${'*'.repeat(titleLength + 2)}***/\n`);

      lines.forEach((line) => file.write(line.toString()));

      file.end();
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Retrive GIT CONFIG for USER NAME and USER EMAIL, repository first or fallback to global config
 */
async function getGitAuthor() {
  function getLocalAuthorName(): string {
    try {
      return execSync('git config --local user.name').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  function getLocalAuthorEmail() {
    try {
      return execSync('git config --local user.email').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  function getGlobalAuthorName() {
    try {
      return execSync('git config --global user.user').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  function getGlobalAuthorEmail() {
    try {
      return execSync('git config --global user.email').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  function getDefaultAuthorName() {
    try {
      return execSync('git config user.name').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  function getDefaultAuthorEmail() {
    try {
      return execSync('git config user.email').toString().trim();
    } catch (err) {
      return err as string;
    }
  }

  let authorName = getLocalAuthorName();
  let authorEmail = getLocalAuthorEmail();

  if (!authorName) {
    //GIT LOCAL didn't return anything! Try GIT GLOBAL.

    authorName = getGlobalAuthorName();
    authorEmail = getGlobalAuthorEmail();

    if (!authorName) {
      //Also GIT GLOBAL didn't return anything! Try GIT defaults.

      authorName = getDefaultAuthorName();
      authorEmail = getDefaultAuthorEmail();
    }
  }

  if (!authorName) return 'Unknown author configured on this Git Repository';
  else if (authorEmail) return `${authorName} (${authorEmail})`;
  else return authorName;
}
