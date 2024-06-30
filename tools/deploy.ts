import { openAsBlob } from 'fs';
import path from 'path';
import fs from 'fs';
import { execSync } from 'child_process';

const dir = `dist/apps/pg-diff`;
execSync(`nx run pg-diff:build`, { stdio: 'inherit' });
execSync(`cd ${dir} & npm pack`, {
  stdio: 'inherit',
});

const pkg = JSON.parse(fs.readFileSync(dir + '/package.json').toString());
const name = `${pkg.name}-${pkg.version}.tgz`;

uploadFile(dir + '/' + name)
  .then((res) => res.text())
  .then(console.info);

async function uploadFile(filePath: string) {
  const file = await openAsBlob(filePath);
  const formData = new FormData();
  formData.set('package', file, path.basename(filePath));
  return fetch(process.env['PUBLISH_URL']!!, {
    method: 'POST',
    body: formData,
  });
}
