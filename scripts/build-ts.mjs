/**
 * Zero-Dependency TypeScript-to-ESM Build Pipeline (`scripts/build-ts.mjs`)
 *
 * Ensures ChronosProxy can be built and tested in any Node >=18 environment even
 * before `npm install` or in air-gapped CI sandboxes, while preserving line numbers
 * for accurate stack traces.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DIST_DIR = path.join(ROOT_DIR, 'dist');

function transpileTsToJs(tsSource) {
  // 1. Remove `export type ...` and `import type ...` blocks (preserving newlines)
  let out = tsSource.replace(
    /^[ \t]*(?:export\s+)?type\s+[A-Za-z0-9_]+\s*=\s*[\s\S]*?;\s*$/gm,
    (match) => match.replace(/[^\n]/g, ' ')
  );

  out = out.replace(
    /^[ \t]*import\s+type\s+\{[\s\S]*?\}\s+from\s+['"][^'"]+['"];?[ \t]*$/gm,
    (match) => match.replace(/[^\n]/g, ' ')
  );

  // 2. Remove `export interface ... { ... }` blocks with balanced braces
  out = stripInterfaceBlocks(out);

  // 3. Remove TypeScript class property declarations without initializers & access modifiers
  out = out.replace(
    /^([ \t]*)(?:public|private|protected)\s+(?:readonly\s+)?([A-Za-z0-9_]+)\s*:\s*[^=;\n]+;([ \t]*)$/gm,
    '$1// field $2$3'
  );
  out = out.replace(
    /\b(?:public|private|protected)\s+(?:readonly\s+)?/g,
    ''
  );
  out = out.replace(/\breadonly\s+/g, '');

  // 4. Remove definite assignment `let releaseLock!:` and non-null assertions `this.server!.`
  out = out.replace(/(\b(?:let|const)\s+[A-Za-z0-9_]+)!/g, '$1');
  out = out.replace(/([A-Za-z0-9_)\]])!(?=\.|\[|\s*;|\s*\))/g, '$1');

  // 5. Strip direct `(param: { ... })` and `): { ... }` and `as { ... }` object type annotations FIRST
  out = stripColonObjectTypes(out);
  out = stripAsObjectTypes(out);

  // 6. Strip generic type arguments `<...>` on `Map`, `Set`, `Promise`, `Record`, `Partial`
  out = stripGenericCallsAndDecls(out);

  // 7. Strip function return types `): TypeName` before `{` or `=>` while preserving newlines
  out = out.replace(
    /\):[ \t]*(?:[A-Za-z0-9_$.<>[\]|&'"]|\s)+(?=\s*(?:\{|=>))/g,
    (m) => ')' + m.slice(1).replace(/[^\n]/g, ' ')
  );

  // 8. Strip type predicates `(s): s is SqliteSnapshot =>`
  out = out.replace(
    /\(([A-Za-z0-9_]+)\):[ \t]*[A-Za-z0-9_]+\s+is\s+[A-Za-z0-9_.]+\s*=>/g,
    '($1) =>'
  );

  // 9. Strip arrow function type annotations like `mutator: (tables: LogicalTableRows) => void` or `let releaseLock: () => void;`
  out = out.replace(
    /([A-Za-z0-9_?]):[ \t]*\([^)]*\)\s*=>\s*[A-Za-z0-9_.]+(?=\s*[,)=;])/g,
    '$1'
  );

  // 10. Strip parameter/variable/field type annotations `ident: Type` (excluding bare `null`/`undefined` as first atom so object literal `before: undefined` is never touched)
  const paramTypeAtom = `(?:1\\s*\\|\\s*2\\s*\\|\\s*3|'pre'\\s*\\|\\s*'post'|import\\(['"][^'"]+['"]\\)\\.[A-Za-z0-9_.]+|[a-z_][A-Za-z0-9_]*\\.[A-Z][A-Za-z0-9_.]*|(?:string|number|boolean|unknown|void|any|bigint|object|never|[A-Z][A-Za-z0-9_.]*)(?:\\[\\])*)`;
  const unionMemberAtom = `(?:${paramTypeAtom}|null|undefined)`;
  const paramTypeRe = new RegExp(
    `([A-Za-z0-9_?]):[ \\t]*${paramTypeAtom}(?:\\s*\\|\\s*${unionMemberAtom})*(?=\\s*[,)=;])`,
    'g'
  );
  out = out.replace(paramTypeRe, '$1');

  // 11. Remove optional parameter question marks `param?:` or `param?` in parameter lists
  out = out.replace(/([(\s,][A-Za-z0-9_]+)\?(?=\s*[,)=])/g, '$1');

  // 12. Remove `as ...` type assertions (but NEVER `import * as foo` or `export * as foo`)
  const asTypeAtom = `(?:${unionMemberAtom}|'[^']*'|"[^"]*")`;
  const asTypeRe = new RegExp(
    `(?<!\\*)\\s+as\\s+(?:Record<[^>;\\n]+>|${asTypeAtom}(?:\\s*\\|\\s*${asTypeAtom})*)`,
    'g'
  );
  out = out.replace(asTypeRe, '');

  return out;
}

function stripInterfaceBlocks(source) {
  const regex = /^[ \t]*(?:export\s+)?interface\s+[A-Za-z0-9_<>]+\s*\{/gm;
  let result = source.split('');
  let match;
  while ((match = regex.exec(source)) !== null) {
    const startIdx = match.index;
    let braceStart = match.index + match[0].length - 1;
    let depth = 1;
    let i = braceStart + 1;
    while (i < source.length && depth > 0) {
      if (source[i] === '{') depth++;
      else if (source[i] === '}') depth--;
      i++;
    }
    for (let k = startIdx; k < i; k++) {
      if (result[k] !== '\n') result[k] = ' ';
    }
  }
  return result.join('');
}

function stripGenericCallsAndDecls(source) {
  const targets = ['Map', 'Set', 'Promise', 'Record', 'Partial'];
  let out = source;
  for (const t of targets) {
    const re = new RegExp(`\\b(${t})\\s*<`, 'g');
    let match;
    const chars = out.split('');
    while ((match = re.exec(out)) !== null) {
      const openIdx = match.index + match[0].length - 1;
      let depth = 1;
      let i = openIdx + 1;
      while (i < out.length && depth > 0) {
        if (out[i] === '<') depth++;
        else if (out[i] === '>') depth--;
        i++;
      }
      for (let k = openIdx; k < i; k++) {
        if (chars[k] !== '\n') chars[k] = ' ';
      }
    }
    out = chars.join('');
  }
  return out;
}

function stripColonObjectTypes(source) {
  const chars = source.split('');
  const re = /(?:(\([ \t]*[A-Za-z0-9_]+\??|\)):[ \t]*)\{/g;
  let match;
  while ((match = re.exec(source)) !== null) {
    const colonIdx = source.indexOf(':', match.index);
    const openBraceIdx = match.index + match[0].length - 1;
    let depth = 1;
    let i = openBraceIdx + 1;
    while (i < source.length && depth > 0) {
      if (source[i] === '{') depth++;
      else if (source[i] === '}') depth--;
      i++;
    }
    if (source.slice(i, i + 2) === '[]') i += 2;
    for (let k = colonIdx; k < i; k++) {
      if (chars[k] !== '\n') chars[k] = ' ';
    }
  }
  return chars.join('');
}

function stripAsObjectTypes(source) {
  const chars = source.split('');
  const re = /\s+as\s+\{/g;
  let match;
  while ((match = re.exec(source)) !== null) {
    const startIdx = match.index;
    const openBraceIdx = match.index + match[0].length - 1;
    let depth = 1;
    let i = openBraceIdx + 1;
    while (i < source.length && depth > 0) {
      if (source[i] === '{') depth++;
      else if (source[i] === '}') depth--;
      i++;
    }
    for (let k = startIdx; k < i; k++) {
      if (chars[k] !== '\n') chars[k] = ' ';
    }
  }
  return chars.join('');
}

function walkFiles(dir, ext) {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  let files = [];
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      files = files.concat(walkFiles(full, ext));
    } else if (e.isFile() && full.endsWith(ext)) {
      files.push(full);
    }
  }
  return files;
}

function main() {
  const tsFiles = [
    ...walkFiles(path.join(ROOT_DIR, 'src'), '.ts'),
    ...walkFiles(path.join(ROOT_DIR, 'test'), '.ts'),
  ];

  for (const file of tsFiles) {
    const rel = path.relative(ROOT_DIR, file).split(path.sep).join('/');
    const outRel = rel.replace(/^src\//, '').replace(/\.ts$/, '.js');
    const outFile = rel.startsWith('test/')
      ? path.join(DIST_DIR, rel.replace(/\.ts$/, '.js'))
      : path.join(DIST_DIR, outRel);

    const rawTs = fs.readFileSync(file, 'utf8');
    let js = transpileTsToJs(rawTs);
    if (rel.startsWith('test/')) {
      js = js.replace(/from\s+['"]\.\.\/src\/([^'"]+)['"]/g, "from '../$1'");
    }

    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    fs.writeFileSync(outFile, js, 'utf8');

    execFileSync(process.execPath, ['--check', outFile], { stdio: 'inherit' });
  }

  console.log(`[build-ts] Compiled and syntax-verified ${tsFiles.length} TypeScript modules to dist/`);
}

main();
