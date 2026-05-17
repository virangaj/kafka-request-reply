# Contributing & Release Guide

This document covers everything needed to develop, version, and publish `kafka-request-reply` to npm.

---

## Table of Contents

- [Project Setup](#project-setup)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Building](#building)
- [First Time npm Publish](#first-time-npm-publish)
- [Releasing a New Version](#releasing-a-new-version)
- [Version Guidelines](#version-guidelines)
- [Linking GitHub](#linking-github)
- [Checklist Before Every Release](#checklist-before-every-release)

---

## Project Setup

### Prerequisites

- Node.js >= 18
- npm >= 9
- Git

### Install dependencies

```bash
git clone https://github.com/YOUR_USERNAME/kafka-request-reply.git
cd kafka-request-reply
npm install
```

### Link locally for development testing

```bash
# in the package folder
npm run build
npm link

# in your test project
npm link kafka-request-reply
```

Every time you change source files, rebuild the package:

```bash
npm run build
```

Or watch mode — rebuilds automatically on every save (JS only, not types):

```bash
npm run dev
```

---

## Project Structure

```
kafka-request-reply/
├── src/
│   ├── index.ts                        ← main entry point (Node.js)
│   ├── types/
│   │   └── index.ts                    ← all shared TypeScript interfaces
│   ├── core/
│   │   ├── kafka-client.ts             ← KafkaClient top-level class
│   │   ├── kafka-producer.ts           ← emit() and request() logic
│   │   ├── kafka-consumer.manager.ts   ← subscribe, run, late-subscribe
│   │   └── utils.ts                    ← generateCorrelationId, parseEnvelope
│   ├── decorators/
│   │   └── kafka-consumer.decorator.ts ← @KafkaConsumer (WeakMap based)
│   ├── registry/
│   │   └── kafka-consumer.registry.ts  ← routes messages to handlers
│   └── nestjs/
│       ├── index.ts                    ← NestJS subpath entry point
│       ├── kafka.module.ts             ← register() and registerAsync()
│       ├── kafka-consumer.explorer.ts  ← auto-discovers @KafkaConsumer
│       └── kafka.constants.ts          ← DI injection tokens
├── dist/                               ← built output (never edit manually)
├── tsconfig.json                       ← TypeScript config for development
├── tsconfig.build.json                 ← TypeScript config for declarations only
├── tsup.config.ts                      ← tsup bundler config (JS output)
├── package.json
├── README.md                           ← user-facing documentation
└── CONTRIBUTING.md                     ← this file
```

---

## Development Workflow

### Making changes

1. Edit files inside `src/`
2. Run `npm run build` to compile
3. Test with your linked local project
4. Commit changes

### Key things to know

**Decorator** — uses a `WeakMap` instead of `reflect-metadata`. No `experimentalDecorators` required from users. Both legacy and modern TC39 decorator syntax are supported.

**Registry** — `KafkaConsumerRegistry.execute()` decides the message path:
- Has a registered handler → request path → call handler → auto-reply if headers present
- No handler + has `correlationId` header → reply path → resolve pending Promise
- No handler + no `correlationId` → unknown topic → warn and drop

**Producer** — injects `resolveReply`, `rejectReply`, and `sendRaw` callbacks into the registry via `bindProducer()` to avoid circular imports.

**Multi-pod** — reply topics must be per-pod. Users inject `POD_NAME` from Kubernetes downward API and append it to reply topic names.

---

## Building

```bash
npm run build
```

This runs two commands in sequence:

1. `tsup` — compiles `src/index.ts` and `src/nestjs/index.ts` into both CJS (`.js`) and ESM (`.mjs`) formats
2. `tsc -p tsconfig.build.json` — generates clean `.d.ts` declaration files mirroring the source folder structure

Output goes to `dist/`:

```
dist/
├── index.js          ← CJS
├── index.mjs         ← ESM
├── index.d.ts        ← TypeScript declarations
├── nestjs/
│   ├── index.js
│   ├── index.mjs
│   └── index.d.ts
├── core/
├── decorators/
├── registry/
└── types/
```

Only `dist/` is published to npm (controlled by the `files` field in `package.json`).

---

## First Time npm Publish

### 1. Create an npm account

Go to [npmjs.com](https://www.npmjs.com) and sign up.

### 2. Login from terminal

```bash
npm login
```

Enter your username, password, and OTP if 2FA is enabled.

### 3. Check the package name is available

```bash
npm search kafka-request-reply
```

Or visit [npmjs.com/package/kafka-request-reply](https://www.npmjs.com/package/kafka-request-reply) directly.

### 4. Verify `package.json` is complete

Make sure these fields are set:

```json
{
  "name": "kafka-request-reply",
  "version": "1.0.0",
  "description": "...",
  "main": "dist/index.js",
  "module": "dist/index.mjs",
  "types": "dist/index.d.ts",
  "files": ["dist"],
  "repository": {
    "type": "git",
    "url": "https://github.com/YOUR_USERNAME/kafka-request-reply.git"
  },
  "homepage": "https://github.com/YOUR_USERNAME/kafka-request-reply#readme",
  "bugs": {
    "url": "https://github.com/YOUR_USERNAME/kafka-request-reply/issues"
  }
}
```

### 5. Preview what will be published

```bash
npm pack --dry-run
```

You should only see files from `dist/`. If you see `src/` or `node_modules/` something is wrong with the `files` field.

### 6. Build

```bash
npm run build
```

### 7. Publish

```bash
npm publish
```

If using a scoped name like `@yourname/kafka-request-reply`:

```bash
npm publish --access public
```

### 8. Verify it published

```bash
npm info kafka-request-reply
```

Or visit `https://www.npmjs.com/package/kafka-request-reply`.

---

## Releasing a New Version

### Step-by-step

```bash
# 1. Make sure you are on main and everything is committed
git checkout main
git pull origin main
git status   # should be clean

# 2. Run your tests (when added)
# npm test

# 3. Bump the version — this updates package.json AND creates a git tag
npm version patch   # for bug fixes
npm version minor   # for new features
npm version major   # for breaking changes

# 4. Build
npm run build

# 5. Preview what will be published
npm pack --dry-run

# 6. Publish to npm
npm publish

# 7. Push commits and tags to GitHub
git push origin main --tags
```

### Create a GitHub release

After pushing the tag:

1. Go to your repo on GitHub
2. Click **Releases** → **Draft a new release**
3. Select the tag you just pushed (e.g. `v1.0.1`)
4. Write release notes (see [Version Guidelines](#version-guidelines))
5. Click **Publish release**

---

## Version Guidelines

Follow [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`

| Change type | Command | Example | When to use |
|---|---|---|---|
| Bug fix | `npm version patch` | `1.0.0 → 1.0.1` | Fix a bug, no API change |
| New feature | `npm version minor` | `1.0.0 → 1.1.0` | Add something new, backward compatible |
| Breaking change | `npm version major` | `1.0.0 → 2.0.0` | Remove or rename API, change behavior |

### What counts as a breaking change

- Renaming or removing any exported class, function, or type
- Changing the signature of `emit()`, `request()`, or `@KafkaConsumer`
- Changing the message envelope format `{ data: ... }`
- Dropping support for a Node.js major version
- Changing `peerDependencies` version requirements

### What is NOT a breaking change

- Adding a new optional parameter with a default value
- Adding a new export
- Internal refactors that don't affect the public API
- Dependency version bumps (unless they change peer requirements)
- Performance improvements
- Documentation updates

### Release notes template

```md
## v1.1.0

### What's new
- Added X feature

### Bug fixes
- Fixed Y issue

### Internal
- Refactored Z

### Migration
<!-- only needed for major versions -->
- Before: `producer.request(a, b, c)`
- After: `producer.request(a, b, c, options)`
```

---

## Linking GitHub

### 1. Create the repo

Go to [github.com/new](https://github.com/new). Name it `kafka-request-reply`. Leave it empty.

### 2. Add `.gitignore`

```
node_modules/
dist/
*.log
.DS_Store
```

### 3. Initialize and push

```bash
git init
git add .
git commit -m "chore: initial release v1.0.0"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/kafka-request-reply.git
git push -u origin main
```

### 4. Push the initial tag

```bash
git tag v1.0.0
git push origin v1.0.0
```

### 5. Add repo fields to `package.json`

```json
{
  "repository": {
    "type": "git",
    "url": "https://github.com/YOUR_USERNAME/kafka-request-reply.git"
  },
  "homepage": "https://github.com/YOUR_USERNAME/kafka-request-reply#readme",
  "bugs": {
    "url": "https://github.com/YOUR_USERNAME/kafka-request-reply/issues"
  }
}
```

Once published, the npm package page automatically shows a link to GitHub and the GitHub repo shows the package under the **Packages** sidebar.

---

## Checklist Before Every Release

```
□ All changes committed and pushed to main
□ No TypeScript errors  →  npx tsc --noEmit
□ Build succeeds        →  npm run build
□ Dry run looks clean   →  npm pack --dry-run (only dist/ files)
□ Version bumped        →  npm version patch/minor/major
□ Published to npm      →  npm publish
□ Tags pushed           →  git push origin main --tags
□ GitHub release created with notes
```