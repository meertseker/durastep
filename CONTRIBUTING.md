# Contributing

Thanks for considering a contribution. durastep is small on purpose; the bar
for new surface area is "does a team running a real pipeline need this, and
can it be explained in guarantees.md".

## Setup

```sh
git clone https://github.com/meertseker/durastep.git
cd durastep
npm install
npm test          # needs nothing but Node 20+
npm run example   # in-memory end-to-end run
```

Optional adapter suites (Postgres store and queue; BullMQ needs Redis too):

```sh
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=pw postgres:16-alpine
docker run --rm -d -p 6379:6379 redis:7-alpine
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres REDIS_URL=redis://localhost:6379 npm test
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run example:postgres
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run demo:kill
```

## Before you open a pull request

```sh
npm run lint && npm run typecheck && npm test && npm run build && npm run example
```

CI runs the same commands on Node 20 and 22.

## Guidelines

- **Tests first for behaviour changes.** Orchestrator and executor changes
  need a test that fails without them. Concurrency claims need a test that
  actually races (`test/helpers.ts` has an interleaving store wrapper).
- **Guarantees are a contract.** If a change alters what happens when a
  process dies somewhere, update `docs/guarantees.md` in the same PR.
- **No runtime dependencies in the core.** Adapters may use optional peer
  dependencies.
- **Public API changes** need JSDoc on every export and a CHANGELOG entry.
- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `chore:`).
- **Store implementations** should pass `test/store-contract.ts`.

## Reporting bugs

Use the bug report template. A failing test or a minimal script with
`MemoryStore` + `InlineDispatcher` is the fastest way to a fix.
