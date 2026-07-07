import { expect, test, type Page } from "@playwright/test";

const BARREL_PORT = process.env.BARREL_PORT ?? "18080";
const SERVER = `http://127.0.0.1:${BARREL_PORT}`;
const TOKEN = process.env.BARREL_TOKEN ?? "itest-token";

const HARNESS = "/test/browser/harness.html";

let counter = 0;
function uniqueName(): string {
  counter += 1;
  return `br_${Date.now().toString(36)}_${counter}`;
}

async function restPut(db: string, id: string, body: unknown): Promise<void> {
  const r = await fetch(`${SERVER}/db/${db}/doc/${id}`, {
    method: "PUT",
    headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`restPut ${r.status}`);
}

async function createDb(db: string): Promise<void> {
  const r = await fetch(`${SERVER}/db/${db}`, {
    method: "PUT",
    headers: { authorization: `Bearer ${TOKEN}` },
  });
  if (!r.ok) throw new Error(`createDb ${r.status}`);
}

async function open(
  page: Page,
  name: string,
  db: string,
  multiTab: boolean,
): Promise<boolean> {
  await page.goto(HARNESS);
  await page.waitForFunction(() => (window as unknown as { __ready?: boolean }).__ready === true);
  return page.evaluate(
    async ({ name, remote, multiTab }) => {
      return (window as unknown as {
        bl: { open(n: string, o: unknown): Promise<boolean> };
      }).bl.open(name, { remote, multiTab });
    },
    { name, remote: { url: SERVER, db, token: TOKEN }, multiTab },
  );
}

function call<T>(page: Page, method: string, ...args: unknown[]): Promise<T> {
  return page.evaluate(
    ({ method, args }) => {
      const bl = (window as unknown as { bl: Record<string, (...a: unknown[]) => unknown> }).bl;
      return bl[method]!(...args) as unknown;
    },
    { method, args },
  ) as Promise<T>;
}

test("OPFS persists a write across a page reload", async ({ page }) => {
  const name = uniqueName();
  const db = uniqueName();
  await createDb(db);

  await open(page, name, db, false);
  await call(page, "put", { id: "a", v: 1 });
  await call(page, "close");

  // reopen after a reload: the OPFS snapshot is read back
  await open(page, name, db, false);
  const got = await call<{ v: number } | undefined>(page, "get", "a");
  expect(got).toMatchObject({ v: 1 });
});

test("OPFS persists an attachment across a page reload", async ({ page }) => {
  const name = uniqueName();
  const db = uniqueName();
  await createDb(db);

  await open(page, name, db, false);
  await call(page, "putAttText", "doc1", "note.txt", "attachment body");
  await call(page, "close");

  await open(page, name, db, false);
  const text = await call<string | null>(page, "getAttText", "doc1", "note.txt");
  expect(text).toBe("attachment body");
});

test("two tabs elect one leader; the follower proxies writes", async ({ context }) => {
  const name = uniqueName();
  const db = uniqueName();
  await createDb(db);

  const p1 = await context.newPage();
  const p2 = await context.newPage();

  await open(p1, name, db, true);
  await open(p2, name, db, true);

  // leadership settles asynchronously (Web Locks): wait for exactly one
  await expect
    .poll(async () => {
      const a = await call<boolean>(p1, "isLeader");
      const b = await call<boolean>(p2, "isLeader");
      return (a ? 1 : 0) + (b ? 1 : 0);
    }, { timeout: 10_000 })
    .toBe(1);

  const l1 = await call<boolean>(p1, "isLeader");
  const leader = l1 ? p1 : p2;
  const follower = l1 ? p2 : p1;

  // the follower's write is proxied to the leader's store
  await call(follower, "put", { id: "x", v: 7 });
  const onLeader = await call<{ v: number } | undefined>(leader, "get", "x");
  expect(onLeader).toMatchObject({ v: 7 });
  // and reads back through the follower proxy
  const onFollower = await call<{ v: number } | undefined>(follower, "get", "x");
  expect(onFollower).toMatchObject({ v: 7 });
});

test("a live pull propagates a server doc to the leader tab", async ({ page }) => {
  const name = uniqueName();
  const db = uniqueName();
  await createDb(db);

  await open(page, name, db, false);
  await call(page, "live", {});
  await restPut(db, "srv", { hello: "browser" });

  await expect
    .poll(async () => call<{ hello?: string } | undefined>(page, "get", "srv"), {
      timeout: 15_000,
    })
    .toMatchObject({ hello: "browser" });
});
