/**
 * Integration helpers: talk to the real barrel_server over REST to set
 * up databases, spaces, and capability grants for the sync tests.
 */
export const BASE = process.env.BARREL_URL ?? "http://127.0.0.1:18080";
export const TOKEN = process.env.BARREL_TOKEN ?? "itest-token";

let counter = 0;
export function uniqueDb(prefix = "lit"): string {
  counter += 1;
  // a random suffix keeps names unique across vitest's isolated files
  // (each file's counter resets and Date.now() can coincide)
  const rand = Math.floor(Math.random() * 0xffffff).toString(36);
  return `${prefix}_${Date.now().toString(36)}_${counter}_${rand}`;
}

function authHeaders(token = TOKEN): Record<string, string> {
  return { authorization: `Bearer ${token}` };
}

/** Create a plain database (global token). */
export async function createDb(db: string): Promise<void> {
  const r = await fetch(`${BASE}/db/${db}`, {
    method: "PUT",
    headers: authHeaders(),
  });
  if (!r.ok) throw new Error(`createDb ${db}: ${r.status}`);
}

/** REST PUT a document (global token); returns the server _rev. */
export async function restPut(
  db: string,
  id: string,
  body: Record<string, unknown>,
): Promise<string> {
  const r = await fetch(`${BASE}/db/${db}/doc/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { ...authHeaders(), "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`restPut ${db}/${id}: ${r.status}`);
  const json = (await r.json()) as { rev: string };
  return json.rev;
}

/** REST GET a document (global token); undefined on 404. */
export async function restGet(
  db: string,
  id: string,
): Promise<Record<string, unknown> | undefined> {
  const r = await fetch(`${BASE}/db/${db}/doc/${encodeURIComponent(id)}`, {
    headers: authHeaders(),
  });
  if (r.status === 404) return undefined;
  if (!r.ok) throw new Error(`restGet ${db}/${id}: ${r.status}`);
  return (await r.json()) as Record<string, unknown>;
}

/** Create a space; returns its id (a db name). */
export async function createSpace(label: string): Promise<string> {
  const r = await fetch(`${BASE}/spaces`, {
    method: "POST",
    headers: { ...authHeaders(), "content-type": "application/json" },
    body: JSON.stringify({ label }),
  });
  if (!r.ok) throw new Error(`createSpace: ${r.status}`);
  const json = (await r.json()) as { space: string };
  return json.space;
}

/** Issue a capability token for a space with the given rights. */
export async function grant(
  space: string,
  rights: ("read" | "write" | "admin")[],
): Promise<string> {
  const r = await fetch(`${BASE}/spaces/${space}/grants`, {
    method: "POST",
    headers: { ...authHeaders(), "content-type": "application/json" },
    body: JSON.stringify({ rights }),
  });
  if (!r.ok) throw new Error(`grant: ${r.status}`);
  const json = (await r.json()) as { token: string };
  return json.token;
}
