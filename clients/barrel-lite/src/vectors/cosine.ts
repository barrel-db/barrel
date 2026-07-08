/**
 * Cosine similarity, matching the server's vector ranking. The server
 * score is 1 - cosine_distance = dot(a,b)/(‖a‖·‖b‖), sorted descending
 * (barrel_vectordb_hnsw). Vectors are NOT assumed unit length: divide
 * by both norms at compare time, exactly as the server does.
 */
export function dot(a: Float32Array, b: Float32Array): number {
  const n = Math.min(a.length, b.length);
  let sum = 0;
  for (let i = 0; i < n; i++) sum += (a[i] as number) * (b[i] as number);
  return sum;
}

export function norm(a: Float32Array): number {
  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const v = a[i] as number;
    sum += v * v;
  }
  return Math.sqrt(sum);
}

/** Cosine similarity in [-1, 1]; 0 when either vector has zero norm. */
export function cosine(a: Float32Array, b: Float32Array): number {
  const na = norm(a);
  const nb = norm(b);
  if (na === 0 || nb === 0) return 0;
  return dot(a, b) / (na * nb);
}
