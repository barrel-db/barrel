;; Emits one row per element of an i64 array field (slot 0), keyed by
;; [index] with the element as value, up to 64 elements.
(module
  (import "barrel_v1" "field" (func $field (param i32 i32 i32) (result i32)))
  (import "barrel_v1" "emit" (func $emit (param i32 i32 i32 i32)))
  (memory 1 1)
  (func (export "map") (local $len i32) (local $n i32) (local $i i32) (local $p i32) (local $w i32)
    (local.set $len (call $field (i32.const 0) (i32.const 0) (i32.const 4096)))
    (if (i32.lt_s (local.get $len) (i32.const 1)) (then return))
    ;; array header: 0x80 | n for n < 24, 0x98 n for n < 256
    (local.set $n (i32.and (i32.load8_u (i32.const 0)) (i32.const 31)))
    (local.set $p (i32.const 1))
    (if (i32.eq (local.get $n) (i32.const 24))
      (then (local.set $n (i32.load8_u (i32.const 1))) (local.set $p (i32.const 2))))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
        ;; key: [i] as a one-element CBOR array of a small unsigned int
        (i32.store8 (i32.const 8000) (i32.const 0x81))
        (i32.store8 (i32.const 8001) (local.get $i))
        ;; value width from the item header
        (local.set $w (call $width (i32.load8_u (local.get $p))))
        (call $emit (i32.const 8000) (i32.const 2) (local.get $p) (local.get $w))
        (local.set $p (i32.add (local.get $p) (local.get $w)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next))))
  (func $width (param $b i32) (result i32)
    (local $info i32)
    (local.set $info (i32.and (local.get $b) (i32.const 31)))
    (if (result i32) (i32.lt_u (local.get $info) (i32.const 24))
      (then (i32.const 1))
      (else (i32.add (i32.const 1)
                     (i32.shl (i32.const 1) (i32.sub (local.get $info) (i32.const 24))))))))
