;; Emits the text in slot 0 twice as the key, with value 1: one document,
;; two rows under the same key.
(module
  (import "barrel_v1" "field" (func $field (param i32 i32 i32) (result i32)))
  (import "barrel_v1" "emit" (func $emit (param i32 i32 i32 i32)))
  (memory 1 1)
  (func (export "map") (local $len i32)
    (local.set $len (call $field (i32.const 0) (i32.const 0) (i32.const 1024)))
    (if (i32.lt_s (local.get $len) (i32.const 0)) (then return))
    (i32.store8 (i32.const 2000) (i32.const 1))
    (call $emit (i32.const 0) (local.get $len) (i32.const 2000) (i32.const 1))
    (call $emit (i32.const 0) (local.get $len) (i32.const 2000) (i32.const 1))))
