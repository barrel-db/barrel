;; Emits (customer, cents) from two typed fields: slot 0 text, slot 1 i64.
(module
  (import "barrel_v1" "field" (func $field (param i32 i32 i32) (result i32)))
  (import "barrel_v1" "emit" (func $emit (param i32 i32 i32 i32)))
  (memory (export "memory") 1 1)
  (func (export "map") (local $key i32) (local $value i32)
    (local.set $key (call $field (i32.const 0) (i32.const 0) (i32.const 1024)))
    (if (i32.lt_s (local.get $key) (i32.const 0)) (then return))
    (local.set $value (call $field (i32.const 1) (i32.const 1024) (i32.const 16)))
    (if (i32.ge_s (local.get $value) (i32.const 0))
      (then (call $emit (i32.const 0) (local.get $key)
                        (i32.const 1024) (local.get $value))))))
