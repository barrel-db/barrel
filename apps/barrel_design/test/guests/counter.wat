;; Increments a mutable global on every call and emits its value as
;; both key and value: a fresh instance always emits 1.
(module
  (import "barrel_v1" "emit" (func $emit (param i32 i32 i32 i32)))
  (memory 1 1)
  (global $n (mut i32) (i32.const 0))
  (func (export "map")
    (global.set $n (i32.add (global.get $n) (i32.const 1)))
    ;; CBOR unsigned integers below 24 are one byte
    (i32.store8 (i32.const 0) (global.get $n))
    (call $emit (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1))))
