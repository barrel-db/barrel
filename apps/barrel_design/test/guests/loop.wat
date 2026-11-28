;; Never returns: stopped by fuel or by the wall-clock deadline.
(module
  (memory 1 1)
  (func (export "map") (loop $l (br $l))))
