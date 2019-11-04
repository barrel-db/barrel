-record(view, {barrel,
               ref,
               mod,
               config}).

-define(view(BarrelId, ViewId), {n, l, {barrel_view, {BarrelId, ViewId}}}).
-define(view_proc(BarrelId, ViewId), {via, gproc, ?view(BarrelId, ViewId)}).
