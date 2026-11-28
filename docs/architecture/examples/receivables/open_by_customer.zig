const std = @import("std");
const invoice = @import("invoice.zig");

export fn map() void {
    var fields: invoice.Fields = undefined;
    const doc = fields.read() orelse return;

    var balance: i128 = 0;
    for (doc.line_cents) |cents| balance += cents;

    const cents = std.math.cast(i64, balance) orelse @trap();
    invoice.emit(.{ doc.customer, doc.due_on }, cents);
}
