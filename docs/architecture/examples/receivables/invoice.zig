//! Guest binding for the three field slots of the receivables example.
const std = @import("std");

const host = struct {
    extern "barrel_v1" fn field(slot: u32, dest: [*]u8, capacity: u32) i32;
    extern "barrel_v1" fn emit(key: [*]const u8, key_len: u32, value: [*]const u8, value_len: u32) void;
};

const Slot = enum(u32) { customer = 0, due_on = 1, line_cents = 2 };
const customer_bytes = 256;
const date_bytes = 32;
const max_lines = 64;

pub const Fields = struct {
    customer: [customer_bytes]u8,
    due_on: [date_bytes]u8,
    amounts: [max_lines]i64,

    pub fn read(self: *Fields) ?Document {
        const customer = readField(.customer, &self.customer) orelse return null;
        const due_on = readField(.due_on, &self.due_on) orelse return null;

        // A definite array of up to 64 signed integers uses at most 2 + 64*9 bytes.
        var wire: [2 + max_lines * 9]u8 = undefined;
        var remaining = readField(.line_cents, &wire) orelse return null;
        const array = takeHeader(&remaining);
        if (array.major != 4 or array.argument > max_lines) @trap();
        const amounts = self.amounts[0..@intCast(array.argument)];
        for (amounts) |*amount| {
            const item = takeHeader(&remaining);
            const magnitude = std.math.cast(i64, item.argument) orelse @trap();
            amount.* = switch (item.major) {
                0 => magnitude,
                1 => ~magnitude, // CBOR encodes a negative integer n as -1 - n.
                else => @trap(),
            };
        }
        if (remaining.len != 0) @trap();
        return .{
            .customer = .{ .encoded = customer },
            .due_on = .{ .encoded = due_on },
            .line_cents = amounts,
        };
    }
};

const Text = struct { encoded: []const u8 };

const Document = struct {
    customer: Text,
    due_on: Text,
    line_cents: []const i64,
};

pub fn emit(key: [2]Text, cents: i64) void {
    // Each field is already a complete CBOR text value. The array header
    // followed by the two field encodings is a complete composite key.
    var key_buf: [1 + customer_bytes + date_bytes]u8 = undefined;
    key_buf[0] = 0x82;
    var used: usize = 1;
    for (key) |part| {
        if (part.encoded.len > key_buf.len - used) @trap();
        @memcpy(key_buf[used..][0..part.encoded.len], part.encoded);
        used += part.encoded.len;
    }
    var value_buf: [9]u8 = undefined;
    const value = encodeInteger(&value_buf, cents);
    host.emit(&key_buf, used, value.ptr, value.len);
}

fn readField(slot: Slot, dest: []u8) ?[]const u8 {
    const result = host.field(@backingInt(slot), dest.ptr, dest.len);
    if (result == -1) return null;
    const len = std.math.cast(usize, result) orelse @trap();
    if (len > dest.len) @trap();
    return dest[0..len];
}

const Header = struct { major: u8, argument: u64 };

fn takeHeader(remaining: *[]const u8) Header {
    if (remaining.len == 0) @trap();
    const first = remaining.*[0];
    remaining.* = remaining.*[1..];
    const info = first & 31;
    const width: usize = switch (info) {
        0...23 => return .{ .major = first >> 5, .argument = info },
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        else => @trap(),
    };
    if (width > remaining.len) @trap();
    var argument: u64 = 0;
    for (remaining.*[0..width]) |byte| argument = (argument << 8) | byte;
    remaining.* = remaining.*[width..];
    return .{ .major = first >> 5, .argument = argument };
}

fn encodeInteger(buf: *[9]u8, cents: i64) []const u8 {
    const major: u8 = if (cents < 0) 0x20 else 0;
    const magnitude: u64 = @intCast(if (cents < 0) ~cents else cents);
    if (magnitude < 24) {
        buf[0] = major | @as(u8, @intCast(magnitude));
        return buf[0..1];
    }
    inline for (.{ u8, u16, u32, u64 }, 0..) |T, tag| {
        if (magnitude <= std.math.maxInt(T)) {
            buf[0] = major | @as(u8, 24 + tag);
            std.mem.writeInt(T, buf[1..][0..@sizeOf(T)], @intCast(magnitude), .big);
            return buf[0 .. 1 + @sizeOf(T)];
        }
    }
    unreachable;
}
