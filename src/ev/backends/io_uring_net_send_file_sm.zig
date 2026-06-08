// SPDX-License-Identifier: MIT
//
// State machine for the io_uring native splice (`NetSendFile`) op.
//
// The machine is pure: it owns no fds, allocates nothing, and never touches
// the ring. The caller drives it with two entry points:
//
//   * `start()`  — produces the first action after the pipe has been
//                  acquired. Must only be called once per op.
//   * `onCqe(res)` — fed every io_uring CQE for the op. Returns the next
//                  action.
//   * `resumeAction()` — re-derives the action for the current phase
//                  without consuming a CQE. Used by the submit path when
//                  it was deferred (SQ-full) and is now retrying.
//
// Splitting the SM out of `io_uring.zig` lets us unit-test all transitions
// with fabricated `(phase, res)` tuples — no real pipe, no real socket.
// The SQ-full deferred-resubmit bug described in zio-splice's
// SPLICE-PLAN.md §13 is the specific motivation for `resumeAction()`.

const std = @import("std");
const linux = std.os.linux;

pub const Phase = enum { initial, splicing_in, splicing_out };

/// Result the caller should act on after each transition.
pub const Action = union(enum) {
    /// Issue a `splice(file -> pipe)` SQE for `len` bytes at the SM's
    /// current `offset`.
    submit_in: struct { len: u32 },
    /// Issue a `splice(pipe -> socket)` SQE for `len` bytes.
    submit_out: struct { len: u32 },
    /// Operation finished successfully — call `setResult(.net_send_file, total)`.
    complete: struct { total: usize },
    /// Operation failed — `errno` is the raw negative CQE result. The
    /// caller maps it to the appropriate error set using `phase`.
    fail: struct { errno: i32 },
};

pub const State = struct {
    phase: Phase = .initial,
    /// Current file offset (advances as splice_out completes).
    offset: u64 = 0,
    /// Bytes still allowed to read from the file.
    remaining: usize = 0,
    /// Bytes successfully sent to the socket. Returned as the op result.
    total: usize = 0,
    /// Bytes currently sitting in the pipe waiting to be drained to the socket.
    /// Set when entering `.splicing_out`, decremented on each (possibly short)
    /// pipe->socket completion.
    pipe_drain_remaining: u32 = 0,
    /// Cap on each `splice(file -> pipe)` request — typically the pipe's
    /// kernel-set capacity from `F_GETPIPE_SZ`.
    pipe_size: u32 = 0,

    /// Begin the op. Transitions `.initial -> .splicing_in` and returns
    /// the first SQE the caller should issue.
    pub fn start(self: *State, offset: u64, remaining: usize, pipe_size: u32) Action {
        std.debug.assert(self.phase == .initial);
        std.debug.assert(pipe_size > 0);
        self.offset = offset;
        self.remaining = remaining;
        self.pipe_size = pipe_size;
        self.phase = .splicing_in;
        return .{ .submit_in = .{ .len = chunkLen(remaining, pipe_size) } };
    }

    /// Consume a CQE. The caller passes the raw `cqe.res` — negative for
    /// errno, zero or positive for byte count.
    pub fn onCqe(self: *State, res: i32) Action {
        if (res < 0) return .{ .fail = .{ .errno = res } };
        const n: u32 = @intCast(res);
        switch (self.phase) {
            .initial => unreachable, // CQE before start() is a caller bug
            .splicing_in => {
                if (n == 0) {
                    // EOF on the source file — done with whatever we've already sent.
                    return .{ .complete = .{ .total = self.total } };
                }
                self.pipe_drain_remaining = n;
                self.phase = .splicing_out;
                return .{ .submit_out = .{ .len = n } };
            },
            .splicing_out => {
                if (n == 0) {
                    // Zero-length pipe->socket completion: socket closed mid-stream.
                    // Synthesize an EPIPE — the caller maps it to ConnectionReset.
                    return .{ .fail = .{ .errno = -@as(i32, @intFromEnum(linux.E.PIPE)) } };
                }
                self.offset += n;
                self.remaining -= n;
                self.total += n;
                self.pipe_drain_remaining -= n;
                if (self.pipe_drain_remaining > 0) {
                    // Short pipe->socket write; drain the rest before reading more.
                    return .{ .submit_out = .{ .len = self.pipe_drain_remaining } };
                }
                if (self.remaining == 0) {
                    return .{ .complete = .{ .total = self.total } };
                }
                self.phase = .splicing_in;
                return .{ .submit_in = .{ .len = chunkLen(self.remaining, self.pipe_size) } };
            },
        }
    }

    /// Re-derive the SQE for the current phase without consuming a CQE.
    /// Called by the backend's submit path after a getSqeOrDefer() retry —
    /// the SM state already reflects what the next SQE should be, so we
    /// just need to read it back out.
    pub fn resumeAction(self: *const State) Action {
        return switch (self.phase) {
            .initial => unreachable,
            .splicing_in => .{ .submit_in = .{ .len = chunkLen(self.remaining, self.pipe_size) } },
            .splicing_out => .{ .submit_out = .{ .len = self.pipe_drain_remaining } },
        };
    }

    fn chunkLen(remaining: usize, pipe_size: u32) u32 {
        const cap: usize = @min(remaining, @as(usize, pipe_size));
        return @intCast(cap);
    }
};

// -----------------------------------------------------------------------------
// Tests — drive the SM with fabricated CQE values. No real pipe / socket / fd.
// -----------------------------------------------------------------------------

const testing = std.testing;

test "start: large remaining clamps to pipe_size" {
    var sm: State = .{};
    const a = sm.start(0, 4 * 1024 * 1024, 1 << 20);
    try testing.expectEqual(@as(u32, 1 << 20), a.submit_in.len);
    try testing.expectEqual(Phase.splicing_in, sm.phase);
    try testing.expectEqual(@as(usize, 4 * 1024 * 1024), sm.remaining);
}

test "start: small remaining keeps full ask" {
    var sm: State = .{};
    const a = sm.start(0, 10_000, 1 << 20);
    try testing.expectEqual(@as(u32, 10_000), a.submit_in.len);
}

test "splicing_in CQE > 0 -> submit_out for that many bytes" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    const a = sm.onCqe(4096);
    try testing.expectEqual(@as(u32, 4096), a.submit_out.len);
    try testing.expectEqual(Phase.splicing_out, sm.phase);
    try testing.expectEqual(@as(u32, 4096), sm.pipe_drain_remaining);
}

test "splicing_in CQE == 0 -> complete with current total" {
    var sm: State = .{};
    _ = sm.start(100, 10_000, 4096);
    sm.total = 9_000; // simulate prior progress
    const a = sm.onCqe(0);
    try testing.expectEqual(@as(usize, 9_000), a.complete.total);
}

test "splicing_out full drain with remaining bytes -> next submit_in" {
    var sm: State = .{};
    _ = sm.start(0, 8192, 4096); // pipe_size = 4096, 2 chunks total
    _ = sm.onCqe(4096); // splicing_in -> splicing_out for 4096
    const a = sm.onCqe(4096); // socket drained full pipe
    try testing.expectEqual(@as(u32, 4096), a.submit_in.len);
    try testing.expectEqual(Phase.splicing_in, sm.phase);
    try testing.expectEqual(@as(u64, 4096), sm.offset);
    try testing.expectEqual(@as(usize, 4096), sm.remaining);
    try testing.expectEqual(@as(usize, 4096), sm.total);
    try testing.expectEqual(@as(u32, 0), sm.pipe_drain_remaining);
}

test "splicing_out full drain, no remaining -> complete" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    _ = sm.onCqe(4096);
    const a = sm.onCqe(4096);
    try testing.expectEqual(@as(usize, 4096), a.complete.total);
}

test "splicing_out short write -> submit_out for the rest of the pipe load" {
    var sm: State = .{};
    _ = sm.start(0, 1 << 20, 1 << 20);
    _ = sm.onCqe(1 << 20); // pipe holds 1 MiB
    const a = sm.onCqe(500_000); // socket only drained 500k
    try testing.expectEqual(@as(u32, (1 << 20) - 500_000), a.submit_out.len);
    try testing.expectEqual(Phase.splicing_out, sm.phase); // stays in splicing_out
    try testing.expectEqual(@as(usize, 500_000), sm.total);
    try testing.expectEqual(@as(u64, 500_000), sm.offset);
    try testing.expectEqual(@as(u32, (1 << 20) - 500_000), sm.pipe_drain_remaining);
}

test "splicing_in CQE < 0 -> fail" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    const a = sm.onCqe(-@as(i32, @intFromEnum(linux.E.IO)));
    try testing.expectEqual(@as(i32, -@as(i32, @intFromEnum(linux.E.IO))), a.fail.errno);
    try testing.expectEqual(Phase.splicing_in, sm.phase); // caller inspects phase for mapping
}

test "splicing_out CQE < 0 -> fail with phase preserved" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    _ = sm.onCqe(4096);
    const a = sm.onCqe(-@as(i32, @intFromEnum(linux.E.CONNRESET)));
    try testing.expectEqual(@as(i32, -@as(i32, @intFromEnum(linux.E.CONNRESET))), a.fail.errno);
    try testing.expectEqual(Phase.splicing_out, sm.phase);
}

test "splicing_out CQE == 0 -> fail with EPIPE (socket dead mid-stream)" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    _ = sm.onCqe(4096);
    const a = sm.onCqe(0);
    try testing.expectEqual(@as(i32, -@as(i32, @intFromEnum(linux.E.PIPE))), a.fail.errno);
}

test "splicing_in cancel -> fail with ECANCELED" {
    var sm: State = .{};
    _ = sm.start(0, 4096, 4096);
    const a = sm.onCqe(-@as(i32, @intFromEnum(linux.E.CANCELED)));
    try testing.expectEqual(@as(i32, -@as(i32, @intFromEnum(linux.E.CANCELED))), a.fail.errno);
}

test "resumeAction in .splicing_in derives the right SQE for SQ-full retry" {
    var sm: State = .{};
    _ = sm.start(0, 8192, 4096);
    // Pretend submit was deferred — state was set but no SQE went out.
    const a = sm.resumeAction();
    try testing.expectEqual(@as(u32, 4096), a.submit_in.len);
}

test "resumeAction in .splicing_out preserves drain_remaining" {
    var sm: State = .{};
    _ = sm.start(0, 1 << 20, 1 << 20);
    _ = sm.onCqe(1 << 20); // now in splicing_out with pipe_drain_remaining = 1 MiB
    _ = sm.onCqe(700_000); // short write — stays in splicing_out
    const a = sm.resumeAction();
    try testing.expectEqual(@as(u32, (1 << 20) - 700_000), a.submit_out.len);
}

test "resumeAction is idempotent — multiple retries yield identical SQE" {
    var sm: State = .{};
    _ = sm.start(0, 8192, 4096);
    const a1 = sm.resumeAction();
    const a2 = sm.resumeAction();
    const a3 = sm.resumeAction();
    try testing.expectEqual(a1.submit_in.len, a2.submit_in.len);
    try testing.expectEqual(a2.submit_in.len, a3.submit_in.len);
    try testing.expectEqual(Phase.splicing_in, sm.phase);
}

test "walk a complete 2-chunk transfer" {
    var sm: State = .{};
    _ = sm.start(0, 8192, 4096);
    // chunk 1
    _ = sm.onCqe(4096); // file->pipe
    _ = sm.onCqe(4096); // pipe->socket -> back to splicing_in
    try testing.expectEqual(Phase.splicing_in, sm.phase);
    try testing.expectEqual(@as(usize, 4096), sm.total);
    // chunk 2
    _ = sm.onCqe(4096);
    const final = sm.onCqe(4096);
    try testing.expectEqual(@as(usize, 8192), final.complete.total);
}

test "walk a transfer with short writes on both legs" {
    var sm: State = .{};
    _ = sm.start(0, 10_000, 8192);
    // file->pipe returns short (e.g. read hit end of disk block)
    var a = sm.onCqe(3000);
    try testing.expectEqual(@as(u32, 3000), a.submit_out.len);
    // pipe->socket also returns short
    a = sm.onCqe(1000);
    try testing.expectEqual(Phase.splicing_out, sm.phase);
    try testing.expectEqual(@as(u32, 2000), a.submit_out.len);
    // drain remaining of this load
    a = sm.onCqe(2000);
    try testing.expectEqual(Phase.splicing_in, sm.phase);
    try testing.expectEqual(@as(u32, 7000), a.submit_in.len);
    try testing.expectEqual(@as(usize, 3000), sm.total);
    try testing.expectEqual(@as(u64, 3000), sm.offset);
    // load second chunk fully
    a = sm.onCqe(7000);
    try testing.expectEqual(@as(u32, 7000), a.submit_out.len);
    // drain
    const final = sm.onCqe(7000);
    try testing.expectEqual(@as(usize, 10_000), final.complete.total);
}

test "EINTR-style flow: submit path uses resumeAction after spurious wakeup" {
    // Mimic the loop poll() pushing the completion back onto pending on EINTR.
    // The submit handler then re-runs without a CQE — must NOT call onCqe(),
    // and must re-derive the SQE from current state.
    var sm: State = .{};
    _ = sm.start(0, 8192, 4096);
    _ = sm.onCqe(4096); // now in splicing_out, pipe_drain_remaining = 4096
    // Pretend the pipe->socket SQE returned -EINTR. Loop pushed us back onto
    // pending; submit will call resumeAction() — state unchanged.
    const a = sm.resumeAction();
    try testing.expectEqual(Phase.splicing_out, sm.phase);
    try testing.expectEqual(@as(u32, 4096), a.submit_out.len);
}
