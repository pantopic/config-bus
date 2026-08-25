//! Mirrors module/storage-kv/patch/lcs (Myers LCS over byte sequences).
//!
//! The Go package caps the search depth at maxDiffs/2 = 4, which bounds every
//! intermediate structure to a small constant size, so this port uses fixed
//! buffers and needs no allocator.

const std = @import("std");

pub const max_diffs = 8;

/// Upper bound on the number of Diffs toDiffs can produce (one per diagonal
/// plus one trailing edit).
pub const max_out_diffs = lcs_cap + 1;

/// A Diff is a replacement of a portion of A by a portion of B.
pub const Diff = struct {
    start: usize = 0, // offsets of portion to delete in A
    end: usize = 0,
    repl_start: usize = 0, // offset of replacement text in B
    repl_end: usize = 0,
};

/// A diag is a piece of the edit graph where A[X+i] == B[Y+i], for 0<=i<Len.
const Diag = struct {
    x: isize = 0,
    y: isize = 0,
    len: isize = 0,
};

const lcs_cap = 64;

/// A fixed-capacity list of diagonals.
const Lcs = struct {
    items: [lcs_cap]Diag = undefined,
    len: usize = 0,

    fn slice(l: *Lcs) []Diag {
        return l.items[0..l.len];
    }

    /// sort sorts in place, by lowest X, and if tied, inversely by Len
    fn sort(l: *Lcs) void {
        std.mem.sort(Diag, l.slice(), {}, struct {
            fn less(_: void, a: Diag, b: Diag) bool {
                if (a.x != b.x) return a.x < b.x;
                return a.len > b.len;
            }
        }.less);
    }

    /// prepend a diagonal (x,y)-(x+1,y+1) segment either to an empty lcs
    /// or to its first Diag.
    fn prepend(l: *Lcs, x: isize, y: isize) void {
        if (l.len > 0) {
            const d = &l.items[0];
            if (d.x == x + 1 and d.y == y + 1) {
                // extend the diagonal down and to the left
                d.x = x;
                d.y = y;
                d.len += 1;
                return;
            }
        }
        var i = l.len;
        while (i > 0) : (i -= 1) {
            l.items[i] = l.items[i - 1];
        }
        l.items[0] = .{ .x = x, .y = y, .len = 1 };
        l.len += 1;
    }

    /// append appends a diagonal, or extends the existing one.
    fn append(l: *Lcs, x: isize, y: isize) void {
        if (l.len > 0) {
            const last = &l.items[l.len - 1];
            // Expand last element if adjoining.
            if (last.x + last.len == x and last.y + last.len == y) {
                last.len += 1;
                return;
            }
        }
        l.items[l.len] = .{ .x = x, .y = y, .len = 1 };
        l.len += 1;
    }

    fn appendAll(l: *Lcs, other: *const Lcs) void {
        for (other.items[0..other.len]) |d| {
            l.items[l.len] = d;
            l.len += 1;
        }
    }

    /// repair overlapping lcs; only called if two-sided stops early
    fn fix(l: *Lcs) Lcs {
        if (l.len == 0) return .{};
        std.mem.sort(Diag, l.slice(), {}, struct {
            fn less(_: void, a: Diag, b: Diag) bool {
                return a.len > b.len;
            }
        }.less);
        var tmp = Lcs{};
        tmp.items[0] = l.items[0];
        tmp.len = 1;
        for (l.items[1..l.len]) |item| {
            var dir: Direction = .empty;
            var nxt = item;
            for (tmp.items[0..tmp.len]) |in| {
                const r = overlap(in, nxt);
                dir = r.dir;
                nxt = r.diag;
                if (dir == .empty or dir == .bad) break;
            }
            if (nxt.len > 0 and dir != .bad) {
                tmp.items[tmp.len] = nxt;
                tmp.len += 1;
            }
        }
        tmp.sort();
        return tmp;
    }

    /// toDiffs converts an LCS to a list of edits.
    fn toDiffs(l: *const Lcs, alen: usize, blen: usize, out: *[max_out_diffs]Diff) []Diff {
        var n: usize = 0;
        var pa: isize = 0;
        var pb: isize = 0;
        for (l.items[0..l.len]) |d| {
            if (pa < d.x or pb < d.y) {
                out[n] = .{
                    .start = @intCast(pa),
                    .end = @intCast(d.x),
                    .repl_start = @intCast(pb),
                    .repl_end = @intCast(d.y),
                };
                n += 1;
            }
            pa = d.x + d.len;
            pb = d.y + d.len;
        }
        if (pa < @as(isize, @intCast(alen)) or pb < @as(isize, @intCast(blen))) {
            out[n] = .{
                .start = @intCast(pa),
                .end = alen,
                .repl_start = @intCast(pb),
                .repl_end = blen,
            };
            n += 1;
        }
        return out[0..n];
    }
};

const Direction = enum {
    empty, // diag is empty (so not in lcs)
    leftdown, // proposed acceptably to the left and below
    rightup, // proposed diag is acceptably to the right and above
    bad, // proposed diag is inconsistent with the lcs so far
};

/// overlap trims the proposed diag prop so it doesn't overlap with
/// the existing diag that has already been added to the lcs.
fn overlap(exist: Diag, prop_in: Diag) struct { dir: Direction, diag: Diag } {
    var prop = prop_in;
    if (prop.x <= exist.x and exist.x < prop.x + prop.len) {
        // remove the end of prop where it overlaps with the X end of exist
        const delta = prop.x + prop.len - exist.x;
        prop.len -= delta;
        if (prop.len <= 0) return .{ .dir = .empty, .diag = prop };
    }
    if (exist.x <= prop.x and prop.x < exist.x + exist.len) {
        // remove the beginning of prop where overlaps with exist
        const delta = exist.x + exist.len - prop.x;
        prop.len -= delta;
        if (prop.len <= 0) return .{ .dir = .empty, .diag = prop };
        prop.x += delta;
        prop.y += delta;
    }
    if (prop.y <= exist.y and exist.y < prop.y + prop.len) {
        // remove the end of prop that overlaps (in Y) with exist
        const delta = prop.y + prop.len - exist.y;
        prop.len -= delta;
        if (prop.len <= 0) return .{ .dir = .empty, .diag = prop };
    }
    if (exist.y <= prop.y and prop.y < exist.y + exist.len) {
        // remove the beginning of prop that overlaps with exist
        const delta = exist.y + exist.len - prop.y;
        prop.len -= delta;
        if (prop.len <= 0) return .{ .dir = .empty, .diag = prop };
        prop.x += delta;
        prop.y += delta;
    }
    if (prop.x + prop.len <= exist.x and prop.y + prop.len <= exist.y) {
        return .{ .dir = .leftdown, .diag = prop };
    }
    if (exist.x + exist.len <= prop.x and exist.y + exist.len <= prop.y) {
        return .{ .dir = .rightup, .diag = prop };
    }
    // prop can't be in an lcs that contains exist
    return .{ .dir = .bad, .diag = prop };
}

/// enforce constraint on d, k
fn ok(d: isize, k: isize) bool {
    return d >= 0 and -d <= k and k <= d;
}

// For each D, vec[D] has length D+1,
// and the label for (D, k) is stored in vec[D][(D+k)/2].
const label_rows = max_diffs; // limit is maxDiffs/2; rows never exceed limit+1
const Label = struct {
    vec: [label_rows][label_rows]isize = @splat(@splat(0)),

    fn set(t: *Label, d: isize, k: isize, x: isize) void {
        t.vec[@intCast(d)][@intCast(@divTrunc(d + k, 2))] = x;
    }

    fn get(t: *const Label, d: isize, k: isize) isize {
        return t.vec[@intCast(d)][@intCast(@divTrunc(d + k, 2))];
    }
};

fn commonPrefixLen(a: []const u8, b: []const u8) isize {
    const n = @min(a.len, b.len);
    var i: usize = 0;
    while (i < n and a[i] == b[i]) : (i += 1) {}
    return @intCast(i);
}

fn commonSuffixLen(a: []const u8, b: []const u8) isize {
    const n = @min(a.len, b.len);
    var i: usize = 0;
    while (i < n and a[a.len - 1 - i] == b[b.len - 1 - i]) : (i += 1) {}
    return @intCast(i);
}

/// editGraph carries the information for computing the lcs of two sequences.
const EditGraph = struct {
    a: []const u8,
    b: []const u8,
    vf: Label = .{}, // forward and backward labels
    vb: Label = .{},

    limit: isize, // maximal value of D
    // the bounding rectangle of the current edit graph
    lx: isize = 0,
    ly: isize = 0,
    ux: isize,
    uy: isize,
    delta: isize, // common subexpression: (ux-lx)-(uy-ly)

    fn seqCommonPrefixLen(e: *EditGraph, ai: isize, aj: isize, bi: isize, bj: isize) isize {
        return commonPrefixLen(e.a[@intCast(ai)..@intCast(aj)], e.b[@intCast(bi)..@intCast(bj)]);
    }

    fn seqCommonSuffixLen(e: *EditGraph, ai: isize, aj: isize, bi: isize, bj: isize) isize {
        return commonSuffixLen(e.a[@intCast(ai)..@intCast(aj)], e.b[@intCast(bi)..@intCast(bj)]);
    }

    // --- FORWARD ---

    /// fdone decides if the forward path has reached the upper right
    /// corner of the rectangle. If so, it also returns the computed lcs.
    fn fdone(e: *EditGraph, d: isize, k: isize) ?Lcs {
        // x, y, k are relative to the rectangle
        const x = e.vf.get(d, k);
        const y = x - k;
        if (x == e.ux and y == e.uy) {
            return e.forwardlcs(d, k);
        }
        return null;
    }

    /// run the forward algorithm, until success or up to the limit on D.
    fn forward(e: *EditGraph) Lcs {
        e.setForward(0, 0, e.lx);
        if (e.fdone(0, 0)) |ans| return ans;
        // from D to D+1
        var d: isize = 0;
        while (d < e.limit) : (d += 1) {
            e.setForward(d + 1, -(d + 1), e.getForward(d, -d));
            if (e.fdone(d + 1, -(d + 1))) |ans| return ans;
            e.setForward(d + 1, d + 1, e.getForward(d, d) + 1);
            if (e.fdone(d + 1, d + 1)) |ans| return ans;
            var k = -d + 1;
            while (k <= d - 1) : (k += 2) {
                // these are tricky and easy to get backwards
                const lookv = e.lookForward(k, e.getForward(d, k - 1) + 1);
                const lookh = e.lookForward(k, e.getForward(d, k + 1));
                if (lookv > lookh) {
                    e.setForward(d + 1, k, lookv);
                } else {
                    e.setForward(d + 1, k, lookh);
                }
                if (e.fdone(d + 1, k)) |ans| return ans;
            }
        }
        // D is too large; find the D path with maximal x+y inside the
        // rectangle and use that to compute the found part of the lcs
        var kmax = -e.limit - 1;
        var diagmax: isize = -1;
        var k = -e.limit;
        while (k <= e.limit) : (k += 2) {
            const x = e.getForward(e.limit, k);
            const y = x - k;
            if (x + y > diagmax and x <= e.ux and y <= e.uy) {
                diagmax = x + y;
                kmax = k;
            }
        }
        return e.forwardlcs(e.limit, kmax);
    }

    /// recover the lcs by backtracking from the farthest point reached
    fn forwardlcs(e: *EditGraph, d_in: isize, k_in: isize) Lcs {
        var ans = Lcs{};
        var d = d_in;
        var k = k_in;
        var x = e.getForward(d, k);
        while (x != 0 or x - k != 0) {
            if (ok(d - 1, k - 1) and x - 1 == e.getForward(d - 1, k - 1)) {
                // if (x-1,y) is labelled D-1, x--,D--,k--,continue
                d -= 1;
                k -= 1;
                x -= 1;
                continue;
            } else if (ok(d - 1, k + 1) and x == e.getForward(d - 1, k + 1)) {
                // if (x,y-1) is labelled D-1, x, D--,k++, continue
                d -= 1;
                k += 1;
                continue;
            }
            // if (x-1,y-1)--(x,y) is a diagonal, prepend,x--,y--, continue
            const y = x - k;
            ans.prepend(x + e.lx - 1, y + e.ly - 1);
            x -= 1;
        }
        return ans;
    }

    /// start at (x,y), go up the diagonal as far as possible,
    /// and label the result with d
    fn lookForward(e: *EditGraph, k: isize, relx: isize) isize {
        const rely = relx - k;
        var x = relx + e.lx;
        const y = rely + e.ly;
        if (x < e.ux and y < e.uy) {
            x += e.seqCommonPrefixLen(x, e.ux, y, e.uy);
        }
        return x;
    }

    fn setForward(e: *EditGraph, d: isize, k: isize, relx: isize) void {
        const x = e.lookForward(k, relx);
        e.vf.set(d, k, x - e.lx);
    }

    fn getForward(e: *EditGraph, d: isize, k: isize) isize {
        return e.vf.get(d, k);
    }

    // --- BACKWARD ---

    /// bdone decides if the backward path has reached the lower left corner
    fn bdone(e: *EditGraph, d: isize, k: isize) ?Lcs {
        // x, y, k are relative to the rectangle
        const x = e.vb.get(d, k);
        const y = x - (k + e.delta);
        if (x == 0 and y == 0) {
            return e.backwardlcs(d, k);
        }
        return null;
    }

    /// run the backward algorithm, until success or up to the limit on D.
    fn backward(e: *EditGraph) Lcs {
        e.setBackward(0, 0, e.ux);
        if (e.bdone(0, 0)) |ans| return ans;
        // from D to D+1
        var d: isize = 0;
        while (d < e.limit) : (d += 1) {
            e.setBackward(d + 1, -(d + 1), e.getBackward(d, -d) - 1);
            if (e.bdone(d + 1, -(d + 1))) |ans| return ans;
            e.setBackward(d + 1, d + 1, e.getBackward(d, d));
            if (e.bdone(d + 1, d + 1)) |ans| return ans;
            var k = -d + 1;
            while (k <= d - 1) : (k += 2) {
                // these are tricky and easy to get wrong
                const lookv = e.lookBackward(k, e.getBackward(d, k - 1));
                const lookh = e.lookBackward(k, e.getBackward(d, k + 1) - 1);
                if (lookv < lookh) {
                    e.setBackward(d + 1, k, lookv);
                } else {
                    e.setBackward(d + 1, k, lookh);
                }
                if (e.bdone(d + 1, k)) |ans| return ans;
            }
        }

        // D is too large; find the D path with minimal x+y inside the
        // rectangle and use that to compute the part of the lcs found
        var kmax = -e.limit - 1;
        var diagmin: isize = 1 << 25;
        var k = -e.limit;
        while (k <= e.limit) : (k += 2) {
            const x = e.getBackward(e.limit, k);
            const y = x - (k + e.delta);
            if (x + y < diagmin and x >= 0 and y >= 0) {
                diagmin = x + y;
                kmax = k;
            }
        }
        if (kmax < -e.limit) {
            @panic("lcs: no paths");
        }
        return e.backwardlcs(e.limit, kmax);
    }

    /// recover the lcs by backtracking
    fn backwardlcs(e: *EditGraph, d_in: isize, k_in: isize) Lcs {
        var ans = Lcs{};
        var d = d_in;
        var k = k_in;
        var x = e.getBackward(d, k);
        while (x != e.ux or x - (k + e.delta) != e.uy) {
            if (ok(d - 1, k - 1) and x == e.getBackward(d - 1, k - 1)) {
                // D--, k--, x unchanged
                d -= 1;
                k -= 1;
                continue;
            } else if (ok(d - 1, k + 1) and x + 1 == e.getBackward(d - 1, k + 1)) {
                // D--, k++, x++
                d -= 1;
                k += 1;
                x += 1;
                continue;
            }
            const y = x - (k + e.delta);
            ans.append(x + e.lx, y + e.ly);
            x += 1;
        }
        return ans;
    }

    /// start at (x,y), go down the diagonal as far as possible
    fn lookBackward(e: *EditGraph, k: isize, relx: isize) isize {
        const rely = relx - (k + e.delta); // forward k = k + e.delta
        var x = relx + e.lx;
        const y = rely + e.ly;
        if (x > 0 and y > 0) {
            x -= e.seqCommonSuffixLen(0, x, 0, y);
        }
        return x;
    }

    /// convert to rectangle, and label the result with d
    fn setBackward(e: *EditGraph, d: isize, k: isize, relx: isize) void {
        const x = e.lookBackward(k, relx);
        e.vb.set(d, k, x - e.lx);
    }

    fn getBackward(e: *EditGraph, d: isize, k: isize) isize {
        return e.vb.get(d, k);
    }

    // -- TWOSIDED ---

    fn twosided(e: *EditGraph) Lcs {
        // The termination condition could be improved, as either the forward
        // or backward pass could succeed before Myers' Lemma applies.
        e.setForward(0, 0, e.lx);
        e.setBackward(0, 0, e.ux);

        // from D to D+1
        var d: isize = 0;
        while (d < e.limit) : (d += 1) {
            // just finished a backwards pass, so check
            if (e.twoDone(d, d)) |got| {
                return e.twolcs(d, d, got);
            }
            // do a forwards pass (D to D+1)
            e.setForward(d + 1, -(d + 1), e.getForward(d, -d));
            e.setForward(d + 1, d + 1, e.getForward(d, d) + 1);
            var k = -d + 1;
            while (k <= d - 1) : (k += 2) {
                // these are tricky and easy to get backwards
                const lookv = e.lookForward(k, e.getForward(d, k - 1) + 1);
                const lookh = e.lookForward(k, e.getForward(d, k + 1));
                if (lookv > lookh) {
                    e.setForward(d + 1, k, lookv);
                } else {
                    e.setForward(d + 1, k, lookh);
                }
            }
            // just did a forward pass, so check
            if (e.twoDone(d + 1, d)) |got| {
                return e.twolcs(d + 1, d, got);
            }
            // do a backward pass, D to D+1
            e.setBackward(d + 1, -(d + 1), e.getBackward(d, -d) - 1);
            e.setBackward(d + 1, d + 1, e.getBackward(d, d));
            k = -d + 1;
            while (k <= d - 1) : (k += 2) {
                // these are tricky and easy to get wrong
                const lookv = e.lookBackward(k, e.getBackward(d, k - 1));
                const lookh = e.lookBackward(k, e.getBackward(d, k + 1) - 1);
                if (lookv < lookh) {
                    e.setBackward(d + 1, k, lookv);
                } else {
                    e.setBackward(d + 1, k, lookh);
                }
            }
        }

        // D too large. combine a forward and backward partial lcs
        // first, a forward one
        var kmax = -e.limit - 1;
        var diagmax: isize = -1;
        var k = -e.limit;
        while (k <= e.limit) : (k += 2) {
            const x = e.getForward(e.limit, k);
            const y = x - k;
            if (x + y > diagmax and x <= e.ux and y <= e.uy) {
                diagmax = x + y;
                kmax = k;
            }
        }
        if (kmax < -e.limit) {
            @panic("lcs: no forward paths");
        }
        var lcs = e.forwardlcs(e.limit, kmax);
        // now a backward one
        // find the D path with minimal x+y inside the rectangle and
        // use that to compute the lcs
        var diagmin: isize = 1 << 25; // infinity
        k = -e.limit;
        while (k <= e.limit) : (k += 2) {
            const x = e.getBackward(e.limit, k);
            const y = x - (k + e.delta);
            if (x + y < diagmin and x >= 0 and y >= 0) {
                diagmin = x + y;
                kmax = k;
            }
        }
        if (kmax < -e.limit) {
            @panic("lcs: no backward paths");
        }
        const back = e.backwardlcs(e.limit, kmax);
        lcs.appendAll(&back);
        // These may overlap (e.forwardlcs and e.backwardlcs return sorted lcs)
        return lcs.fix();
    }

    /// Does Myers' Lemma apply?
    fn twoDone(e: *EditGraph, df: isize, db: isize) ?isize {
        if (@mod(df + db + e.delta, 2) != 0) {
            return null; // diagonals cannot overlap
        }
        var kmin = -db + e.delta;
        if (-df > kmin) {
            kmin = -df;
        }
        var kmax = db + e.delta;
        if (df < kmax) {
            kmax = df;
        }
        var k = kmin;
        while (k <= kmax) : (k += 2) {
            const x = e.vf.get(df, k);
            const u = e.vb.get(db, k - e.delta);
            if (u <= x) {
                // is it worth looking at all the other k?
                var l = k;
                while (l <= kmax) : (l += 2) {
                    const xx = e.vf.get(df, l);
                    const yy = xx - l;
                    const uu = e.vb.get(db, l - e.delta);
                    const vv = uu - l;
                    if (xx == uu or uu == 0 or vv == 0 or yy == e.uy or xx == e.ux) {
                        return l;
                    }
                }
                return k;
            }
        }
        return null;
    }

    fn twolcs(e: *EditGraph, df: isize, db: isize, kf: isize) Lcs {
        // db==df || db+1==df
        const x = e.vf.get(df, kf);
        const y = x - kf;
        const kb = kf - e.delta;
        const u = e.vb.get(db, kb);
        const v = u - kf;

        // Look for some special cases to avoid computing either of these paths.
        if (x == u) {
            // already patched together
            var lcs = e.forwardlcs(df, kf);
            const back = e.backwardlcs(db, kb);
            lcs.appendAll(&back);
            lcs.sort();
            return lcs;
        }

        // is (u-1,v) or (u,v-1) labelled df-1?
        // if so, that forward df-1-path plus a horizontal or vertical edge
        // is the df-path to (u,v), then plus the db-path to (N,M)
        if (u > 0 and ok(df - 1, u - 1 - v) and e.vf.get(df - 1, u - 1 - v) == u - 1) {
            var lcs = e.forwardlcs(df - 1, u - 1 - v);
            const back = e.backwardlcs(db, kb);
            lcs.appendAll(&back);
            lcs.sort();
            return lcs;
        }
        if (v > 0 and ok(df - 1, u - (v - 1)) and e.vf.get(df - 1, u - (v - 1)) == u) {
            var lcs = e.forwardlcs(df - 1, u - (v - 1));
            const back = e.backwardlcs(db, kb);
            lcs.appendAll(&back);
            lcs.sort();
            return lcs;
        }

        // The path can't possibly contribute to the lcs because it
        // is all horizontal or vertical edges
        if (u == 0 or v == 0 or x == e.ux or y == e.uy) {
            if (u == 0 or v == 0) {
                return e.backwardlcs(db, kb);
            }
            return e.forwardlcs(df, kf);
        }

        // is (x+1,y) or (x,y+1) labelled db-1?
        if (x + 1 <= e.ux and ok(db - 1, x + 1 - y - e.delta) and e.vb.get(db - 1, x + 1 - y - e.delta) == x + 1) {
            var lcs = e.backwardlcs(db - 1, kb + 1);
            const fwd = e.forwardlcs(df, kf);
            lcs.appendAll(&fwd);
            lcs.sort();
            return lcs;
        }
        if (y + 1 <= e.uy and ok(db - 1, x - (y + 1) - e.delta) and e.vb.get(db - 1, x - (y + 1) - e.delta) == x) {
            var lcs = e.backwardlcs(db - 1, kb - 1);
            const fwd = e.forwardlcs(df, kf);
            lcs.appendAll(&fwd);
            lcs.sort();
            return lcs;
        }

        // need to compute another path
        var lcs = e.backwardlcs(db, kb);
        const oldx = e.ux;
        const oldy = e.uy;
        e.ux = u;
        e.uy = v;
        const fwd = e.forward();
        lcs.appendAll(&fwd);
        e.ux = oldx;
        e.uy = oldy;
        lcs.sort();
        return lcs;
    }
};

/// DiffBytes returns the differences between two byte sequences.
/// The result is a slice into `out`.
pub fn diffBytes(a: []const u8, b: []const u8, out: *[max_out_diffs]Diff) []Diff {
    // A limit on how deeply the LCS algorithm should search.
    const limit: isize = max_diffs / 2;
    var g = EditGraph{
        .a = a,
        .b = b,
        .limit = limit,
        .ux = @intCast(a.len),
        .uy = @intCast(b.len),
        .delta = @as(isize, @intCast(a.len)) - @as(isize, @intCast(b.len)),
    };
    const lcs = g.twosided();
    return lcs.toDiffs(a.len, b.len, out);
}

test "diffBytes reconstructs" {
    const cases = [_][2][]const u8{
        .{ "abcdefg", "abcdefg" },
        .{ "abcdefg", "abXdefg" },
        .{ "", "abc" },
        .{ "abc", "" },
        .{ "aaabbbccc", "aaaccc" },
        .{ "the quick brown fox", "the quick brown foxes jumped" },
    };
    for (cases) |c| {
        var out: [max_out_diffs]Diff = undefined;
        const diffs = diffBytes(c[0], c[1], &out);
        var buf: [128]u8 = undefined;
        var n: usize = 0;
        var prev: usize = 0;
        for (diffs) |d| {
            @memcpy(buf[n..][0 .. d.start - prev], c[0][prev..d.start]);
            n += d.start - prev;
            @memcpy(buf[n..][0 .. d.repl_end - d.repl_start], c[1][d.repl_start..d.repl_end]);
            n += d.repl_end - d.repl_start;
            prev = d.end;
        }
        @memcpy(buf[n..][0 .. c[0].len - prev], c[0][prev..]);
        n += c[0].len - prev;
        try std.testing.expectEqualSlices(u8, c[1], buf[0..n]);
    }
}
