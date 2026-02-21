const std = @import("std");
const platform = @import("platform.zig");
const bus = @import("bus.zig");

const log = std.log.scoped(.cron);

pub const JobType = enum {
    shell,
    agent,

    pub fn asStr(self: JobType) []const u8 {
        return switch (self) {
            .shell => "shell",
            .agent => "agent",
        };
    }

    pub fn parse(raw: []const u8) JobType {
        if (std.ascii.eqlIgnoreCase(raw, "agent")) return .agent;
        return .shell;
    }
};

pub const SessionTarget = enum {
    isolated,
    main,

    pub fn asStr(self: SessionTarget) []const u8 {
        return switch (self) {
            .isolated => "isolated",
            .main => "main",
        };
    }

    pub fn parse(raw: []const u8) SessionTarget {
        if (std.ascii.eqlIgnoreCase(raw, "main")) return .main;
        return .isolated;
    }
};

pub const ScheduleKind = enum { cron, at, every };

pub const Schedule = union(ScheduleKind) {
    cron: struct { expr: []const u8, tz: ?[]const u8 },
    at: struct { timestamp_s: i64 },
    every: struct { every_ms: u64 },
};

pub const DeliveryMode = enum {
    none,
    always,
    on_error,
    on_success,

    pub fn asStr(self: DeliveryMode) []const u8 {
        return switch (self) {
            .none => "none",
            .always => "always",
            .on_error => "on_error",
            .on_success => "on_success",
        };
    }

    pub fn parse(raw: []const u8) DeliveryMode {
        if (std.ascii.eqlIgnoreCase(raw, "always")) return .always;
        if (std.ascii.eqlIgnoreCase(raw, "on_error")) return .on_error;
        if (std.ascii.eqlIgnoreCase(raw, "on_success")) return .on_success;
        return .none;
    }
};

pub const DeliveryConfig = struct {
    mode: DeliveryMode = .none,
    channel: ?[]const u8 = null,
    to: ?[]const u8 = null,
    best_effort: bool = true,
};

pub const CronRun = struct {
    id: u64,
    job_id: []const u8,
    started_at_s: i64,
    finished_at_s: i64,
    status: []const u8,
    output: ?[]const u8,
    duration_ms: ?i64,
};

pub const CronJobPatch = struct {
    expression: ?[]const u8 = null,
    command: ?[]const u8 = null,
    prompt: ?[]const u8 = null,
    name: ?[]const u8 = null,
    enabled: ?bool = null,
    model: ?[]const u8 = null,
    delete_after_run: ?bool = null,
};

/// A scheduled cron job.
pub const CronJob = struct {
    id: []const u8,
    expression: []const u8,
    command: []const u8,
    next_run_secs: i64 = 0,
    last_run_secs: ?i64 = null,
    last_status: ?[]const u8 = null,
    paused: bool = false,
    one_shot: bool = false,
    job_type: JobType = .shell,
    session_target: SessionTarget = .isolated,
    prompt: ?[]const u8 = null,
    name: ?[]const u8 = null,
    model: ?[]const u8 = null,
    enabled: bool = true,
    delete_after_run: bool = false,
    created_at_s: i64 = 0,
    last_output: ?[]const u8 = null,
    delivery: DeliveryConfig = .{},
};

fn freeJobOwnedStrings(allocator: std.mem.Allocator, job: CronJob) void {
    allocator.free(job.id);
    allocator.free(job.expression);
    allocator.free(job.command);
    if (job.prompt) |p| allocator.free(p);
    if (job.name) |n| allocator.free(n);
    if (job.model) |m| allocator.free(m);
    if (job.last_output) |o| allocator.free(o);
    if (job.delivery.channel) |c| allocator.free(c);
    if (job.delivery.to) |t| allocator.free(t);
}

fn makeAgentSessionKey(
    allocator: std.mem.Allocator,
    job: *const CronJob,
    channel: []const u8,
    chat_id: []const u8,
) ![]const u8 {
    if (job.session_target == .main and job.delivery.channel != null and job.delivery.to != null) {
        return std.fmt.allocPrint(allocator, "{s}:{s}", .{ channel, chat_id });
    }
    return std.fmt.allocPrint(allocator, "cron:{s}", .{job.id});
}

fn publishAgentWake(
    allocator: std.mem.Allocator,
    job: *const CronJob,
    prompt: []const u8,
    out_bus: *bus.Bus,
) !void {
    const channel = job.delivery.channel orelse "system";
    const chat_id = job.delivery.to orelse "default";
    const session_key = try makeAgentSessionKey(allocator, job, channel, chat_id);
    defer allocator.free(session_key);

    const inbound = try bus.makeInbound(
        allocator,
        channel,
        "scheduler",
        chat_id,
        prompt,
        session_key,
    );
    out_bus.publishInbound(inbound) catch |err| {
        inbound.deinit(allocator);
        return err;
    };
}

/// Duration unit for "once" delay parsing.
pub const DurationUnit = enum {
    seconds,
    minutes,
    hours,
    days,
    weeks,
};

/// Parse a human delay string like "30m", "2h", "1d" into seconds.
pub fn parseDuration(input: []const u8) !i64 {
    const trimmed = std.mem.trim(u8, input, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyDelay;

    // Check if last char is a unit letter
    const last = trimmed[trimmed.len - 1];
    var num_str: []const u8 = undefined;
    var multiplier: i64 = undefined;

    if (std.ascii.isAlphabetic(last)) {
        num_str = trimmed[0 .. trimmed.len - 1];
        multiplier = switch (last) {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            'w' => 604800,
            else => return error.UnknownDurationUnit,
        };
    } else {
        num_str = trimmed;
        multiplier = 60; // default to minutes
    }

    const n = std.fmt.parseInt(i64, std.mem.trim(u8, num_str, " "), 10) catch return error.InvalidDurationNumber;
    if (n <= 0) return error.InvalidDurationNumber;

    const secs = std.math.mul(i64, n, multiplier) catch return error.DurationTooLarge;
    return secs;
}

/// Normalize a cron expression (5 fields -> prepend "0" for seconds).
pub fn normalizeExpression(expression: []const u8) !CronNormalized {
    const trimmed = std.mem.trim(u8, expression, " \t\r\n");
    var field_count: usize = 0;
    var in_field = false;

    for (trimmed) |c| {
        if (c == ' ' or c == '\t') {
            if (in_field) {
                in_field = false;
            }
        } else {
            if (!in_field) {
                field_count += 1;
                in_field = true;
            }
        }
    }

    return switch (field_count) {
        5 => .{ .expression = trimmed, .needs_second_prefix = true },
        6, 7 => .{ .expression = trimmed, .needs_second_prefix = false },
        else => error.InvalidCronExpression,
    };
}

pub const CronNormalized = struct {
    expression: []const u8,
    needs_second_prefix: bool,
};

/// In-memory cron job store (no SQLite dependency for the minimal Zig port).
pub const CronScheduler = struct {
    jobs: std.ArrayListUnmanaged(CronJob),
    runs: std.ArrayListUnmanaged(CronRun) = .empty,
    next_run_id: u64 = 1,
    max_tasks: usize,
    enabled: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_tasks: usize, enabled: bool) CronScheduler {
        return .{
            .jobs = .empty,
            .max_tasks = max_tasks,
            .enabled = enabled,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CronScheduler) void {
        for (self.runs.items) |r| {
            self.allocator.free(r.job_id);
            self.allocator.free(r.status);
            if (r.output) |o| self.allocator.free(o);
        }
        self.runs.deinit(self.allocator);
        for (self.jobs.items) |job| {
            freeJobOwnedStrings(self.allocator, job);
        }
        self.jobs.deinit(self.allocator);
    }

    /// Add a recurring cron job.
    pub fn addJob(self: *CronScheduler, expression: []const u8, command: []const u8) !*CronJob {
        if (self.jobs.items.len >= self.max_tasks) return error.MaxTasksReached;

        // Validate expression
        _ = try normalizeExpression(expression);

        // Generate a simple numeric ID
        var id_buf: [32]u8 = undefined;
        const id = std.fmt.bufPrint(&id_buf, "job-{d}", .{self.jobs.items.len + 1}) catch "job-?";

        try self.jobs.append(self.allocator, .{
            .id = try self.allocator.dupe(u8, id),
            .expression = try self.allocator.dupe(u8, expression),
            .command = try self.allocator.dupe(u8, command),
            .next_run_secs = std.time.timestamp() + 60, // placeholder
            .created_at_s = std.time.timestamp(),
        });

        return &self.jobs.items[self.jobs.items.len - 1];
    }

    /// Add a one-shot delayed task.
    pub fn addOnce(self: *CronScheduler, delay: []const u8, command: []const u8) !*CronJob {
        if (self.jobs.items.len >= self.max_tasks) return error.MaxTasksReached;

        const delay_secs = try parseDuration(delay);
        const now = std.time.timestamp();

        var id_buf: [32]u8 = undefined;
        const id = std.fmt.bufPrint(&id_buf, "once-{d}", .{self.jobs.items.len + 1}) catch "once-?";

        var expr_buf: [64]u8 = undefined;
        const expr = std.fmt.bufPrint(&expr_buf, "@once:{s}", .{delay}) catch "@once";

        try self.jobs.append(self.allocator, .{
            .id = try self.allocator.dupe(u8, id),
            .expression = try self.allocator.dupe(u8, expr),
            .command = try self.allocator.dupe(u8, command),
            .next_run_secs = now + delay_secs,
            .one_shot = true,
            .created_at_s = now,
        });

        return &self.jobs.items[self.jobs.items.len - 1];
    }

    /// List all jobs.
    pub fn listJobs(self: *const CronScheduler) []const CronJob {
        return self.jobs.items;
    }

    /// Get a job by ID.
    pub fn getJob(self: *const CronScheduler, id: []const u8) ?*const CronJob {
        for (self.jobs.items) |*job| {
            if (std.mem.eql(u8, job.id, id)) return job;
        }
        return null;
    }

    /// Get a mutable pointer to a job by ID.
    pub fn getMutableJob(self: *CronScheduler, id: []const u8) ?*CronJob {
        for (self.jobs.items) |*job| {
            if (std.mem.eql(u8, job.id, id)) return job;
        }
        return null;
    }

    /// Update a job's fields from a patch.
    pub fn updateJob(self: *CronScheduler, allocator: std.mem.Allocator, id: []const u8, patch: CronJobPatch) bool {
        const job = self.getMutableJob(id) orelse return false;
        if (patch.expression) |expr| {
            allocator.free(job.expression);
            job.expression = allocator.dupe(u8, expr) catch return false;
        }
        if (patch.command) |cmd| {
            allocator.free(job.command);
            job.command = allocator.dupe(u8, cmd) catch return false;
        }
        if (patch.prompt) |p| {
            if (job.prompt) |old| allocator.free(old);
            job.prompt = allocator.dupe(u8, p) catch return false;
        }
        if (patch.name) |n| {
            if (job.name) |old| allocator.free(old);
            job.name = allocator.dupe(u8, n) catch return false;
        }
        if (patch.model) |m| {
            if (job.model) |old| allocator.free(old);
            job.model = allocator.dupe(u8, m) catch return false;
        }
        if (patch.enabled) |ena| {
            job.enabled = ena;
            job.paused = !ena;
        }
        if (patch.delete_after_run) |d| {
            job.delete_after_run = d;
            job.one_shot = d;
        }
        return true;
    }

    /// Record a completed run for a job.
    pub fn addRun(self: *CronScheduler, allocator: std.mem.Allocator, job_id: []const u8, started_at_s: i64, finished_at_s: i64, status: []const u8, output: ?[]const u8, max_history: usize) !void {
        const entry = CronRun{
            .id = self.next_run_id,
            .job_id = try allocator.dupe(u8, job_id),
            .started_at_s = started_at_s,
            .finished_at_s = finished_at_s,
            .status = try allocator.dupe(u8, status),
            .output = if (output) |o| try allocator.dupe(u8, o) else null,
            .duration_ms = (finished_at_s - started_at_s) * 1000,
        };
        self.next_run_id += 1;
        try self.runs.append(allocator, entry);
        // Prune to max_history per job_id
        if (max_history > 0) {
            var count: usize = 0;
            var i: usize = self.runs.items.len;
            while (i > 0) {
                i -= 1;
                if (std.mem.eql(u8, self.runs.items[i].job_id, job_id)) {
                    count += 1;
                    if (count > max_history) {
                        // Free strings of the pruned run
                        allocator.free(self.runs.items[i].job_id);
                        allocator.free(self.runs.items[i].status);
                        if (self.runs.items[i].output) |o| allocator.free(o);
                        _ = self.runs.orderedRemove(i);
                    }
                }
            }
        }
    }

    /// List recent runs for a given job_id, up to `limit` entries.
    pub fn listRuns(self: *const CronScheduler, job_id: []const u8, limit: usize) []const CronRun {
        // Return last `limit` runs for given job_id (from end of slice)
        var count: usize = 0;
        var start: usize = self.runs.items.len;
        var i: usize = self.runs.items.len;
        while (i > 0 and count < limit) {
            i -= 1;
            if (std.mem.eql(u8, self.runs.items[i].job_id, job_id)) {
                start = i;
                count += 1;
            }
        }
        if (count == 0) return &.{};
        return self.runs.items[start..];
    }

    /// Remove a job by ID, freeing its owned strings.
    pub fn removeJob(self: *CronScheduler, id: []const u8) bool {
        for (self.jobs.items, 0..) |job, i| {
            if (std.mem.eql(u8, job.id, id)) {
                freeJobOwnedStrings(self.allocator, job);
                _ = self.jobs.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    /// Pause a job.
    pub fn pauseJob(self: *CronScheduler, id: []const u8) bool {
        for (self.jobs.items) |*job| {
            if (std.mem.eql(u8, job.id, id)) {
                job.paused = true;
                return true;
            }
        }
        return false;
    }

    /// Resume a job.
    pub fn resumeJob(self: *CronScheduler, id: []const u8) bool {
        for (self.jobs.items) |*job| {
            if (std.mem.eql(u8, job.id, id)) {
                job.paused = false;
                return true;
            }
        }
        return false;
    }

    /// Get due (non-paused) jobs whose next_run <= now.
    pub fn dueJobs(self: *const CronScheduler, allocator: std.mem.Allocator, now_secs: i64) ![]const CronJob {
        var result: std.ArrayListUnmanaged(CronJob) = .empty;
        for (self.jobs.items) |job| {
            if (!job.paused and job.next_run_secs <= now_secs) {
                try result.append(allocator, job);
            }
        }
        return result.items;
    }

    /// Main scheduler loop: check all jobs, execute due ones, sleep until next.
    /// If `out_bus` is provided, job results are delivered to channels per delivery config.
    pub fn run(self: *CronScheduler, poll_secs: u64, out_bus: ?*bus.Bus) void {
        if (!self.enabled) return;

        const poll_ns: u64 = poll_secs * std.time.ns_per_s;

        while (true) {
            const now = std.time.timestamp();
            self.tick(now, out_bus);
            std.Thread.sleep(poll_ns);
        }
    }

    /// Execute one tick of the scheduler: run all due jobs, deliver results, handle one-shots.
    /// Separated from `run` for testability.
    pub fn tick(self: *CronScheduler, now: i64, out_bus: ?*bus.Bus) void {
        // Collect indices of one-shot jobs to remove after iteration
        var remove_indices: [64]usize = undefined;
        var remove_count: usize = 0;

        for (self.jobs.items, 0..) |*job, idx| {
            if (job.paused or job.next_run_secs > now) continue;

            switch (job.job_type) {
                .shell => {
                    // Execute shell command via child process
                    const result = std.process.Child.run(.{
                        .allocator = self.allocator,
                        .argv = &.{ platform.getShell(), platform.getShellFlag(), job.command },
                    }) catch |err| {
                        log.err("cron job '{s}' failed to start: {}", .{ job.id, err });
                        job.last_status = "error";
                        job.last_run_secs = now;
                        job.last_output = null;
                        // Deliver error notification
                        if (out_bus) |b| {
                            _ = deliverResult(self.allocator, job.delivery, "cron job failed to start", false, b) catch {};
                        }
                        continue;
                    };
                    defer self.allocator.free(result.stderr);

                    const success = switch (result.term) {
                        .Exited => |code| code == 0,
                        else => false,
                    };
                    job.last_run_secs = now;
                    job.last_status = if (success) "ok" else "error";

                    // Store and deliver stdout
                    if (job.last_output) |old| self.allocator.free(old);
                    job.last_output = if (result.stdout.len > 0) result.stdout else blk: {
                        self.allocator.free(result.stdout);
                        break :blk null;
                    };

                    if (out_bus) |b| {
                        const output = job.last_output orelse "";
                        _ = deliverResult(self.allocator, job.delivery, output, success, b) catch {};
                    }
                },
                .agent => {
                    const agent_prompt = job.prompt orelse job.command;
                    job.last_run_secs = now;
                    if (agent_prompt.len == 0) {
                        job.last_status = "error";
                        if (job.last_output) |old| self.allocator.free(old);
                        job.last_output = self.allocator.dupe(u8, "empty agent prompt") catch null;
                        continue;
                    }

                    if (job.last_output) |old| self.allocator.free(old);
                    if (out_bus) |b| {
                        publishAgentWake(self.allocator, job, agent_prompt, b) catch |err| {
                            log.err("cron agent wake publish failed for '{s}': {}", .{ job.id, err });
                            job.last_status = "error";
                            job.last_output = self.allocator.dupe(u8, "agent wake publish failed") catch null;
                            continue;
                        };
                        job.last_status = "queued";
                        job.last_output = self.allocator.dupe(u8, agent_prompt) catch null;
                    } else {
                        // Fallback path when no bus is available.
                        job.last_status = "ok";
                        job.last_output = self.allocator.dupe(u8, agent_prompt) catch null;
                    }
                },
            }

            if (job.one_shot or job.delete_after_run) {
                if (remove_count < remove_indices.len) {
                    remove_indices[remove_count] = idx;
                    remove_count += 1;
                } else {
                    // Fallback: just pause it
                    job.paused = true;
                }
            } else {
                // Reschedule: advance by 60s (simple approximation without full cron parser)
                job.next_run_secs = now + 60;
            }
        }

        // Remove one-shot jobs in reverse order to keep indices valid
        if (remove_count > 0) {
            var i: usize = remove_count;
            while (i > 0) {
                i -= 1;
                const rm_idx = remove_indices[i];
                const job = self.jobs.items[rm_idx];
                freeJobOwnedStrings(self.allocator, job);
                _ = self.jobs.orderedRemove(rm_idx);
            }
        }
    }
};

// ── Delivery ─────────────────────────────────────────────────────

/// Deliver a cron job result to a channel via the outbound bus.
/// Returns true if a message was published, false if delivery was skipped.
pub fn deliverResult(
    allocator: std.mem.Allocator,
    delivery: DeliveryConfig,
    output: []const u8,
    success: bool,
    out_bus: *bus.Bus,
) !bool {
    // Skip if mode is none
    if (delivery.mode == .none) return false;

    // Skip if no channel configured
    const channel = delivery.channel orelse return false;

    // Check mode-specific conditions
    switch (delivery.mode) {
        .none => return false,
        .on_success => if (!success) return false,
        .on_error => if (success) return false,
        .always => {},
    }

    // Skip empty output
    if (output.len == 0) return false;

    const chat_id = delivery.to orelse "default";
    const msg = try bus.makeOutbound(allocator, channel, chat_id, output);
    out_bus.publishOutbound(msg) catch |err| {
        // If best_effort, swallow the error after cleaning up
        if (delivery.best_effort) {
            msg.deinit(allocator);
            return false;
        }
        msg.deinit(allocator);
        return err;
    };
    return true;
}

// ── JSON Persistence ─────────────────────────────────────────────

/// Serializable representation of a cron job for JSON persistence.
const JsonCronJob = struct {
    id: []const u8,
    expression: []const u8,
    command: []const u8,
    next_run_secs: i64,
    last_run_secs: ?i64,
    last_status: ?[]const u8,
    paused: bool,
    one_shot: bool,
};

/// Get the default cron.json path: ~/.nullclaw/cron.json
fn cronJsonPath(allocator: std.mem.Allocator) ![]const u8 {
    const home = try platform.getHomeDir(allocator);
    defer allocator.free(home);
    return std.fs.path.join(allocator, &.{ home, ".nullclaw", "cron.json" });
}

/// Ensure the ~/.nullclaw directory exists.
fn ensureCronDir(allocator: std.mem.Allocator) !void {
    const home = try platform.getHomeDir(allocator);
    defer allocator.free(home);
    const dir = try std.fs.path.join(allocator, &.{ home, ".nullclaw" });
    defer allocator.free(dir);
    std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
}

/// Save scheduler jobs to ~/.nullclaw/cron.json.
pub fn saveJobs(scheduler: *const CronScheduler) !void {
    try ensureCronDir(scheduler.allocator);
    const path = try cronJsonPath(scheduler.allocator);
    defer scheduler.allocator.free(path);

    const file = try std.fs.createFileAbsolute(path, .{});
    defer file.close();

    var buf: [8192]u8 = undefined;
    var bw = file.writer(&buf);
    const w = &bw.interface;

    try w.writeAll("[\n");
    for (scheduler.jobs.items, 0..) |job, i| {
        try w.writeAll("  {");
        try w.print("\"id\":\"{s}\",", .{job.id});
        try w.print("\"expression\":\"{s}\",", .{job.expression});
        try w.print("\"command\":\"{s}\",", .{job.command});
        try w.print("\"job_type\":\"{s}\",", .{job.job_type.asStr()});
        try w.print("\"session_target\":\"{s}\",", .{job.session_target.asStr()});
        if (job.prompt) |p| {
            try w.print("\"prompt\":\"{s}\",", .{p});
        } else {
            try w.writeAll("\"prompt\":null,");
        }
        if (job.name) |n| {
            try w.print("\"name\":\"{s}\",", .{n});
        } else {
            try w.writeAll("\"name\":null,");
        }
        if (job.model) |m| {
            try w.print("\"model\":\"{s}\",", .{m});
        } else {
            try w.writeAll("\"model\":null,");
        }
        try w.print("\"enabled\":{s},", .{if (job.enabled) "true" else "false"});
        try w.print("\"delete_after_run\":{s},", .{if (job.delete_after_run) "true" else "false"});
        try w.print("\"created_at_s\":{d},", .{job.created_at_s});
        try w.print("\"next_run_secs\":{d},", .{job.next_run_secs});
        if (job.last_run_secs) |lrs| {
            try w.print("\"last_run_secs\":{d},", .{lrs});
        } else {
            try w.writeAll("\"last_run_secs\":null,");
        }
        if (job.last_status) |ls| {
            try w.print("\"last_status\":\"{s}\",", .{ls});
        } else {
            try w.writeAll("\"last_status\":null,");
        }
        try w.print("\"paused\":{s},", .{if (job.paused) "true" else "false"});
        try w.print("\"one_shot\":{s},", .{if (job.one_shot) "true" else "false"});
        if (job.last_output) |o| {
            try w.print("\"last_output\":\"{s}\",", .{o});
        } else {
            try w.writeAll("\"last_output\":null,");
        }
        try w.writeAll("\"delivery\":{");
        try w.print("\"mode\":\"{s}\",", .{job.delivery.mode.asStr()});
        if (job.delivery.channel) |c| {
            try w.print("\"channel\":\"{s}\",", .{c});
        } else {
            try w.writeAll("\"channel\":null,");
        }
        if (job.delivery.to) |t| {
            try w.print("\"to\":\"{s}\",", .{t});
        } else {
            try w.writeAll("\"to\":null,");
        }
        try w.print("\"best_effort\":{s}", .{if (job.delivery.best_effort) "true" else "false"});
        try w.writeAll("}");
        try w.writeAll("}");
        if (i + 1 < scheduler.jobs.items.len) {
            try w.writeAll(",");
        }
        try w.writeAll("\n");
    }
    try w.writeAll("]\n");
    try w.flush();
}

/// Load jobs from ~/.nullclaw/cron.json into the scheduler.
pub fn loadJobs(scheduler: *CronScheduler) !void {
    const path = try cronJsonPath(scheduler.allocator);
    defer scheduler.allocator.free(path);

    const content = std.fs.cwd().readFileAlloc(scheduler.allocator, path, 1024 * 1024) catch return;
    defer scheduler.allocator.free(content);

    const parsed = std.json.parseFromSlice(std.json.Value, scheduler.allocator, content, .{}) catch return;
    defer parsed.deinit();

    if (parsed.value != .array) return;

    for (parsed.value.array.items) |item| {
        if (item != .object) continue;
        const obj = item.object;

        const id = blk: {
            if (obj.get("id")) |v| {
                if (v == .string) break :blk v.string;
            }
            continue;
        };
        const expression = blk: {
            if (obj.get("expression")) |v| {
                if (v == .string) break :blk v.string;
            }
            continue;
        };
        const command = blk: {
            if (obj.get("command")) |v| {
                if (v == .string) break :blk v.string;
            }
            continue;
        };
        const job_type = blk: {
            if (obj.get("job_type")) |v| {
                if (v == .string) break :blk JobType.parse(v.string);
            }
            break :blk JobType.shell;
        };
        const session_target = blk: {
            if (obj.get("session_target")) |v| {
                if (v == .string) break :blk SessionTarget.parse(v.string);
            }
            break :blk SessionTarget.isolated;
        };
        const prompt: ?[]const u8 = blk: {
            if (obj.get("prompt")) |v| {
                if (v == .string) break :blk v.string;
            }
            break :blk null;
        };
        const name: ?[]const u8 = blk: {
            if (obj.get("name")) |v| {
                if (v == .string) break :blk v.string;
            }
            break :blk null;
        };
        const model: ?[]const u8 = blk: {
            if (obj.get("model")) |v| {
                if (v == .string) break :blk v.string;
            }
            break :blk null;
        };
        const enabled = blk: {
            if (obj.get("enabled")) |v| {
                if (v == .bool) break :blk v.bool;
            }
            break :blk true;
        };
        const delete_after_run = blk: {
            if (obj.get("delete_after_run")) |v| {
                if (v == .bool) break :blk v.bool;
            }
            break :blk false;
        };
        const created_at_s: i64 = blk: {
            if (obj.get("created_at_s")) |v| {
                if (v == .integer) break :blk v.integer;
            }
            break :blk std.time.timestamp();
        };
        const last_output: ?[]const u8 = blk: {
            if (obj.get("last_output")) |v| {
                if (v == .string) break :blk v.string;
            }
            break :blk null;
        };

        const next_run_secs: i64 = blk: {
            if (obj.get("next_run_secs")) |v| {
                if (v == .integer) break :blk v.integer;
            }
            break :blk std.time.timestamp() + 60;
        };

        const paused = blk: {
            if (obj.get("paused")) |v| {
                if (v == .bool) break :blk v.bool;
            }
            break :blk false;
        };

        const one_shot = blk: {
            if (obj.get("one_shot")) |v| {
                if (v == .bool) break :blk v.bool;
            }
            break :blk false;
        };

        var delivery = DeliveryConfig{};
        if (obj.get("delivery")) |dv| {
            if (dv == .object) {
                const dobj = dv.object;
                if (dobj.get("mode")) |mv| {
                    if (mv == .string) delivery.mode = DeliveryMode.parse(mv.string);
                }
                if (dobj.get("channel")) |cv| {
                    if (cv == .string) delivery.channel = cv.string;
                }
                if (dobj.get("to")) |tv| {
                    if (tv == .string) delivery.to = tv.string;
                }
                if (dobj.get("best_effort")) |bv| {
                    if (bv == .bool) delivery.best_effort = bv.bool;
                }
            }
        }

        try scheduler.jobs.append(scheduler.allocator, .{
            .id = try scheduler.allocator.dupe(u8, id),
            .expression = try scheduler.allocator.dupe(u8, expression),
            .command = try scheduler.allocator.dupe(u8, command),
            .job_type = job_type,
            .session_target = session_target,
            .prompt = if (prompt) |p| try scheduler.allocator.dupe(u8, p) else null,
            .name = if (name) |n| try scheduler.allocator.dupe(u8, n) else null,
            .model = if (model) |m| try scheduler.allocator.dupe(u8, m) else null,
            .enabled = enabled,
            .delete_after_run = delete_after_run,
            .created_at_s = created_at_s,
            .next_run_secs = next_run_secs,
            .paused = paused,
            .one_shot = one_shot,
            .last_output = if (last_output) |o| try scheduler.allocator.dupe(u8, o) else null,
            .delivery = .{
                .mode = delivery.mode,
                .channel = if (delivery.channel) |c| try scheduler.allocator.dupe(u8, c) else null,
                .to = if (delivery.to) |t| try scheduler.allocator.dupe(u8, t) else null,
                .best_effort = delivery.best_effort,
            },
        });
    }
}

// ── CLI entry points (called from main.zig) ──────────────────────

/// CLI: list all cron jobs.
pub fn cliListJobs(allocator: std.mem.Allocator) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    const jobs = scheduler.listJobs();
    if (jobs.len == 0) {
        log.info("No scheduled tasks yet.", .{});
        log.info("Usage:", .{});
        log.info("  nullclaw cron add '*/10 * * * *' 'echo hello'", .{});
        log.info("  nullclaw cron once 30m 'echo reminder'", .{});
        return;
    }

    log.info("Scheduled jobs ({d}):", .{jobs.len});
    for (jobs) |job| {
        const flags: []const u8 = blk: {
            if (job.paused and job.one_shot) break :blk " [paused, one-shot]";
            if (job.paused) break :blk " [paused]";
            if (job.one_shot) break :blk " [one-shot]";
            break :blk "";
        };
        const status = job.last_status orelse "n/a";
        log.info("- {s} | {s} | next={d} | status={s}{s} cmd: {s}", .{
            job.id,
            job.expression,
            job.next_run_secs,
            status,
            flags,
            job.command,
        });
    }
}

/// CLI: add a recurring cron job.
pub fn cliAddJob(allocator: std.mem.Allocator, expression: []const u8, command: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    const job = try scheduler.addJob(expression, command);
    try saveJobs(&scheduler);

    log.info("Added cron job {s}", .{job.id});
    log.info("  Expr: {s}", .{job.expression});
    log.info("  Next: {d}", .{job.next_run_secs});
    log.info("  Cmd : {s}", .{job.command});
}

/// CLI: add a one-shot delayed task.
pub fn cliAddOnce(allocator: std.mem.Allocator, delay: []const u8, command: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    const job = try scheduler.addOnce(delay, command);
    try saveJobs(&scheduler);

    log.info("Added one-shot task {s}", .{job.id});
    log.info("  Runs at: {d}", .{job.next_run_secs});
    log.info("  Cmd    : {s}", .{job.command});
}

/// CLI: remove a cron job by ID.
pub fn cliRemoveJob(allocator: std.mem.Allocator, id: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    if (scheduler.removeJob(id)) {
        try saveJobs(&scheduler);
        log.info("Removed cron job {s}", .{id});
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

/// CLI: pause a cron job by ID.
pub fn cliPauseJob(allocator: std.mem.Allocator, id: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    if (scheduler.pauseJob(id)) {
        try saveJobs(&scheduler);
        log.info("Paused job {s}", .{id});
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

/// CLI: resume a paused cron job by ID.
pub fn cliResumeJob(allocator: std.mem.Allocator, id: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    if (scheduler.resumeJob(id)) {
        try saveJobs(&scheduler);
        log.info("Resumed job {s}", .{id});
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

pub fn cliRunJob(allocator: std.mem.Allocator, id: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    if (scheduler.getJob(id)) |job| {
        log.info("Running job '{s}': {s}", .{ id, job.command });
        const result = std.process.Child.run(.{
            .allocator = allocator,
            .argv = &.{ platform.getShell(), platform.getShellFlag(), job.command },
        }) catch |err| {
            log.err("Job '{s}' failed: {s}", .{ id, @errorName(err) });
            return;
        };
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);
        if (result.stdout.len > 0) log.info("{s}", .{result.stdout});
        const exit_code: u8 = switch (result.term) {
            .Exited => |code| code,
            else => 1,
        };
        log.info("Job '{s}' completed (exit {d}).", .{ id, exit_code });
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

/// CLI: update a cron job's expression, command, or enabled state.
pub fn cliUpdateJob(
    allocator: std.mem.Allocator,
    id: []const u8,
    expression: ?[]const u8,
    command: ?[]const u8,
    enabled: ?bool,
) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    const patch = CronJobPatch{
        .expression = expression,
        .command = command,
        .enabled = enabled,
    };
    if (scheduler.updateJob(allocator, id, patch)) {
        try saveJobs(&scheduler);
        log.info("Updated job {s}", .{id});
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

/// CLI: list run history for a cron job.
pub fn cliListRuns(allocator: std.mem.Allocator, id: []const u8) !void {
    var scheduler = CronScheduler.init(allocator, 1024, true);
    defer scheduler.deinit();
    try loadJobs(&scheduler);

    if (scheduler.getJob(id)) |job| {
        log.info("Run history for job {s} ({s}):", .{ id, job.command });
        const status = job.last_status orelse "never run";
        log.info("  Last status: {s}", .{status});
        log.info("  Next run:    {d}", .{job.next_run_secs});
    } else {
        log.warn("Cron job '{s}' not found", .{id});
    }
}

// ── Backwards-compatible type alias ──────────────────────────────────

pub const Task = CronJob;

// ── Tests ────────────────────────────────────────────────────────────

test "parseDuration minutes" {
    try std.testing.expectEqual(@as(i64, 1800), try parseDuration("30m"));
}

test "parseDuration hours" {
    try std.testing.expectEqual(@as(i64, 7200), try parseDuration("2h"));
}

test "parseDuration days" {
    try std.testing.expectEqual(@as(i64, 86400), try parseDuration("1d"));
}

test "parseDuration weeks" {
    try std.testing.expectEqual(@as(i64, 604800), try parseDuration("1w"));
}

test "parseDuration seconds" {
    try std.testing.expectEqual(@as(i64, 30), try parseDuration("30s"));
}

test "parseDuration default unit is minutes" {
    try std.testing.expectEqual(@as(i64, 300), try parseDuration("5"));
}

test "parseDuration empty returns error" {
    try std.testing.expectError(error.EmptyDelay, parseDuration(""));
}

test "parseDuration unknown unit" {
    try std.testing.expectError(error.UnknownDurationUnit, parseDuration("5x"));
}

test "normalizeExpression 5 fields" {
    const result = try normalizeExpression("*/5 * * * *");
    try std.testing.expect(result.needs_second_prefix);
}

test "normalizeExpression 6 fields" {
    const result = try normalizeExpression("0 */5 * * * *");
    try std.testing.expect(!result.needs_second_prefix);
}

test "normalizeExpression 4 fields invalid" {
    try std.testing.expectError(error.InvalidCronExpression, normalizeExpression("* * * *"));
}

test "CronScheduler add and list" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addJob("*/10 * * * *", "echo roundtrip");
    try std.testing.expectEqualStrings("*/10 * * * *", job.expression);
    try std.testing.expectEqualStrings("echo roundtrip", job.command);
    try std.testing.expect(!job.one_shot);
    try std.testing.expect(!job.paused);

    const listed = scheduler.listJobs();
    try std.testing.expectEqual(@as(usize, 1), listed.len);
}

test "CronScheduler addOnce creates one-shot" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addOnce("30m", "echo once");
    try std.testing.expect(job.one_shot);
}

test "CronScheduler remove" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addJob("*/10 * * * *", "echo test");
    try std.testing.expect(scheduler.removeJob(job.id));
    try std.testing.expectEqual(@as(usize, 0), scheduler.listJobs().len);
}

test "CronScheduler pause and resume" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addJob("*/5 * * * *", "echo pause");
    try std.testing.expect(scheduler.pauseJob(job.id));
    try std.testing.expect(scheduler.getJob(job.id).?.paused);
    try std.testing.expect(scheduler.resumeJob(job.id));
    try std.testing.expect(!scheduler.getJob(job.id).?.paused);
}

test "CronScheduler max tasks enforced" {
    var scheduler = CronScheduler.init(std.testing.allocator, 1, true);
    defer scheduler.deinit();

    _ = try scheduler.addJob("*/10 * * * *", "echo first");
    try std.testing.expectError(error.MaxTasksReached, scheduler.addJob("*/11 * * * *", "echo second"));
}

test "CronScheduler getJob found and missing" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addJob("*/5 * * * *", "echo found");
    try std.testing.expect(scheduler.getJob(job.id) != null);
    try std.testing.expect(scheduler.getJob("nonexistent") == null);
}

test "save and load roundtrip" {
    var scheduler = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler.deinit();

    _ = try scheduler.addJob("*/10 * * * *", "echo roundtrip");
    _ = try scheduler.addOnce("5m", "echo oneshot");

    // Save to disk
    try saveJobs(&scheduler);

    // Load into a new scheduler
    var scheduler2 = CronScheduler.init(std.testing.allocator, 10, true);
    defer scheduler2.deinit();
    try loadJobs(&scheduler2);

    try std.testing.expectEqual(@as(usize, 2), scheduler2.listJobs().len);

    const loaded = scheduler2.listJobs();
    try std.testing.expectEqualStrings("*/10 * * * *", loaded[0].expression);
    try std.testing.expectEqualStrings("echo roundtrip", loaded[0].command);
    try std.testing.expect(loaded[1].one_shot);
}

test "JobType parse and asStr" {
    try std.testing.expectEqual(JobType.shell, JobType.parse("shell"));
    try std.testing.expectEqual(JobType.agent, JobType.parse("agent"));
    try std.testing.expectEqual(JobType.agent, JobType.parse("AGENT"));
    try std.testing.expectEqualStrings("shell", JobType.shell.asStr());
    try std.testing.expectEqualStrings("agent", JobType.agent.asStr());
}

test "SessionTarget parse and asStr" {
    try std.testing.expectEqual(SessionTarget.isolated, SessionTarget.parse("isolated"));
    try std.testing.expectEqual(SessionTarget.main, SessionTarget.parse("main"));
    try std.testing.expectEqual(SessionTarget.main, SessionTarget.parse("MAIN"));
    try std.testing.expectEqualStrings("isolated", SessionTarget.isolated.asStr());
    try std.testing.expectEqualStrings("main", SessionTarget.main.asStr());
}

test "CronJob has new fields" {
    const job = CronJob{
        .id = "test",
        .expression = "* * * * *",
        .command = "echo hi",
        .job_type = .agent,
        .session_target = .main,
        .enabled = true,
        .delete_after_run = false,
        .created_at_s = 1000000,
    };
    try std.testing.expectEqual(JobType.agent, job.job_type);
    try std.testing.expectEqual(SessionTarget.main, job.session_target);
    try std.testing.expect(job.enabled);
    try std.testing.expectEqual(@as(i64, 1000000), job.created_at_s);
}

test "getMutableJob returns mutable pointer" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();
    _ = try scheduler.addJob("* * * * *", "echo test");
    const jobs = scheduler.listJobs();
    const id = jobs[0].id;
    const job = scheduler.getMutableJob(id);
    try std.testing.expect(job != null);
    try std.testing.expectEqualStrings(id, job.?.id);
}

test "updateJob modifies job fields" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();
    _ = try scheduler.addJob("* * * * *", "echo original");
    const jobs = scheduler.listJobs();
    const id = jobs[0].id;
    const patch = CronJobPatch{ .command = "echo updated", .enabled = false };
    try std.testing.expect(scheduler.updateJob(allocator, id, patch));
    const updated = scheduler.getJob(id).?;
    try std.testing.expectEqualStrings("echo updated", updated.command);
    try std.testing.expect(!updated.enabled);
    try std.testing.expect(updated.paused);
}

test "getMutableJob returns null for unknown id" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();
    try std.testing.expect(scheduler.getMutableJob("nonexistent") == null);
}

test "addRun and listRuns" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();
    _ = try scheduler.addJob("* * * * *", "echo test");
    const jobs = scheduler.listJobs();
    const id = jobs[0].id;
    try scheduler.addRun(allocator, id, 1000, 1001, "success", "output", 10);
    try scheduler.addRun(allocator, id, 1001, 1002, "error", null, 10);
    const runs = scheduler.listRuns(id, 10);
    try std.testing.expect(runs.len > 0);
}

test "addRun prunes history" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();
    _ = try scheduler.addJob("* * * * *", "echo test");
    const jobs = scheduler.listJobs();
    const id = jobs[0].id;
    // Add 5 runs with max_history=3
    var i: i64 = 0;
    while (i < 5) : (i += 1) {
        try scheduler.addRun(allocator, id, i, i + 1, "success", null, 3);
    }
    const runs = scheduler.listRuns(id, 100);
    try std.testing.expect(runs.len <= 3);
}

// ── Delivery + Bus integration tests ────────────────────────────

test "deliverResult creates correct OutboundMessage" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .always,
        .channel = "telegram",
        .to = "chat123",
    };

    const delivered = try deliverResult(allocator, delivery, "job output here", true, &test_bus);
    try std.testing.expect(delivered);

    // Consume and verify the message
    var msg = test_bus.consumeOutbound().?;
    defer msg.deinit(allocator);
    try std.testing.expectEqualStrings("telegram", msg.channel);
    try std.testing.expectEqualStrings("chat123", msg.chat_id);
    try std.testing.expectEqualStrings("job output here", msg.content);
}

test "deliverResult with mode none does nothing" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .none,
        .channel = "telegram",
        .to = "chat1",
    };

    const delivered = try deliverResult(allocator, delivery, "should not appear", true, &test_bus);
    try std.testing.expect(!delivered);
    try std.testing.expectEqual(@as(usize, 0), test_bus.outboundDepth());
}

test "deliverResult with no channel does nothing" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .always,
        .channel = null,
        .to = "chat1",
    };

    const delivered = try deliverResult(allocator, delivery, "should not appear", true, &test_bus);
    try std.testing.expect(!delivered);
    try std.testing.expectEqual(@as(usize, 0), test_bus.outboundDepth());
}

test "deliverResult on_success skips on failure" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .on_success,
        .channel = "telegram",
        .to = "chat1",
    };

    const delivered = try deliverResult(allocator, delivery, "error output", false, &test_bus);
    try std.testing.expect(!delivered);
    try std.testing.expectEqual(@as(usize, 0), test_bus.outboundDepth());
}

test "deliverResult on_error skips on success" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .on_error,
        .channel = "telegram",
        .to = "chat1",
    };

    const delivered = try deliverResult(allocator, delivery, "ok output", true, &test_bus);
    try std.testing.expect(!delivered);
    try std.testing.expectEqual(@as(usize, 0), test_bus.outboundDepth());
}

test "deliverResult on_error delivers on failure" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .on_error,
        .channel = "discord",
        .to = "room42",
    };

    const delivered = try deliverResult(allocator, delivery, "crash log", false, &test_bus);
    try std.testing.expect(delivered);

    var msg = test_bus.consumeOutbound().?;
    defer msg.deinit(allocator);
    try std.testing.expectEqualStrings("discord", msg.channel);
    try std.testing.expectEqualStrings("room42", msg.chat_id);
    try std.testing.expectEqualStrings("crash log", msg.content);
}

test "deliverResult uses default chat_id when to is null" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .always,
        .channel = "webhook",
        .to = null,
    };

    const delivered = try deliverResult(allocator, delivery, "hello", true, &test_bus);
    try std.testing.expect(delivered);

    var msg = test_bus.consumeOutbound().?;
    defer msg.deinit(allocator);
    try std.testing.expectEqualStrings("default", msg.chat_id);
}

test "deliverResult skips empty output" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const delivery = DeliveryConfig{
        .mode = .always,
        .channel = "telegram",
        .to = "chat1",
    };

    const delivered = try deliverResult(allocator, delivery, "", true, &test_bus);
    try std.testing.expect(!delivered);
    try std.testing.expectEqual(@as(usize, 0), test_bus.outboundDepth());
}

test "deliverResult best_effort swallows closed bus error" {
    const allocator = std.testing.allocator;
    var test_bus = bus.Bus.init();
    test_bus.close(); // close before delivery

    const delivery = DeliveryConfig{
        .mode = .always,
        .channel = "telegram",
        .to = "chat1",
        .best_effort = true,
    };

    // Should not return error because best_effort is true
    const delivered = try deliverResult(allocator, delivery, "msg", true, &test_bus);
    try std.testing.expect(!delivered);
}

test "one-shot job deleted after tick execution" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();

    const job = try scheduler.addOnce("1s", "echo oneshot");
    // Verify job was created
    try std.testing.expect(job.one_shot);
    try std.testing.expectEqual(@as(usize, 1), scheduler.listJobs().len);

    // Force the job to be due now
    scheduler.jobs.items[0].next_run_secs = 0;

    // Tick without bus — the shell command "echo oneshot" will actually run
    scheduler.tick(std.time.timestamp(), null);

    // One-shot job should have been removed
    try std.testing.expectEqual(@as(usize, 0), scheduler.listJobs().len);
}

test "shell job delivers stdout via bus" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();

    var test_bus = bus.Bus.init();
    defer test_bus.close();

    const job = try scheduler.addJob("* * * * *", "echo hello_cron");
    _ = job;

    // Configure delivery
    scheduler.jobs.items[0].delivery = .{
        .mode = .always,
        .channel = try allocator.dupe(u8, "telegram"),
        .to = try allocator.dupe(u8, "chat99"),
    };
    scheduler.jobs.items[0].next_run_secs = 0;

    scheduler.tick(std.time.timestamp(), &test_bus);

    // Verify delivery happened
    try std.testing.expect(test_bus.outboundDepth() > 0);
    var msg = test_bus.consumeOutbound().?;
    defer msg.deinit(allocator);
    try std.testing.expectEqualStrings("telegram", msg.channel);
    try std.testing.expectEqualStrings("chat99", msg.chat_id);
    // The content should contain "hello_cron" from the echo command
    try std.testing.expect(std.mem.indexOf(u8, msg.content, "hello_cron") != null);
}

test "agent job publishes wake message to inbound bus" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();

    var test_bus = bus.Bus.init();
    defer test_bus.close();

    // Create an agent-type job with a prompt
    try scheduler.jobs.append(allocator, .{
        .id = try allocator.dupe(u8, "agent-1"),
        .expression = try allocator.dupe(u8, "* * * * *"),
        .command = try allocator.dupe(u8, "summarize"),
        .job_type = .agent,
        .session_target = .main,
        .prompt = try allocator.dupe(u8, "Summarize today's news"),
        .next_run_secs = 0,
        .delivery = .{
            .mode = .always,
            .channel = try allocator.dupe(u8, "discord"),
            .to = try allocator.dupe(u8, "general"),
        },
    });

    scheduler.tick(std.time.timestamp(), &test_bus);

    // Verify wake event
    try std.testing.expect(test_bus.inboundDepth() > 0);
    var msg = test_bus.consumeInbound().?;
    defer msg.deinit(allocator);
    try std.testing.expectEqualStrings("discord", msg.channel);
    try std.testing.expectEqualStrings("scheduler", msg.sender_id);
    try std.testing.expectEqualStrings("general", msg.chat_id);
    try std.testing.expectEqualStrings("discord:general", msg.session_key);
    try std.testing.expectEqualStrings("Summarize today's news", msg.content);
}

test "DeliveryMode parse and asStr" {
    try std.testing.expectEqual(DeliveryMode.none, DeliveryMode.parse("none"));
    try std.testing.expectEqual(DeliveryMode.always, DeliveryMode.parse("always"));
    try std.testing.expectEqual(DeliveryMode.on_error, DeliveryMode.parse("on_error"));
    try std.testing.expectEqual(DeliveryMode.on_success, DeliveryMode.parse("on_success"));
    try std.testing.expectEqual(DeliveryMode.none, DeliveryMode.parse("unknown"));
    try std.testing.expectEqual(DeliveryMode.always, DeliveryMode.parse("ALWAYS"));

    try std.testing.expectEqualStrings("none", DeliveryMode.none.asStr());
    try std.testing.expectEqualStrings("always", DeliveryMode.always.asStr());
    try std.testing.expectEqualStrings("on_error", DeliveryMode.on_error.asStr());
    try std.testing.expectEqualStrings("on_success", DeliveryMode.on_success.asStr());
}

test "tick without bus still executes jobs" {
    const allocator = std.testing.allocator;
    var scheduler = CronScheduler.init(allocator, 10, true);
    defer scheduler.deinit();

    _ = try scheduler.addJob("* * * * *", "echo silent");
    scheduler.jobs.items[0].next_run_secs = 0;

    // Tick with null bus — should not crash
    scheduler.tick(std.time.timestamp(), null);

    // Job should have been executed and rescheduled
    try std.testing.expectEqualStrings("ok", scheduler.jobs.items[0].last_status.?);
    try std.testing.expect(scheduler.jobs.items[0].next_run_secs > 0);
}

test "cron module compiles" {}
