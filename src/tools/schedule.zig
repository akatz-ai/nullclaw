const std = @import("std");
const root = @import("root.zig");
const Tool = root.Tool;
const ToolResult = root.ToolResult;
const JsonObjectMap = root.JsonObjectMap;
const cron = @import("../cron.zig");
const CronScheduler = cron.CronScheduler;
const loadScheduler = @import("cron_add.zig").loadScheduler;

/// Schedule tool — lets the agent manage recurring and one-shot scheduled tasks.
/// Delegates to the CronScheduler from the cron module for persistent job management.
pub const ScheduleTool = struct {
    const vtable = Tool.VTable{
        .execute = &vtableExecute,
        .name = &vtableName,
        .description = &vtableDesc,
        .parameters_json = &vtableParams,
    };

    pub fn tool(self: *ScheduleTool) Tool {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn vtableExecute(ptr: *anyopaque, allocator: std.mem.Allocator, args: JsonObjectMap) anyerror!ToolResult {
        const self: *ScheduleTool = @ptrCast(@alignCast(ptr));
        return self.execute(allocator, args);
    }

    fn vtableName(_: *anyopaque) []const u8 {
        return "schedule";
    }

    fn vtableDesc(_: *anyopaque) []const u8 {
        return "Manage scheduled tasks. Supports shell commands and agent wake prompts. Actions: create/add/once/list/get/cancel/remove/pause/resume";
    }

    fn vtableParams(_: *anyopaque) []const u8 {
        return 
        \\{"type":"object","properties":{"action":{"type":"string","enum":["create","add","once","list","get","cancel","remove","pause","resume"],"description":"Action to perform"},"expression":{"type":"string","description":"Cron expression for recurring tasks"},"delay":{"type":"string","description":"Delay for one-shot tasks (e.g. '30m', '2h')"},"job_type":{"type":"string","enum":["shell","agent"],"default":"shell","description":"Job kind: shell command or agent wake prompt"},"command":{"type":"string","description":"Shell command to execute (required for shell jobs)"},"prompt":{"type":"string","description":"Agent prompt to process when job_type is 'agent'"},"delivery":{"type":"object","description":"Optional delivery routing for scheduled output","properties":{"mode":{"type":"string","enum":["none","always","on_error","on_success"],"default":"none"},"channel":{"type":"string","description":"Target channel name (e.g. telegram)"},"to":{"type":"string","description":"Target chat/user/room id"}}},"id":{"type":"string","description":"Task ID"}},"required":["action"]}
        ;
    }

    const ParseJobTypeError = error{InvalidJobType};
    const ParseDeliveryError = error{
        DeliveryMustBeObject,
        DeliveryModeMustBeString,
        InvalidDeliveryMode,
        DeliveryChannelMustBeString,
        DeliveryToMustBeString,
    };

    fn parseJobType(args: JsonObjectMap) ParseJobTypeError!cron.JobType {
        const raw = root.getString(args, "job_type") orelse return .shell;
        if (std.ascii.eqlIgnoreCase(raw, "shell")) return .shell;
        if (std.ascii.eqlIgnoreCase(raw, "agent")) return .agent;
        return error.InvalidJobType;
    }

    fn isValidDeliveryMode(raw: []const u8) bool {
        return std.ascii.eqlIgnoreCase(raw, "none") or
            std.ascii.eqlIgnoreCase(raw, "always") or
            std.ascii.eqlIgnoreCase(raw, "on_error") or
            std.ascii.eqlIgnoreCase(raw, "on_success");
    }

    fn parseDelivery(args: JsonObjectMap) ParseDeliveryError!cron.DeliveryConfig {
        var delivery: cron.DeliveryConfig = .{};
        const value = root.getValue(args, "delivery") orelse return delivery;
        if (value != .object) return error.DeliveryMustBeObject;

        const obj = value.object;
        if (obj.get("mode")) |mv| {
            const mode_str = switch (mv) {
                .string => |s| s,
                else => return error.DeliveryModeMustBeString,
            };
            if (!isValidDeliveryMode(mode_str)) return error.InvalidDeliveryMode;
            delivery.mode = cron.DeliveryMode.parse(mode_str);
        }
        if (obj.get("channel")) |cv| {
            delivery.channel = switch (cv) {
                .string => |s| s,
                else => return error.DeliveryChannelMustBeString,
            };
        }
        if (obj.get("to")) |tv| {
            delivery.to = switch (tv) {
                .string => |s| s,
                else => return error.DeliveryToMustBeString,
            };
        }
        if (obj.get("best_effort")) |bv| {
            if (bv == .bool) delivery.best_effort = bv.bool;
        }
        return delivery;
    }

    fn setJobDelivery(allocator: std.mem.Allocator, job: *cron.CronJob, delivery: cron.DeliveryConfig) !void {
        const channel_copy = if (delivery.channel) |c| try allocator.dupe(u8, c) else null;
        errdefer if (channel_copy) |c| allocator.free(c);
        const to_copy = if (delivery.to) |t| try allocator.dupe(u8, t) else null;

        job.delivery = .{
            .mode = delivery.mode,
            .channel = channel_copy,
            .to = to_copy,
            .best_effort = delivery.best_effort,
        };
    }

    fn execute(_: *ScheduleTool, allocator: std.mem.Allocator, args: JsonObjectMap) !ToolResult {
        const action = root.getString(args, "action") orelse
            return ToolResult.fail("Missing 'action' parameter");

        if (std.mem.eql(u8, action, "list")) {
            var scheduler = loadScheduler(allocator) catch {
                return ToolResult.ok("No scheduled jobs.");
            };
            defer scheduler.deinit();

            const jobs = scheduler.listJobs();
            if (jobs.len == 0) {
                return ToolResult.ok("No scheduled jobs.");
            }

            // Format job list
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            const w = buf.writer(allocator);
            try w.print("Scheduled jobs ({d}):\n", .{jobs.len});
            for (jobs) |job| {
                const flags: []const u8 = blk: {
                    if (job.paused and job.one_shot) break :blk " [paused, one-shot]";
                    if (job.paused) break :blk " [paused]";
                    if (job.one_shot) break :blk " [one-shot]";
                    break :blk "";
                };
                const status = job.last_status orelse "pending";
                const primary = if (job.job_type == .agent) (job.prompt orelse job.command) else job.command;
                const primary_label = if (job.job_type == .agent) "prompt" else "cmd";
                try w.print("- {s} | {s} | type={s} | status={s}{s} | {s}: {s}\n", .{
                    job.id,
                    job.expression,
                    job.job_type.asStr(),
                    status,
                    flags,
                    primary_label,
                    primary,
                });
            }
            return ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
        }

        if (std.mem.eql(u8, action, "get")) {
            const id = root.getString(args, "id") orelse
                return ToolResult.fail("Missing 'id' parameter for get action");

            var scheduler = loadScheduler(allocator) catch {
                const msg = try std.fmt.allocPrint(allocator, "Job '{s}' not found", .{id});
                return ToolResult{ .success = false, .output = "", .error_msg = msg };
            };
            defer scheduler.deinit();

            if (scheduler.getJob(id)) |job| {
                const flags: []const u8 = blk: {
                    if (job.paused and job.one_shot) break :blk " [paused, one-shot]";
                    if (job.paused) break :blk " [paused]";
                    if (job.one_shot) break :blk " [one-shot]";
                    break :blk "";
                };
                const status = job.last_status orelse "pending";
                const primary = if (job.job_type == .agent) (job.prompt orelse job.command) else job.command;
                const primary_label = if (job.job_type == .agent) "prompt" else "cmd";
                const msg = try std.fmt.allocPrint(allocator, "Job {s} | {s} | type={s} | next={d} | status={s}{s}\n  {s}: {s}", .{
                    job.id,
                    job.expression,
                    job.job_type.asStr(),
                    job.next_run_secs,
                    status,
                    flags,
                    primary_label,
                    primary,
                });
                return ToolResult{ .success = true, .output = msg };
            }
            const msg = try std.fmt.allocPrint(allocator, "Job '{s}' not found", .{id});
            return ToolResult{ .success = false, .output = "", .error_msg = msg };
        }

        if (std.mem.eql(u8, action, "create") or std.mem.eql(u8, action, "add")) {
            const job_type = parseJobType(args) catch
                return ToolResult.fail("Invalid 'job_type' parameter (expected 'shell' or 'agent')");
            const delivery = parseDelivery(args) catch |err| switch (err) {
                error.DeliveryMustBeObject => return ToolResult.fail("Parameter 'delivery' must be an object"),
                error.DeliveryModeMustBeString => return ToolResult.fail("'delivery.mode' must be a string"),
                error.InvalidDeliveryMode => return ToolResult.fail("Invalid delivery.mode (expected none|always|on_error|on_success)"),
                error.DeliveryChannelMustBeString => return ToolResult.fail("'delivery.channel' must be a string"),
                error.DeliveryToMustBeString => return ToolResult.fail("'delivery.to' must be a string"),
            };
            const expression = root.getString(args, "expression") orelse
                return ToolResult.fail("Missing 'expression' parameter for cron job");
            const prompt = root.getString(args, "prompt");
            const command = root.getString(args, "command");

            const primary_input = switch (job_type) {
                .shell => command orelse return ToolResult.fail("Missing 'command' parameter"),
                .agent => prompt orelse return ToolResult.fail("Missing 'prompt' parameter for agent job"),
            };

            if (job_type == .agent and delivery.mode != .none and delivery.channel == null) {
                return ToolResult.fail("Agent jobs with delivery mode require delivery.channel");
            }

            var scheduler = loadScheduler(allocator) catch {
                return ToolResult.fail("Failed to load scheduler state");
            };
            defer scheduler.deinit();

            const job = scheduler.addJob(expression, primary_input) catch |err| {
                const msg = try std.fmt.allocPrint(allocator, "Failed to create job: {s}", .{@errorName(err)});
                return ToolResult{ .success = false, .output = "", .error_msg = msg };
            };

            if (job_type == .agent) {
                job.job_type = .agent;
                job.prompt = allocator.dupe(u8, primary_input) catch {
                    return ToolResult.fail("Failed to store agent prompt");
                };
                if (delivery.channel != null and delivery.to != null) {
                    job.session_target = .main;
                }
            }
            setJobDelivery(allocator, job, delivery) catch {
                return ToolResult.fail("Failed to store delivery config");
            };

            cron.saveJobs(&scheduler) catch {};

            const msg = try std.fmt.allocPrint(allocator, "Created job {s} | {s} | type={s} | input: {s}", .{
                job.id,
                job.expression,
                job.job_type.asStr(),
                primary_input,
            });
            return ToolResult{ .success = true, .output = msg };
        }

        if (std.mem.eql(u8, action, "once")) {
            const job_type = parseJobType(args) catch
                return ToolResult.fail("Invalid 'job_type' parameter (expected 'shell' or 'agent')");
            const delivery = parseDelivery(args) catch |err| switch (err) {
                error.DeliveryMustBeObject => return ToolResult.fail("Parameter 'delivery' must be an object"),
                error.DeliveryModeMustBeString => return ToolResult.fail("'delivery.mode' must be a string"),
                error.InvalidDeliveryMode => return ToolResult.fail("Invalid delivery.mode (expected none|always|on_error|on_success)"),
                error.DeliveryChannelMustBeString => return ToolResult.fail("'delivery.channel' must be a string"),
                error.DeliveryToMustBeString => return ToolResult.fail("'delivery.to' must be a string"),
            };
            const delay = root.getString(args, "delay") orelse
                return ToolResult.fail("Missing 'delay' parameter for one-shot task");
            const prompt = root.getString(args, "prompt");
            const command = root.getString(args, "command");

            const primary_input = switch (job_type) {
                .shell => command orelse return ToolResult.fail("Missing 'command' parameter"),
                .agent => prompt orelse return ToolResult.fail("Missing 'prompt' parameter for agent job"),
            };

            if (job_type == .agent and delivery.mode != .none and delivery.channel == null) {
                return ToolResult.fail("Agent jobs with delivery mode require delivery.channel");
            }

            var scheduler = loadScheduler(allocator) catch {
                return ToolResult.fail("Failed to load scheduler state");
            };
            defer scheduler.deinit();

            const job = scheduler.addOnce(delay, primary_input) catch |err| {
                const msg = try std.fmt.allocPrint(allocator, "Failed to create one-shot task: {s}", .{@errorName(err)});
                return ToolResult{ .success = false, .output = "", .error_msg = msg };
            };

            if (job_type == .agent) {
                job.job_type = .agent;
                job.prompt = allocator.dupe(u8, primary_input) catch {
                    return ToolResult.fail("Failed to store agent prompt");
                };
                if (delivery.channel != null and delivery.to != null) {
                    job.session_target = .main;
                }
            }
            setJobDelivery(allocator, job, delivery) catch {
                return ToolResult.fail("Failed to store delivery config");
            };

            cron.saveJobs(&scheduler) catch {};

            const msg = try std.fmt.allocPrint(allocator, "Created one-shot task {s} | runs at {d} | type={s} | input: {s}", .{
                job.id,
                job.next_run_secs,
                job.job_type.asStr(),
                primary_input,
            });
            return ToolResult{ .success = true, .output = msg };
        }

        if (std.mem.eql(u8, action, "cancel") or std.mem.eql(u8, action, "remove")) {
            const id = root.getString(args, "id") orelse
                return ToolResult.fail("Missing 'id' parameter for cancel action");

            var scheduler = loadScheduler(allocator) catch {
                return ToolResult.fail("Failed to load scheduler state");
            };
            defer scheduler.deinit();

            if (scheduler.removeJob(id)) {
                cron.saveJobs(&scheduler) catch {};
                const msg = try std.fmt.allocPrint(allocator, "Cancelled job {s}", .{id});
                return ToolResult{ .success = true, .output = msg };
            }
            const msg = try std.fmt.allocPrint(allocator, "Job '{s}' not found", .{id});
            return ToolResult{ .success = false, .output = "", .error_msg = msg };
        }

        if (std.mem.eql(u8, action, "pause") or std.mem.eql(u8, action, "resume")) {
            const id = root.getString(args, "id") orelse
                return ToolResult.fail("Missing 'id' parameter");

            var scheduler = loadScheduler(allocator) catch {
                return ToolResult.fail("Failed to load scheduler state");
            };
            defer scheduler.deinit();

            const is_pause = std.mem.eql(u8, action, "pause");
            const found = if (is_pause) scheduler.pauseJob(id) else scheduler.resumeJob(id);

            if (found) {
                cron.saveJobs(&scheduler) catch {};
                const verb: []const u8 = if (is_pause) "Paused" else "Resumed";
                const msg = try std.fmt.allocPrint(allocator, "{s} job {s}", .{ verb, id });
                return ToolResult{ .success = true, .output = msg };
            }
            const msg = try std.fmt.allocPrint(allocator, "Job '{s}' not found", .{id});
            return ToolResult{ .success = false, .output = "", .error_msg = msg };
        }

        const msg = try std.fmt.allocPrint(allocator, "Unknown action '{s}'", .{action});
        return ToolResult{ .success = false, .output = "", .error_msg = msg };
    }
};

// ── Tests ───────────────────────────────────────────────────────────

test "schedule tool name" {
    var st = ScheduleTool{};
    const t = st.tool();
    try std.testing.expectEqualStrings("schedule", t.name());
}

test "schedule schema has action" {
    var st = ScheduleTool{};
    const t = st.tool();
    const schema = t.parametersJson();
    try std.testing.expect(std.mem.indexOf(u8, schema, "action") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "job_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "prompt") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "delivery") != null);
}

test "schedule list returns success" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"list\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Either "No scheduled jobs." or a formatted job list
    try std.testing.expect(result.output.len > 0);
}

test "schedule unknown action" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"explode\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "Unknown action") != null);
}

test "schedule create with expression" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"create\", \"expression\": \"*/5 * * * *\", \"command\": \"echo hello\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    // Succeeds if HOME/.nullclaw is writable, otherwise may fail gracefully
    if (result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.output, "Created job") != null);
    }
}

test "schedule create agent job with prompt" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"create\", \"job_type\": \"agent\", \"expression\": \"*/30 * * * *\", \"prompt\": \"summarize logs\", \"delivery\": {\"mode\": \"always\", \"channel\": \"telegram\", \"to\": \"chat-1\"}}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    if (result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.output, "type=agent") != null);
    }
}

// ── Additional schedule tests ───────────────────────────────────

test "schedule missing action" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "action") != null);
}

test "schedule get missing id" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"get\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "id") != null);
}

test "schedule get nonexistent job" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"get\", \"id\": \"nonexistent-123\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "not found") != null);
}

test "schedule cancel requires id" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"cancel\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "schedule cancel nonexistent job returns not found" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"cancel\", \"id\": \"job-nonexistent\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    // Job doesn't exist in the real scheduler, so cancel returns not-found or success if previously created
    if (!result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "not found") != null);
    }
}

test "schedule remove nonexistent job returns not found" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"remove\", \"id\": \"job-nonexistent\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    if (!result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "not found") != null);
    }
}

test "schedule pause nonexistent job returns not found" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"pause\", \"id\": \"job-nonexistent\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    if (!result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "not found") != null);
    }
}

test "schedule resume nonexistent job returns not found" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"resume\", \"id\": \"job-nonexistent\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    defer if (result.error_msg) |e| std.testing.allocator.free(e);
    if (!result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "not found") != null);
    }
}

test "schedule once creates one-shot task" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"once\", \"delay\": \"30m\", \"command\": \"echo later\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    if (result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.output, "one-shot") != null);
    }
}

test "schedule add creates recurring job" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"add\", \"expression\": \"0 * * * *\", \"command\": \"echo hourly\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer if (result.output.len > 0) std.testing.allocator.free(result.output);
    if (result.success) {
        try std.testing.expect(std.mem.indexOf(u8, result.output, "Created job") != null);
    }
}

test "schedule create missing command" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"create\", \"expression\": \"* * * * *\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "command") != null);
}

test "schedule create agent missing prompt" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"create\", \"job_type\": \"agent\", \"expression\": \"* * * * *\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "prompt") != null);
}

test "schedule create missing expression" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"create\", \"command\": \"echo hi\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "expression") != null);
}

test "schedule once missing delay" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"once\", \"command\": \"echo hi\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "delay") != null);
}

test "schedule pause requires id" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"pause\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "schedule resume requires id" {
    var st = ScheduleTool{};
    const t = st.tool();
    const parsed = try root.parseTestArgs("{\"action\": \"resume\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}
