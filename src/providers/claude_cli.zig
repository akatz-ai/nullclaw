const std = @import("std");
const root = @import("root.zig");

const Provider = root.Provider;
const ChatRequest = root.ChatRequest;
const ChatResponse = root.ChatResponse;
const ChatMessage = root.ChatMessage;

/// Provider that delegates to the `claude` CLI (Claude Code).
///
/// Runs `claude -p <prompt> --output-format stream-json --model <model>`
/// and parses the stream-json output for a `type: "result"` event.
pub const ClaudeCliProvider = struct {
    allocator: std.mem.Allocator,
    model: []const u8,
    last_session_id: ?[]const u8 = null,
    last_error_detail: ?[]const u8 = null,

    const DEFAULT_MODEL = "claude-opus-4-6";
    const CLI_NAME = "claude";
    const TIMEOUT_NS: u64 = 300 * std.time.ns_per_s;
    const ClaudeResult = struct {
        content: []const u8,
        session_id: ?[]const u8 = null,
        is_error: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, model: ?[]const u8) !ClaudeCliProvider {
        // Verify CLI is in PATH
        try checkCliAvailable(allocator, CLI_NAME);
        return .{
            .allocator = allocator,
            .model = model orelse DEFAULT_MODEL,
            .last_session_id = null,
            .last_error_detail = null,
        };
    }

    /// Create a Provider vtable interface.
    pub fn provider(self: *ClaudeCliProvider) Provider {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Provider.VTable{
        .chatWithSystem = chatWithSystemImpl,
        .chat = chatImpl,
        .supportsNativeTools = supportsNativeToolsImpl,
        .getName = getNameImpl,
        .deinit = deinitImpl,
    };

    fn chatWithSystemImpl(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        system_prompt: ?[]const u8,
        message: []const u8,
        model: []const u8,
        _: f64,
    ) anyerror![]const u8 {
        const self: *ClaudeCliProvider = @ptrCast(@alignCast(ptr));
        const effective_model = if (model.len > 0) model else self.model;

        // Combine system prompt with message if provided
        const prompt = if (system_prompt) |sys|
            try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ sys, message })
        else
            try allocator.dupe(u8, message);
        defer allocator.free(prompt);

        const claude_result = try runClaude(allocator, prompt, effective_model, self.last_session_id, TIMEOUT_NS);
        errdefer allocator.free(claude_result.content);
        defer if (claude_result.session_id) |sid| allocator.free(sid);

        if (claude_result.is_error) {
            self.updateErrorDetail(claude_result.content);
            return classifyCliError(claude_result.content);
        }

        try self.updateSessionId(claude_result.session_id);
        return claude_result.content;
    }

    fn chatImpl(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        request: ChatRequest,
        model: []const u8,
        _: f64,
    ) anyerror!ChatResponse {
        const self: *ClaudeCliProvider = @ptrCast(@alignCast(ptr));
        const effective_model = if (model.len > 0) model else self.model;

        // Extract last user message as prompt
        const prompt = extractLastUserMessage(request.messages) orelse return error.NoUserMessage;
        const timeout_ns = timeoutNsFromSecs(request.timeout_secs);
        const claude_result = try runClaude(allocator, prompt, effective_model, self.last_session_id, timeout_ns);
        errdefer allocator.free(claude_result.content);
        defer if (claude_result.session_id) |sid| allocator.free(sid);

        if (claude_result.is_error) {
            self.updateErrorDetail(claude_result.content);
            return classifyCliError(claude_result.content);
        }

        try self.updateSessionId(claude_result.session_id);
        const response_model = try allocator.dupe(u8, effective_model);
        return ChatResponse{ .content = claude_result.content, .model = response_model };
    }

    fn supportsNativeToolsImpl(_: *anyopaque) bool {
        return false;
    }

    fn getNameImpl(_: *anyopaque) []const u8 {
        return "claude-cli";
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *ClaudeCliProvider = @ptrCast(@alignCast(ptr));
        if (self.last_session_id) |sid| {
            self.allocator.free(sid);
            self.last_session_id = null;
        }
        if (self.last_error_detail) |detail| {
            self.allocator.free(detail);
            self.last_error_detail = null;
        }
    }

    fn updateSessionId(self: *ClaudeCliProvider, new_session_id: ?[]const u8) !void {
        if (new_session_id) |sid| {
            const duped = try self.allocator.dupe(u8, sid);
            if (self.last_session_id) |old_sid| {
                self.allocator.free(old_sid);
            }
            self.last_session_id = duped;
        }
    }

    fn updateErrorDetail(self: *ClaudeCliProvider, detail: []const u8) void {
        if (self.last_error_detail) |old| self.allocator.free(old);
        self.last_error_detail = self.allocator.dupe(u8, detail) catch null;
    }

    /// Run the claude CLI and parse stream-json output.
    fn runClaude(
        allocator: std.mem.Allocator,
        prompt: []const u8,
        model: []const u8,
        resume_session_id: ?[]const u8,
        timeout_ns: u64,
    ) !ClaudeResult {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(allocator);

        try args.appendSlice(allocator, &.{
            CLI_NAME,
            "-p",
            prompt,
            "--output-format",
            "json",
            "--model",
            model,
        });
        if (resume_session_id) |sid| {
            try args.appendSlice(allocator, &.{ "--resume", sid });
        }

        var child = std.process.Child.init(args.items, allocator);
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        var watchdog_state = WatchdogState{};
        var watchdog_thread: ?std.Thread = null;
        var watchdog_joined = false;
        if (timeout_ns > 0) {
            watchdog_thread = std.Thread.spawn(.{}, Watchdog.watch, .{ &watchdog_state, child.id, timeout_ns }) catch |err| {
                std.posix.kill(child.id, std.posix.SIG.KILL) catch {};
                _ = child.wait() catch {};
                return err;
            };
        }
        defer {
            watchdog_state.stop.store(true, .release);
            if (!watchdog_joined) {
                if (watchdog_thread) |wt| {
                    wt.join();
                }
            }
        }

        var stderr_state = StderrReader.State{
            .stderr_pipe = child.stderr.?,
            .allocator = allocator,
        };
        const stderr_thread = std.Thread.spawn(.{}, StderrReader.readStderr, .{&stderr_state}) catch |err| {
            std.posix.kill(child.id, std.posix.SIG.KILL) catch {};
            _ = child.wait() catch {};
            return err;
        };
        var stderr_joined = false;
        defer {
            if (!stderr_joined) {
                stderr_thread.join();
            }
            if (stderr_state.result) |stderr_buf| allocator.free(stderr_buf);
        }

        // Read all stdout while stderr is read concurrently by stderr_thread.
        const max_output: usize = 4 * 1024 * 1024; // 4 MB
        const stdout_result = child.stdout.?.readToEndAlloc(allocator, max_output) catch |err| {
            _ = child.wait() catch {};
            return err;
        };
        defer allocator.free(stdout_result);

        stderr_thread.join();
        stderr_joined = true;

        const term = try child.wait();
        watchdog_state.stop.store(true, .release);
        if (watchdog_thread) |wt| {
            wt.join();
        }
        watchdog_joined = true;

        switch (term) {
            .Exited => |code| {
                if (code != 0) {
                    const parsed_nonzero = parseStreamJson(allocator, stdout_result) catch null;
                    if (parsed_nonzero) |result| return result;
                    return fallbackErrorResult(allocator, stderr_state.result, code);
                }
            },
            .Signal => |sig| {
                if (timeout_ns > 0 and sig == std.posix.SIG.KILL) return error.CliTimeout;
                return error.CliProcessFailed;
            },
            else => return error.CliProcessFailed,
        }

        // Parse stream-json: each line is a JSON object, find type="result"
        return parseStreamJson(allocator, stdout_result);
    }

    fn fallbackErrorResult(allocator: std.mem.Allocator, stderr_output: ?[]const u8, code: u8) !ClaudeResult {
        const trimmed = if (stderr_output) |stderr_buf|
            std.mem.trim(u8, stderr_buf, " \t\r\n")
        else
            "";

        const fallback_content = if (trimmed.len > 0)
            try allocator.dupe(u8, trimmed)
        else
            try std.fmt.allocPrint(allocator, "claude exited with code {}", .{code});

        return ClaudeResult{
            .content = fallback_content,
            .session_id = null,
            .is_error = true,
        };
    }

    fn timeoutNsFromSecs(timeout_secs: u64) u64 {
        if (timeout_secs == 0) return TIMEOUT_NS;
        return std.math.mul(u64, timeout_secs, std.time.ns_per_s) catch std.math.maxInt(u64);
    }

    /// Parse claude stream-json output lines for a result event.
    fn parseStreamJson(allocator: std.mem.Allocator, output: []const u8) !ClaudeResult {
        var lines = std.mem.splitScalar(u8, output, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            const parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch continue;
            defer parsed.deinit();

            if (parsed.value != .object) continue;
            const obj = parsed.value.object;

            // Look for type: "result"
            if (obj.get("type")) |type_val| {
                if (type_val == .string and std.mem.eql(u8, type_val.string, "result")) {
                    if (obj.get("result")) |result_val| {
                        if (result_val == .string) {
                            var session_id: ?[]const u8 = null;
                            errdefer if (session_id) |sid| allocator.free(sid);

                            if (obj.get("session_id")) |sid_val| {
                                if (sid_val == .string) {
                                    session_id = try allocator.dupe(u8, sid_val.string);
                                }
                            }

                            var is_error = false;
                            if (obj.get("is_error")) |is_error_val| {
                                if (is_error_val == .bool) {
                                    is_error = is_error_val.bool;
                                }
                            }

                            return ClaudeResult{
                                .content = try allocator.dupe(u8, result_val.string),
                                .session_id = session_id,
                                .is_error = is_error,
                            };
                        }
                    }
                }
            }
        }
        return error.NoResultInOutput;
    }

    /// Health check: run `claude --version` and verify exit code 0.
    pub fn healthCheck(allocator: std.mem.Allocator) !void {
        try checkCliVersion(allocator, CLI_NAME);
    }
};

fn classifyCliError(message: []const u8) anyerror {
    if (std.mem.indexOf(u8, message, "Not logged in") != null) return error.CliAuthError;
    if (std.mem.indexOf(u8, message, "issue with the selected model") != null) return error.CliModelError;
    if (std.mem.indexOf(u8, message, "Input must be provided") != null) return error.CliInputError;
    if (std.mem.indexOf(u8, message, "cannot be launched inside another") != null) return error.CliNestedSession;
    return error.CliErrorResponse;
}

const StderrReader = struct {
    const State = struct {
        stderr_pipe: std.fs.File,
        allocator: std.mem.Allocator,
        result: ?[]u8 = null,
    };

    fn readStderr(state: *State) void {
        state.result = state.stderr_pipe.readToEndAlloc(state.allocator, 64 * 1024) catch null;
    }
};

const WatchdogState = struct {
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

const Watchdog = struct {
    fn watch(state: *WatchdogState, pid: std.posix.pid_t, timeout_ns: u64) void {
        const poll_interval_ns: u64 = 50 * std.time.ns_per_ms;
        var elapsed_ns: u64 = 0;

        while (elapsed_ns < timeout_ns) {
            if (state.stop.load(.acquire)) return;

            const remaining_ns = timeout_ns - elapsed_ns;
            const sleep_ns = @min(poll_interval_ns, remaining_ns);
            std.Thread.sleep(sleep_ns);
            elapsed_ns += sleep_ns;
        }

        if (state.stop.load(.acquire)) return;
        std.posix.kill(pid, std.posix.SIG.KILL) catch {};
    }
};

// ════════════════════════════════════════════════════════════════════════════
// Shared helpers
// ════════════════════════════════════════════════════════════════════════════

/// Check if a CLI tool is available in PATH using `which`.
fn checkCliAvailable(allocator: std.mem.Allocator, cli_name: []const u8) !void {
    const argv = [_][]const u8{ "which", cli_name };
    var child = std.process.Child.init(&argv, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    try child.spawn();
    const out = child.stdout.?.readToEndAlloc(allocator, 4096) catch {
        _ = child.wait() catch {};
        return error.CliNotFound;
    };
    allocator.free(out);
    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) return error.CliNotFound;
        },
        else => return error.CliNotFound,
    }
}

/// Run `<cli> --version` and verify exit code 0.
fn checkCliVersion(allocator: std.mem.Allocator, cli_name: []const u8) !void {
    const argv = [_][]const u8{ cli_name, "--version" };
    var child = std.process.Child.init(&argv, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    try child.spawn();
    const out = child.stdout.?.readToEndAlloc(allocator, 4096) catch {
        _ = child.wait() catch {};
        return error.CliNotFound;
    };
    allocator.free(out);
    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) return error.CliNotFound;
        },
        else => return error.CliNotFound,
    }
}

/// Extract the content of the last user message from a message slice.
fn extractLastUserMessage(messages: []const ChatMessage) ?[]const u8 {
    var i = messages.len;
    while (i > 0) {
        i -= 1;
        if (messages[i].role == .user) return messages[i].content;
    }
    return null;
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

test "ClaudeCliProvider.getNameImpl returns claude-cli" {
    const vtable = ClaudeCliProvider.vtable;
    var dummy: u8 = 0;
    try std.testing.expectEqualStrings("claude-cli", vtable.getName(@ptrCast(&dummy)));
}

test "extractLastUserMessage finds last user" {
    const msgs = [_]ChatMessage{
        ChatMessage.system("Be helpful"),
        ChatMessage.user("first"),
        ChatMessage.assistant("ok"),
        ChatMessage.user("second"),
    };
    const result = extractLastUserMessage(&msgs);
    try std.testing.expectEqualStrings("second", result.?);
}

test "extractLastUserMessage returns null for no user" {
    const msgs = [_]ChatMessage{
        ChatMessage.system("Be helpful"),
        ChatMessage.assistant("ok"),
    };
    try std.testing.expect(extractLastUserMessage(&msgs) == null);
}

test "extractLastUserMessage empty messages" {
    const msgs = [_]ChatMessage{};
    try std.testing.expect(extractLastUserMessage(&msgs) == null);
}

test "parseStreamJson extracts result" {
    const input =
        \\{"type":"start","session_id":"abc123"}
        \\{"type":"content","content":"partial"}
        \\{"type":"result","result":"Hello from Claude CLI!"}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);
    try std.testing.expectEqualStrings("Hello from Claude CLI!", result.content);
}

test "parseStreamJson no result returns error" {
    const input =
        \\{"type":"start","session_id":"abc123"}
        \\{"type":"content","content":"partial"}
    ;
    const result = ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    try std.testing.expectError(error.NoResultInOutput, result);
}

test "parseStreamJson handles empty input" {
    const result = ClaudeCliProvider.parseStreamJson(std.testing.allocator, "");
    try std.testing.expectError(error.NoResultInOutput, result);
}

test "parseStreamJson handles invalid json lines gracefully" {
    const input =
        \\not json at all
        \\{"type":"result","result":"found it"}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);
    try std.testing.expectEqualStrings("found it", result.content);
}

test "parseStreamJson skips result with non-string value" {
    const input =
        \\{"type":"result","result":42}
    ;
    const result = ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    try std.testing.expectError(error.NoResultInOutput, result);
}

test "parseStreamJson extracts session_id field" {
    const input =
        \\{"type":"result","result":"ok","session_id":"702fc7d0-a779-4091-892e-b84fb1e0b345"}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);

    try std.testing.expectEqualStrings("ok", result.content);
    try std.testing.expect(result.session_id != null);
    try std.testing.expectEqualStrings("702fc7d0-a779-4091-892e-b84fb1e0b345", result.session_id.?);
}

test "parseStreamJson returns null session_id when field missing" {
    const input =
        \\{"type":"result","result":"ok"}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);

    try std.testing.expect(result.session_id == null);
}

test "parseStreamJson extracts is_error field" {
    const input =
        \\{"type":"result","result":"invalid model","is_error":true}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);

    try std.testing.expect(result.is_error);
    try std.testing.expectEqualStrings("invalid model", result.content);
}

test "parseStreamJson handles non-zero-exit error JSON payload" {
    const input =
        \\{"type":"result","is_error":true,"result":"There's an issue with the selected model (bad-model). It may not exist or you may not have access to it."}
    ;
    const result = try ClaudeCliProvider.parseStreamJson(std.testing.allocator, input);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);

    try std.testing.expect(result.is_error);
    try std.testing.expectEqualStrings(
        "There's an issue with the selected model (bad-model). It may not exist or you may not have access to it.",
        result.content,
    );
}

test "classifyCliError returns specific typed errors" {
    try std.testing.expectEqual(
        error.CliAuthError,
        classifyCliError("Not logged in · Please run /login"),
    );
    try std.testing.expectEqual(
        error.CliModelError,
        classifyCliError("There's an issue with the selected model (bad-model). It may not exist."),
    );
    try std.testing.expectEqual(
        error.CliInputError,
        classifyCliError("Error: Input must be provided either through stdin or as a prompt argument when using --print"),
    );
    try std.testing.expectEqual(
        error.CliNestedSession,
        classifyCliError("Error: Claude Code cannot be launched inside another Claude Code session."),
    );
    try std.testing.expectEqual(
        error.CliErrorResponse,
        classifyCliError("some unknown Claude CLI failure"),
    );
}

test "fallbackErrorResult builds stderr-based ClaudeResult" {
    const stderr_text = "  Error: Input must be provided either through stdin or as a prompt argument when using --print  \n";
    const result = try ClaudeCliProvider.fallbackErrorResult(std.testing.allocator, stderr_text, 1);
    defer std.testing.allocator.free(result.content);
    defer if (result.session_id) |sid| std.testing.allocator.free(sid);

    try std.testing.expect(result.is_error);
    try std.testing.expect(result.session_id == null);
    try std.testing.expectEqualStrings(
        "Error: Input must be provided either through stdin or as a prompt argument when using --print",
        result.content,
    );
}

test "ClaudeResult defaults are correct" {
    const result = ClaudeCliProvider.ClaudeResult{
        .content = "sample",
    };
    try std.testing.expectEqualStrings("sample", result.content);
    try std.testing.expect(result.session_id == null);
    try std.testing.expect(!result.is_error);
}

test "ClaudeCliProvider vtable has correct function pointers" {
    const vtable = ClaudeCliProvider.vtable;
    var dummy: u8 = 0;
    try std.testing.expectEqualStrings("claude-cli", vtable.getName(@ptrCast(&dummy)));
    try std.testing.expect(!vtable.supportsNativeTools(@ptrCast(&dummy)));
}

test "ClaudeCliProvider.init returns CliNotFound for missing binary" {
    const result = checkCliAvailable(std.testing.allocator, "nonexistent_binary_xyzzy_12345");
    try std.testing.expectError(error.CliNotFound, result);
}

test "ClaudeCliProvider default model is claude-opus-4-6" {
    try std.testing.expectEqualStrings("claude-opus-4-6", ClaudeCliProvider.DEFAULT_MODEL);
}

test "runClaude return type accepts CliTimeout error" {
    const ReturnType = @typeInfo(@TypeOf(ClaudeCliProvider.runClaude)).@"fn".return_type.?;
    const result: ReturnType = error.CliTimeout;
    try std.testing.expectError(error.CliTimeout, result);
}

test "timeoutNsFromSecs converts seconds to nanoseconds" {
    const timeout_ns = ClaudeCliProvider.timeoutNsFromSecs(5);
    try std.testing.expectEqual(@as(u64, 5 * std.time.ns_per_s), timeout_ns);
}

test "timeoutNsFromSecs falls back to default when timeout is zero" {
    const timeout_ns = ClaudeCliProvider.timeoutNsFromSecs(0);
    try std.testing.expectEqual(ClaudeCliProvider.TIMEOUT_NS, timeout_ns);
}

// Integration timeout/deadlock behavior requires spawning intentionally slow processes and is
// better covered in end-to-end tests.
