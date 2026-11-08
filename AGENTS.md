AEGIS — Agent Operating Rules
These rules apply to all AI coding agents working in this repository.
1. Project authority
- AEGIS is a StarkTech project.
- The project owner has final authority over product, architecture, security, and implementation decisions.
- Treat recommendations as proposals, not authorization.
- Do not make independent product or architecture decisions.
2. Default operating mode
- Before changing code, inspect the relevant implementation, its callers, tests, interfaces, and documentation.
- Prefer small, focused, reviewable changes.
- Preserve existing behavior and public APIs unless a change is explicitly approved.
- Do not perform broad rewrites or unrelated cleanup.
- Do not assume that a feature exists because it appears in the README, a design document, a comment, or this instruction file. Verify the actual source code.
- Clearly distinguish verified behavior, incomplete implementation, missing functionality, and potential risks.
3. Approval and write boundaries
- If the user requests an audit, review, explanation, investigation, or plan, treat the task as read-only.
- During a read-only task, do not create, modify, delete, rename, move, or format repository files.
- Do not install, update, or remove dependencies during a read-only task.
- Do not modify go.mod, go.sum, CI workflows, Makefiles, editor settings, or other configuration during a read-only task.
- Do not run commands that may write to the repository during a read-only task.
- Do not commit, push, switch branches, or alter Git state unless explicitly requested.
- A request to analyze or recommend a fix is not permission to implement it.
- Before implementation, obtain explicit approval for the proposed scope. If the requested scope is unclear, ask one focused clarification question.
- Never claim that a change or command succeeded unless its result was actually observed.
4. Go engineering standards
- Follow current idiomatic Go practices and the repository's established conventions.
- Prefer the Go standard library when it is sufficient.
- Add a dependency only when there is a clear technical justification and approval where required.
- Handle errors explicitly; do not silently discard errors.
- Use context.Context for operations that need cancellation or deadlines.
- Make goroutine ownership, shutdown, and resource cleanup explicit.
- Bound queues, buffers, packet sizes, retries, and other resource-consuming inputs.
- Protect shared mutable state and verify concurrency behavior.
- Avoid custom cryptography. Use established, reviewed cryptographic protocols and libraries.
- Do not treat CRC32 or another non-cryptographic checksum as authentication.
- Do not describe a basic ACK mechanism as complete reliable delivery without verifying ordering, retransmission, duplicate handling, timeout, congestion, and failure behavior.
5. Tests and verification
- Add or update relevant tests for every approved implementation change.
- Test failure paths and malformed input, not only successful execution.
- Use deterministic tests where practical.
- Use go test -race when applicable.
- Do not claim tests, benchmarks, lint, or builds passed unless they were actually run and the output was checked.
- Report commands that were not run and explain why.
- Do not present benchmark results without the hardware, Go version, workload, methodology, and relevant limitations.
6. Security and networking
- Consider malformed packets, oversized lengths, integer overflow, replay, spoofing, packet loss, duplication, reordering, network partitions, and resource exhaustion.
- Treat network input as untrusted.
- Do not invent protocol guarantees that the implementation does not provide.
- Separate transport, packet protocol, reliability, security, and distributed coordination responsibilities.
- Do not equate heartbeat-based failure detection with consensus or safe leader election.
- For leader election, explicitly consider membership, terms/epochs, split-brain, partitions, and the consistency model.
7. Reporting
For each significant finding, include:
- Severity: Critical, High, Medium, or Low.
- Verification status: Verified, Partially Implemented, Unimplemented, Potential Risk, or Unverified.
- Exact file path and relevant symbol when available.
- Observed behavior and supporting evidence.
- Technical impact and conditions under which the issue occurs.
- Recommended action and tests needed.
Never invent file paths, symbols, line numbers, test results, or implementation details.