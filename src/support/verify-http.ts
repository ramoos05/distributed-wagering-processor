import { mkdirSync } from "node:fs";
import { db } from "../migrate";

// Starts three real API processes against the configured test database. SQS is
// checked as well when TEST_SQS=1; financial HTTP tests can run independently.
if (import.meta.main) {
  await db.initialize();
  try { await db.runMigrations(); } finally { await db.destroy(); }
  mkdirSync(".test-artifacts", { recursive: true });
  const ports = [3100, 3101, 3102];
  const children = ports.map(PORT => Bun.spawn([process.execPath, "src/main.ts"], {
    env: { ...process.env, PORT: String(PORT) },
    stdout: Bun.file(`.test-artifacts/api-${PORT}.log`),
    stderr: Bun.file(`.test-artifacts/api-${PORT}.error.log`),
  }));
  try {
    for (const port of ports) {
      let live = false;
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        try { live = (await fetch(`http://localhost:${port}/health/live`)).ok; } catch {}
        if (live) break;
        await Bun.sleep(100);
      }
      if (!live) throw new Error(`API ${port} failed to start; see .test-artifacts`);
    }
    console.log(JSON.stringify({ event: "test_api_processes", pids: children.map(child => child.pid) }));
    const result = Bun.spawn([process.execPath, "test", "src/integration.test.ts"], {
      env: { ...process.env, TEST_API_URLS: ports.map(port => `http://localhost:${port}`).join(",") },
      stdout: "inherit", stderr: "inherit",
    });
    process.exitCode = await result.exited;
  } finally {
    for (const child of children) child.kill("SIGTERM");
    await Promise.all(children.map(child => child.exited));
  }
}
