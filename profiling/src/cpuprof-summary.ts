// Summarises V8 .cpuprofile files: self time by function and by package.
//   npx tsx src/cpuprof-summary.ts results/cpuprof/<file>.cpuprofile [top]
import fs from "node:fs";

interface Node {
  id: number;
  callFrame: { functionName: string; url: string; lineNumber: number };
  hitCount?: number;
  children?: number[];
}

const [file, topArg] = process.argv.slice(2);
if (!file) {
  console.error("usage: cpuprof-summary <file.cpuprofile> [top]");
  process.exit(1);
}
const top = parseInt(topArg ?? "30", 10);
const profile = JSON.parse(fs.readFileSync(file, "utf-8")) as {
  nodes: Node[];
  samples: number[];
  timeDeltas: number[];
};

const selfSamples = new Map<number, number>();
for (const id of profile.samples) selfSamples.set(id, (selfSamples.get(id) ?? 0) + 1);
const total = profile.samples.length;

function pkgOf(url: string): string {
  if (!url) return "(native/vm)";
  const nm = url.lastIndexOf("node_modules/");
  if (nm !== -1) {
    const rest = url.slice(nm + 13).split("/");
    return rest[0]!.startsWith("@") ? `${rest[0]}/${rest[1]}` : rest[0]!;
  }
  if (url.includes("/dist/")) return "notifkit";
  if (url.startsWith("node:")) return url.split("/")[0]!;
  return url.split("/").slice(-2).join("/");
}

const byFn = new Map<string, number>();
const byPkg = new Map<string, number>();
for (const node of profile.nodes) {
  const n = selfSamples.get(node.id) ?? 0;
  if (!n) continue;
  const { functionName, url, lineNumber } = node.callFrame;
  const fn = `${functionName || "(anonymous)"}  ${pkgOf(url)}:${lineNumber + 1}`;
  byFn.set(fn, (byFn.get(fn) ?? 0) + n);
  byPkg.set(pkgOf(url), (byPkg.get(pkgOf(url)) ?? 0) + n);
}

const pct = (n: number) => ((n / total) * 100).toFixed(1).padStart(5) + "%";
console.log(`${total} samples\n\nSelf time by package:`);
for (const [k, v] of [...byPkg].sort((a, b) => b[1] - a[1]).slice(0, 20)) console.log(pct(v), k);
console.log(`\nSelf time by function (top ${top}):`);
for (const [k, v] of [...byFn].sort((a, b) => b[1] - a[1]).slice(0, top)) console.log(pct(v), k);
