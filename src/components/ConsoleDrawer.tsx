import React, { useMemo} from "react";
import { useConsoleStore, errorCount} from "../store/useConsoleStore";
import type { LogEntry} from "../lib/types";

function fmtTime(ts: number): string {
const d = new Date(ts);
const p = (n: number) => String(n).padStart(2, "0");
return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/** 6px status dot by level — the only color in a log row. */
function levelDot(level: string): string {
if (level === "error") return "bg-ds-down";
if (level === "warn") return "bg-ds-warn";
return "bg-ds-text-3";
}

function Row({ log}: { log: LogEntry}) {
return (
<div className="flex items-baseline gap-2 px-3 py-1 text-[12px] leading-snug border-b border-ds-line/50">
<span className="num text-ds-text-3 shrink-0 text-[11px]">{fmtTime(log.ts)}</span>
<span className="shrink-0 w-10 flex items-center gap-1.5 text-ds-text-2">
<span className={`inline-block w-1.5 h-1.5 rounded-full ${levelDot(log.level)}`} />
{log.level === "error"? "错误": log.level === "warn"? "警告": "信息"}
</span>
<span className="shrink-0 text-ds-text-2">{log.source}</span>
<span className="text-ds-text-1 break-words min-w-0">{log.message}</span>
</div>
);
}

/**
* Bottom console drawer: slides up from the window bottom, shows the
* backend's runtime logs (newest first, last 300 kept). The tab
* filters to error lines only — e.g. why a connection failed or timed out.
*/
export default function ConsoleDrawer({
open,
onClose,
}: {
open: boolean;
onClose: () => void;
}) {
const logs = useConsoleStore((s) => s.logs);
const filter = useConsoleStore((s) => s.filter);
const setFilter = useConsoleStore((s) => s.setFilter);
const clear = useConsoleStore((s) => s.clear);
const shown = useMemo(
() => (filter === "error"? logs.filter((l) => l.level === "error"): logs),
[logs, filter]
);
const errors = errorCount(logs);

return (
<div
className={`fixed inset-x-0 bottom-0 z-40 flex flex-col bg-ds-panel border-t border-ds-line shadow-lg transition-transform duration-200 ${
open? "translate-y-0": "translate-y-full pointer-events-none"
}`}
style={{ height: 300}}
aria-hidden={!open}
>
<div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-ds-line">
<span className="text-[13px] font-semibold text-ds-text-1">控制台</span>
<div className="flex items-center gap-1 ml-2">
<button
onClick={() => setFilter("all")}
className={`px-2.5 py-1 rounded text-xs ${
filter === "all"? "bg-ds-accent text-ds-accent-contrast": "text-ds-text-2 hover:bg-ds-panel-2"
}`}
>
全部
</button>
<button
onClick={() => setFilter("error")}
className={`px-2.5 py-1 rounded text-xs ${
filter === "error"? "bg-ds-accent text-ds-accent-contrast": "text-ds-text-2 hover:bg-ds-panel-2"
}`}
>
错误{errors > 0? ` (${errors})`: ""}
</button>
</div>
<div className="flex-1" />
<button onClick={clear} className="px-2 py-1 rounded text-xs text-ds-text-2 hover:bg-ds-panel-2">
清空
</button>
<button onClick={onClose} className="px-2 py-1 rounded text-xs text-ds-text-2 hover:bg-ds-panel-2">
关闭 ✕
</button>
</div>
<div className="flex-1 overflow-y-auto">
{shown.length === 0? (
<div className="px-3 py-4 text-xs text-ds-text-3">
{filter === "error"? "暂无错误日志": "暂无日志 — 连接账户后这里会显示后台运行日志"}
</div>
): (
shown.map((l, i) => <Row key={`${l.ts}-${i}`} log={l} />)
)}
</div>
</div>
);
}
