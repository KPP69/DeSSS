import React, { useEffect, useState } from "react";
import { EmptyState, Button } from "../components/ui";
import { api } from "../lib/tauri";
import type { ProxyState } from "../lib/types";

/**
 * System page: local proxy setting. The backend resolves the proxy in
 * this order — the value set here, HTTP(S)_PROXY env vars, then the
 * Windows system proxy registry — and uses it for both reqwest (REST)
 * and the WS dial (HTTP CONNECT). Applies when an account is
 * (re)connected.
 */
export default function SystemPage() {
  const [proxy, setProxy] = useState<ProxyState | null>(null);
  const [input, setInput] = useState("");
  const [msg, setMsg] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api
      .getProxy()
      .then((p) => {
        setProxy(p);
        setInput(p.custom ?? "");
      })
      .catch(() => {});
  }, []);

  const save = () => {
    setSaving(true);
    setMsg("");
    api
      .setProxy(input)
      .then((p) => {
        setProxy(p);
        setMsg("已保存，重新连接账户后生效");
      })
      .catch((e) => setMsg(`保存失败：${String(e)}`))
      .finally(() => setSaving(false));
  };

  return (
    <div>
      <div className="bg-panel border border-line rounded-lg p-4 mb-4 max-w-xl">
        <div className="text-sm font-medium text-t1 mb-1">本地代理（Clash Verge 等）</div>
        <div className="text-xs text-t2 mb-3 leading-relaxed">
          留空则不设置自定义代理，自动读取环境变量与 Windows 系统代理。填写后 REST 与 WS 都会走该代理。
        </div>
        <div className="flex items-center gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="http://127.0.0.1:7897"
            className="flex-1 min-w-0 bg-overlay border border-line rounded px-3 py-2 text-sm text-t1 outline-none focus:border-primary num"
          />
          <Button onClick={save} disabled={saving}>
            {saving ? "保存中…" : "保存"}
          </Button>
        </div>
        {proxy && (
          <div className="text-xs text-t3 mt-2 num">
            当前生效：{proxy.effective ?? "未使用代理（直连）"}
          </div>
        )}
        {msg && <div className="text-xs text-t2 mt-2">{msg}</div>}
      </div>
      <EmptyState>空壳页面 — 接口契约已就绪，业务待接入（不接真实交易所）</EmptyState>
    </div>
  );
}
