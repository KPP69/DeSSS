import React, { useEffect, useState } from "react";
import { Button } from "../components/ui";
import ThemeSwitcher from "../components/ThemeSwitcher";
import { useInvoke } from "../hooks/useInvoke";
import { api } from "../lib/tauri";
import { useVenueStore } from "../store/useVenueStore";

/**
 * System page: local proxy setting. The backend resolves the proxy in
 * this order — the value set here, HTTP(S)_PROXY env vars, then the
 * Windows system proxy registry — and uses it for both reqwest (REST)
 * and the WS dial (HTTP CONNECT). Applies when an account is
 * (re)connected.
 *
 * Proxy + catalog both come from `useInvoke` (Phase 2, I11); saving
 * the proxy is a click mutation that refreshes the proxy afterwards.
 */
export default function SystemPage() {
  const proxy = useInvoke(() => api.getProxy(), []);
  const catalog = useInvoke(() => api.getCatalogStatus(), []);
  const [input, setInput] = useState("");
  const [msg, setMsg] = useState("");
  const [saving, setSaving] = useState(false);
  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;

  useEffect(() => {
    if (proxy.data) setInput(proxy.data.custom ?? "");
  }, [proxy.data]);

  const save = async () => {
    setSaving(true);
    setMsg("");
    try {
      await api.setProxy(input);
      proxy.refresh();
      setMsg("已保存，重新连接账户后生效");
    } catch (e) {
      setMsg(`保存失败：${String(e)}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div>
      <div className="bg-ds-panel border border-ds-line rounded-[10px] shadow-card p-4 mb-4 max-w-xl">
        <div className="font-semibold text-ds-text-1 mb-1">外观主题</div>
        <div className="text-xs text-ds-text-2 mb-3 leading-relaxed">
          浅色 / 暖色 / 深色，切换后立即生效，保存在本机。
        </div>
        <ThemeSwitcher />
      </div>
      <div className="bg-ds-panel border border-ds-line rounded-[10px] shadow-card p-4 mb-4 max-w-xl">
        <div className="font-semibold text-ds-text-1 mb-1">本地代理（Clash Verge 等）</div>
        <div className="text-xs text-ds-text-2 mb-3 leading-relaxed">
          留空则不设置自定义代理，自动读取环境变量与 Windows 系统代理。填写后 REST 与 WS 都会走该代理。
        </div>
        <div className="flex items-center gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="http://127.0.0.1:7897"
            className="flex-1 min-w-0 bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent num"
          />
          <Button onClick={save} disabled={saving}>
            {saving ? "保存中…" : "保存"}
          </Button>
        </div>
        {proxy.data && (
          <div className="text-xs text-ds-text-3 mt-2 num">
            当前生效：{proxy.data.effective ?? "未使用代理（直连）"}
          </div>
        )}
        {msg && <div className="text-xs text-ds-text-2 mt-2">{msg}</div>}
      </div>
      <div className="bg-ds-panel border border-ds-line rounded-[10px] shadow-card p-4 mb-4 max-w-xl">
        <div className="font-semibold text-ds-text-1 mb-1">数据目录</div>
        <div className="text-xs text-ds-text-2 mb-3 leading-relaxed">
          品种目录落库保存，搜索不走网络；后台每 12 小时刷新一次。
        </div>
        {catalog.data ? (
          <>
            <div className="text-xs text-ds-text-2 leading-relaxed num">
              {catalog.data.venues.map((v) => (
                <div key={v.exchangeId} className="flex justify-between gap-2">
                  <span>{venueName(v.exchangeId)}</span>
                  <span>
                    {v.rows} 个品种
                    {" · "}
                    {v.error
                      ? "刷新失败"
                      : v.updatedAt == null
                        ? "未就绪"
                        : `更新于 ${new Date(v.updatedAt).toLocaleString("zh-CN", { hour12: false })}`}
                  </span>
                </div>
              ))}
            </div>
            <div className="text-xs text-ds-text-3 mt-3 num break-all">
              日志目录：{catalog.data.logDir}
            </div>
          </>
        ) : (
          <div className="text-xs text-ds-text-3">加载中…</div>
        )}
      </div>
    </div>
  );
}
